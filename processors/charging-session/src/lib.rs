//! Rust port of the legacy Spark `ChargingSessionJob`: one record per
//! charging session, built from start and stop events.
//!
//! Pipeline, matching the legacy job step by step:
//! 1. parse each JSON line; malformed lines are skipped,
//! 2. keep `charging_start` and `charging_stop` events,
//! 3. de-duplicate by `event_id`,
//! 4. walk each VIN's events in event order (`ts_ms`, then `event_id`):
//!    - a start opens a session, replacing an unfinished one,
//!    - a stop closes the open session, or is ignored if none is open,
//!    - a session longer than [`MAX_OPEN_MS`] is discarded,
//!    - a start still open at the end of the input yields nothing.
//!
//! Like the legacy fixture replay, the whole input is one bounded batch, so
//! every event is past the watermark. Output is one JSON line per session,
//! ascending by VIN, then start time.

use std::io::{self, BufRead, Write};

use common::{
    dedup_by_event_id, group_by_vin_ordered, parse_line, EventType, Location, VehicleEvent, Vin,
};
use serde::Serialize;

/// Longest session that is emitted, in milliseconds (24 h).
pub const MAX_OPEN_MS: i64 = 24 * 60 * 60 * 1000;

/// Output record, one per session. Absent optional fields are omitted, like
/// Spark's `to_json`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChargingSession {
    pub vin: Vin,
    pub start_event_id: String,
    pub stop_event_id: String,
    pub start_ts_ms: i64,
    pub stop_ts_ms: i64,
    pub duration_s: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_soc_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_soc_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub energy_kwh: Option<f64>,
    /// Location of the start event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
}

/// Counters that are safe to log: no VIN, no location.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub lines: usize,
    pub malformed: usize,
    pub ignored: usize,
    pub duplicates: usize,
    pub vins: usize,
    pub sessions: usize,
    /// Sessions dropped for lasting longer than [`MAX_OPEN_MS`].
    pub too_long: usize,
    /// Stops without an open session.
    pub unmatched_stops: usize,
    /// Starts never closed by a stop.
    pub open_starts: usize,
}

fn is_charging(event: &VehicleEvent) -> bool {
    matches!(
        event.event_type,
        EventType::ChargingStart | EventType::ChargingStop
    )
}

fn session(start: &VehicleEvent, stop: &VehicleEvent) -> ChargingSession {
    ChargingSession {
        vin: start.vin.clone(),
        start_event_id: start.event_id.clone(),
        stop_event_id: stop.event_id.clone(),
        start_ts_ms: start.ts_ms,
        stop_ts_ms: stop.ts_ms,
        duration_s: (stop.ts_ms - start.ts_ms) / 1000,
        start_soc_pct: start.soc_pct,
        end_soc_pct: stop.soc_pct,
        energy_kwh: stop.energy_kwh,
        location: start.location,
    }
}

/// Pairs one VIN's events, already de-duplicated and in event order.
fn pair(ordered: &[VehicleEvent], stats: &mut Stats) -> Vec<ChargingSession> {
    let mut out = Vec::new();
    let mut open: Option<&VehicleEvent> = None;
    for event in ordered {
        match event.event_type {
            EventType::ChargingStart => {
                if open.replace(event).is_some() {
                    stats.open_starts += 1;
                }
            }
            EventType::ChargingStop => match open.take() {
                Some(start) if event.ts_ms - start.ts_ms <= MAX_OPEN_MS => {
                    out.push(session(start, event));
                }
                Some(_) => stats.too_long += 1,
                None => stats.unmatched_stops += 1,
            },
            _ => {}
        }
    }
    if open.is_some() {
        stats.open_starts += 1;
    }
    out
}

/// Builds charging sessions from already parsed events.
pub fn sessions(events: impl IntoIterator<Item = VehicleEvent>) -> (Vec<ChargingSession>, Stats) {
    let mut stats = Stats::default();
    let charging: Vec<VehicleEvent> = events
        .into_iter()
        .filter(|e| {
            let keep = is_charging(e);
            if !keep {
                stats.ignored += 1;
            }
            keep
        })
        .collect();
    let before = charging.len();
    let unique = dedup_by_event_id(charging);
    stats.duplicates = before - unique.len();

    let groups = group_by_vin_ordered(unique);
    stats.vins = groups.len();
    let out: Vec<ChargingSession> = groups
        .values()
        .flat_map(|events| pair(events, &mut stats))
        .collect();
    stats.sessions = out.len();
    (out, stats)
}

/// Reads JSON lines from `input` and writes one JSON line per session to
/// `output`.
pub fn run(input: impl BufRead, mut output: impl Write) -> io::Result<Stats> {
    let mut events = Vec::new();
    let mut lines = 0;
    let mut malformed = 0;
    for (idx, line) in input.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        lines += 1;
        match parse_line(&line, idx + 1) {
            Ok(event) => events.push(event),
            Err(err) => {
                malformed += 1;
                eprintln!("charging-session: skipping {err}");
            }
        }
    }
    let (sessions, mut stats) = sessions(events);
    stats.lines = lines;
    stats.malformed = malformed;
    for session in &sessions {
        serde_json::to_writer(&mut output, session)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 60 * 60 * 1000;

    fn event(id: &str, vin: &str, event_type: &str, ts: i64) -> String {
        format!(r#"{{"event_id":"{id}","vin":"{vin}","event_type":"{event_type}","ts_ms":{ts}}}"#)
    }

    fn start(id: &str, vin: &str, ts: i64) -> String {
        event(id, vin, "charging_start", ts)
    }

    fn stop(id: &str, vin: &str, ts: i64) -> String {
        event(id, vin, "charging_stop", ts)
    }

    fn run_lines(lines: &[String]) -> (Vec<serde_json::Value>, Stats) {
        let input = lines.join("\n");
        let mut out = Vec::new();
        let stats = run(input.as_bytes(), &mut out).unwrap();
        let values = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        (values, stats)
    }

    fn ids(out: &[serde_json::Value]) -> Vec<(&str, &str)> {
        out.iter()
            .map(|s| {
                (
                    s["start_event_id"].as_str().unwrap(),
                    s["stop_event_id"].as_str().unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn pairs_in_event_order_not_arrival_order() {
        let (out, _) = run_lines(&[stop("b", "V1", 2 * HOUR), start("a", "V1", HOUR)]);
        assert_eq!(ids(&out), [("a", "b")]);
        assert_eq!(out[0]["duration_s"], 3600);
    }

    #[test]
    fn duplicate_events_are_ignored() {
        let (out, stats) = run_lines(&[
            start("a", "V1", 0),
            start("a", "V1", 0),
            stop("b", "V1", HOUR),
            stop("b", "V1", HOUR),
        ]);
        assert_eq!(ids(&out), [("a", "b")]);
        assert_eq!(stats.duplicates, 2);
    }

    #[test]
    fn newer_start_replaces_open_one_and_lone_stop_is_ignored() {
        let (out, stats) = run_lines(&[
            stop("x", "V1", 0),
            start("a", "V1", HOUR),
            start("b", "V1", 2 * HOUR),
            stop("c", "V1", 3 * HOUR),
            start("d", "V1", 4 * HOUR),
        ]);
        assert_eq!(ids(&out), [("b", "c")]);
        assert_eq!(stats.unmatched_stops, 1);
        assert_eq!(stats.open_starts, 2);
    }

    #[test]
    fn sessions_longer_than_24_hours_are_dropped() {
        let (out, stats) = run_lines(&[
            start("a", "V1", 0),
            stop("b", "V1", MAX_OPEN_MS + 1),
            stop("c", "V1", MAX_OPEN_MS + 2),
            start("d", "V2", 0),
            stop("e", "V2", MAX_OPEN_MS),
        ]);
        assert_eq!(ids(&out), [("d", "e")]);
        assert_eq!(stats.too_long, 1);
        assert_eq!(stats.unmatched_stops, 1);
    }

    #[test]
    fn writes_payload_from_start_and_stop_and_omits_absent_fields() {
        let (out, _) = run_lines(&[
            r#"{"event_id":"a","vin":"V1","event_type":"charging_start","ts_ms":0,"soc_pct":20.0,"location":{"lat":1.5}}"#.to_string(),
            r#"{"event_id":"b","vin":"V1","event_type":"charging_stop","ts_ms":1000,"soc_pct":80.0,"energy_kwh":5.5,"location":{"lat":9.0,"lon":9.0}}"#.to_string(),
        ]);
        let s = &out[0];
        assert_eq!(s["start_soc_pct"], 20.0);
        assert_eq!(s["end_soc_pct"], 80.0);
        assert_eq!(s["energy_kwh"], 5.5);
        assert!(
            s["location"] == serde_json::json!({"lat": 1.5}),
            "location is not the start's"
        );

        let (out, _) = run_lines(&[start("a", "V1", 0), stop("b", "V1", 1000)]);
        let keys: Vec<&String> = out[0].as_object().unwrap().keys().collect();
        assert_eq!(keys.len(), 6, "absent optional fields must be omitted");
    }

    #[test]
    fn output_is_sorted_by_vin_then_start() {
        let (out, stats) = run_lines(&[
            start("c", "V2", 0),
            stop("d", "V2", 10),
            start("e", "V1", 100),
            stop("f", "V1", 110),
            start("a", "V1", 0),
            stop("b", "V1", 10),
            event("o", "V1", "ota_status", 5),
        ]);
        assert_eq!(ids(&out), [("a", "b"), ("e", "f"), ("c", "d")]);
        assert_eq!(stats.ignored, 1);
        assert_eq!(stats.vins, 2);
        assert_eq!(stats.sessions, 3);
    }
}
