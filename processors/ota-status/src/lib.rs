//! Rust port of the legacy Spark `OtaStatusJob`: the latest OTA status per VIN.
//!
//! Pipeline, matching the legacy job step by step:
//! 1. parse each JSON line; malformed lines are skipped,
//! 2. keep `ota_status` events that have `campaign_id`, `software_version`
//!    and `status`,
//! 3. de-duplicate by `event_id`,
//! 4. order each VIN's events by (`ts_ms`, `event_id`) and keep the last.
//!
//! Output is one JSON line per VIN, ascending by VIN. Location is never read.

use std::io::{self, BufRead, Write};

use common::{dedup_by_event_id, group_by_vin_ordered, parse_line, EventType, VehicleEvent, Vin};
use serde::Serialize;

/// Output record, one per VIN.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OtaStatus {
    pub vin: Vin,
    pub campaign_id: String,
    pub software_version: String,
    pub status: String,
    pub status_ts_ms: i64,
    pub source_event_id: String,
}

/// Counters that are safe to log: no VIN, no location.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub lines: usize,
    pub malformed: usize,
    pub ignored: usize,
    pub duplicates: usize,
    pub vins: usize,
}

fn to_ota(event: &VehicleEvent) -> Option<OtaStatus> {
    if event.event_type != EventType::OtaStatus {
        return None;
    }
    Some(OtaStatus {
        vin: event.vin.clone(),
        campaign_id: event.campaign_id.clone()?,
        software_version: event.software_version.clone()?,
        status: event.status.clone()?,
        status_ts_ms: event.ts_ms,
        source_event_id: event.event_id.clone(),
    })
}

/// Computes the latest OTA status per VIN from already parsed events.
pub fn latest_status(events: impl IntoIterator<Item = VehicleEvent>) -> (Vec<OtaStatus>, Stats) {
    let mut stats = Stats::default();
    let ota: Vec<VehicleEvent> = events
        .into_iter()
        .filter(|e| {
            let keep = to_ota(e).is_some();
            if !keep {
                stats.ignored += 1;
            }
            keep
        })
        .collect();
    let before = ota.len();
    let unique = dedup_by_event_id(ota);
    stats.duplicates = before - unique.len();

    let out: Vec<OtaStatus> = group_by_vin_ordered(unique)
        .values()
        .filter_map(|events| events.last().and_then(to_ota))
        .collect();
    stats.vins = out.len();
    (out, stats)
}

/// Reads JSON lines from `input` and writes one JSON line per VIN to `output`.
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
                eprintln!("ota-status: skipping {err}");
            }
        }
    }
    let (statuses, mut stats) = latest_status(events);
    stats.lines = lines;
    stats.malformed = malformed;
    for status in &statuses {
        serde_json::to_writer(&mut output, status)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ota(id: &str, vin: &str, ts: i64, status: &str) -> String {
        format!(
            r#"{{"event_id":"{id}","vin":"{vin}","event_type":"ota_status","ts_ms":{ts},"campaign_id":"c1","software_version":"2.0.0","status":"{status}"}}"#
        )
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

    #[test]
    fn latest_event_time_wins_regardless_of_arrival() {
        let (out, _) = run_lines(&[
            ota("b", "V1", 20, "succeeded"),
            ota("a", "V1", 10, "installing"),
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["status"], "succeeded");
        assert_eq!(out[0]["status_ts_ms"], 20);
    }

    #[test]
    fn duplicate_event_ids_are_counted_once() {
        let (out, stats) = run_lines(&[
            ota("a", "V1", 10, "installing"),
            ota("a", "V1", 10, "installing"),
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(stats.duplicates, 1);
    }

    #[test]
    fn skips_other_types_incomplete_and_malformed_lines() {
        let lines = [
            r#"{"event_id":"x","vin":"V1","event_type":"charging_start","ts_ms":99}"#.to_string(),
            r#"{"event_id":"y","vin":"V1","event_type":"ota_status","ts_ms":98}"#.to_string(),
            "not json".to_string(),
            ota("a", "V1", 10, "installing"),
        ];
        let (out, stats) = run_lines(&lines);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["source_event_id"], "a");
        assert_eq!(
            stats,
            Stats {
                lines: 4,
                malformed: 1,
                ignored: 2,
                duplicates: 0,
                vins: 1
            }
        );
    }
}
