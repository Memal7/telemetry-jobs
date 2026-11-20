//! Vehicle event envelope shared by all jobs, parsed from one JSON line.

use std::fmt;

use serde_json::{Map, Value};

use crate::redact::{Location, Vin};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    OtaStatus,
    ChargingStart,
    ChargingStop,
    Other,
}

impl EventType {
    fn from_name(name: &str) -> Self {
        match name {
            "ota_status" => Self::OtaStatus,
            "charging_start" => Self::ChargingStart,
            "charging_stop" => Self::ChargingStop,
            _ => Self::Other,
        }
    }
}

/// One vehicle event. Envelope fields are required; payload fields are
/// optional and validated by each job. `Debug` never shows VIN or location.
#[derive(Debug, Clone, PartialEq)]
pub struct VehicleEvent {
    pub event_id: String,
    pub vin: Vin,
    pub event_type: EventType,
    pub ts_ms: i64,
    pub campaign_id: Option<String>,
    pub software_version: Option<String>,
    pub status: Option<String>,
    pub soc_pct: Option<f64>,
    pub energy_kwh: Option<f64>,
    pub location: Option<Location>,
}

/// Why a line was rejected. Holds no input text, so it is safe to log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Not valid JSON, or not a JSON object.
    Malformed { line: usize, column: usize },
    /// `event_id`, `vin`, `event_type` or `ts_ms` is missing, empty or
    /// `ts_ms` is not an integer.
    MissingEnvelope { line: usize },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed { line, column } => {
                write!(f, "malformed event on input line {line}, column {column}")
            }
            Self::MissingEnvelope { line } => write!(
                f,
                "event on input line {line} lacks a valid event_id, vin, event_type or ts_ms"
            ),
        }
    }
}

impl std::error::Error for ParseError {}

/// Parses one JSON line with the legacy jobs' rules (Spark `from_json` in
/// permissive mode, see `legacy/.../VehicleEvents.scala`):
///
/// - the line must start with a JSON object; trailing text is ignored and
///   for a repeated key the last value wins,
/// - string fields accept any JSON value; non-strings become their JSON text
///   (`7` becomes `"7"`),
/// - `ts_ms` must be an integer; numeric payload fields must be numbers,
/// - a payload field with the wrong type is treated as absent instead of
///   rejecting the event; `location` is kept if it is an object, with each
///   coordinate that is not a number left empty.
///
/// `line_no` is 1-based and only used for errors. serde_json messages can
/// quote input values, so they are dropped and only the position is kept.
pub fn parse_line(text: &str, line_no: usize) -> Result<VehicleEvent, ParseError> {
    let malformed = |column| ParseError::Malformed {
        line: line_no,
        column,
    };
    let value = match serde_json::Deserializer::from_str(text)
        .into_iter::<Value>()
        .next()
    {
        Some(Ok(value)) => value,
        Some(Err(err)) => return Err(malformed(err.column())),
        None => return Err(malformed(0)),
    };
    let Value::Object(fields) = value else {
        return Err(malformed(1));
    };
    let missing = ParseError::MissingEnvelope { line: line_no };

    let event_id = string(&fields, "event_id")
        .filter(|s| !s.is_empty())
        .ok_or(missing.clone())?;
    let vin = string(&fields, "vin")
        .filter(|s| !s.is_empty())
        .map(Vin::new)
        .ok_or(missing.clone())?;
    let event_type = string(&fields, "event_type").ok_or(missing.clone())?;
    let ts_ms = fields.get("ts_ms").and_then(Value::as_i64).ok_or(missing)?;

    Ok(VehicleEvent {
        event_id,
        vin,
        event_type: EventType::from_name(&event_type),
        ts_ms,
        campaign_id: string(&fields, "campaign_id"),
        software_version: string(&fields, "software_version"),
        status: string(&fields, "status"),
        soc_pct: number(&fields, "soc_pct"),
        energy_kwh: number(&fields, "energy_kwh"),
        location: match fields.get("location") {
            Some(Value::Object(loc)) => Some(Location {
                lat: number(loc, "lat"),
                lon: number(loc, "lon"),
            }),
            _ => None,
        },
    })
}

fn string(fields: &Map<String, Value>, key: &str) -> Option<String> {
    match fields.get(key)? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn number(fields: &Map<String, Value>, key: &str) -> Option<f64> {
    fields.get(key).and_then(Value::as_f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Assertions on VIN or location use `assert!` with a fixed message, so a
    // failing test never prints them.
    const VIN: &str = "TESTVIN0000000001";

    fn ota(extra: &str) -> String {
        format!(
            r#"{{"event_id":"e1","vin":"{VIN}","event_type":"ota_status","ts_ms":10,"campaign_id":"c1","software_version":"1.2.0","status":"installing"{extra}}}"#
        )
    }

    #[test]
    fn parses_ota_event() {
        let event = parse_line(&ota(""), 1).unwrap();
        assert_eq!(event.event_type, EventType::OtaStatus);
        assert!(event.vin.expose() == VIN, "VIN not preserved");
        assert_eq!(event.status.as_deref(), Some("installing"));
        assert!(event.location.is_none(), "unexpected location");
    }

    #[test]
    fn unknown_event_type_maps_to_other() {
        let line =
            format!(r#"{{"event_id":"e1","vin":"{VIN}","event_type":"door_open","ts_ms":1}}"#);
        assert_eq!(parse_line(&line, 1).unwrap().event_type, EventType::Other);
    }

    #[test]
    fn errors_do_not_contain_input() {
        let bad_ts =
            format!(r#"{{"event_id":"e1","vin":"{VIN}","event_type":"ota_status","ts_ms":"x"}}"#);
        let truncated = format!(r#"{{"event_id":"e1","vin":"{VIN}","#);
        let errors = [parse_line(&bad_ts, 7), parse_line(&truncated, 7)].map(Result::unwrap_err);
        assert_eq!(errors[0], ParseError::MissingEnvelope { line: 7 });
        assert!(matches!(errors[1], ParseError::Malformed { line: 7, .. }));
        for err in errors {
            assert!(!err.to_string().contains(VIN));
            assert!(!format!("{err:?}").contains(VIN));
        }
    }

    #[test]
    fn rejects_missing_or_invalid_envelope() {
        for line in [
            r#"{"event_id":"","vin":"V","event_type":"ota_status","ts_ms":1}"#,
            r#"{"event_id":"e","vin":null,"event_type":"ota_status","ts_ms":1}"#,
            r#"{"event_id":"e","vin":"V","ts_ms":1}"#,
            r#"{"event_id":"e","vin":"V","event_type":"ota_status"}"#,
            r#"{"event_id":"e","vin":"V","event_type":"ota_status","ts_ms":1.5}"#,
            r#"{"event_id":"e","vin":"V","event_type":"ota_status","ts_ms":1e3}"#,
        ] {
            assert_eq!(
                parse_line(line, 3),
                Err(ParseError::MissingEnvelope { line: 3 })
            );
        }
    }

    #[test]
    fn rejects_non_objects() {
        for line in ["[1,2]", "42", "", "not json"] {
            assert!(matches!(
                parse_line(line, 2),
                Err(ParseError::Malformed { line: 2, .. })
            ));
        }
    }

    #[test]
    fn incomplete_location_keeps_the_event() {
        for (extra, lat, lon) in [
            (r#","location":{}"#, None, None),
            (r#","location":{"lat":1.5}"#, Some(1.5), None),
            (r#","location":{"lat":"n","lon":2.0}"#, None, Some(2.0)),
        ] {
            let event = parse_line(&ota(extra), 1).unwrap();
            assert!(
                event.location == Some(Location { lat, lon }),
                "location not parsed as expected"
            );
            assert_eq!(event.status.as_deref(), Some("installing"));
        }
    }

    #[test]
    fn invalid_optional_fields_are_treated_as_absent() {
        let event = parse_line(
            &ota(r#","soc_pct":"abc","energy_kwh":[1],"location":"here","extra":{"a":1}"#),
            1,
        )
        .unwrap();
        assert_eq!(event.soc_pct, None);
        assert_eq!(event.energy_kwh, None);
        assert!(event.location.is_none(), "non-object location kept");
        assert_eq!(event.status.as_deref(), Some("installing"));
    }

    #[test]
    fn string_fields_take_the_json_text_of_other_values() {
        let line = r#"{"event_id":8,"vin":"V","event_type":"ota_status","ts_ms":1,"campaign_id":true,"software_version":7,"status":{"a":[1,"b"]}}"#;
        let event = parse_line(line, 1).unwrap();
        assert_eq!(event.event_id, "8");
        assert_eq!(event.campaign_id.as_deref(), Some("true"));
        assert_eq!(event.software_version.as_deref(), Some("7"));
        assert_eq!(event.status.as_deref(), Some(r#"{"a":[1,"b"]}"#));
        let null_status = parse_line(&ota(r#","status":null"#), 1).unwrap();
        assert_eq!(null_status.status, None);
    }

    #[test]
    fn last_repeated_key_wins_and_trailing_text_is_ignored() {
        let event = parse_line(&format!(r#"{} trailing"#, ota(r#","status":"done""#)), 1).unwrap();
        assert_eq!(event.status.as_deref(), Some("done"));
    }

    #[test]
    fn debug_hides_vin_and_location() {
        let line = format!(
            r#"{{"event_id":"e1","vin":"{VIN}","event_type":"charging_start","ts_ms":1,"location":{{"lat":52.52,"lon":13.405}}}}"#
        );
        let text = format!("{:?}", parse_line(&line, 1).unwrap());
        assert!(!text.contains(VIN));
        assert!(!text.contains("52.52"));
        assert!(!text.contains("13.405"));
    }
}
