//! Vehicle event envelope shared by all jobs, parsed from one JSON line.

use std::fmt;

use serde::Deserialize;

use crate::redact::{Location, Vin};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    OtaStatus,
    ChargingStart,
    ChargingStop,
    #[serde(other)]
    Other,
}

/// One vehicle event. Envelope fields are required; payload fields are
/// optional and validated by each job. `Debug` never shows VIN or location.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct VehicleEvent {
    pub event_id: String,
    pub vin: Vin,
    pub event_type: EventType,
    pub ts_ms: i64,
    #[serde(default)]
    pub campaign_id: Option<String>,
    #[serde(default)]
    pub software_version: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub soc_pct: Option<f64>,
    #[serde(default)]
    pub energy_kwh: Option<f64>,
    #[serde(default)]
    pub location: Option<Location>,
}

/// Why a line was rejected. Holds no input text, so it is safe to log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Not valid JSON, or a field has the wrong type.
    Malformed { line: usize, column: usize },
    /// `event_id` or `vin` is empty.
    MissingKey { line: usize },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed { line, column } => {
                write!(f, "malformed event on input line {line}, column {column}")
            }
            Self::MissingKey { line } => {
                write!(f, "event on input line {line} has an empty event_id or vin")
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// Parses one JSON line. `line_no` is 1-based and only used for errors.
///
/// serde_json messages can quote input values, so they are dropped and only
/// the position is kept.
pub fn parse_line(text: &str, line_no: usize) -> Result<VehicleEvent, ParseError> {
    let event: VehicleEvent = serde_json::from_str(text).map_err(|err| ParseError::Malformed {
        line: line_no,
        column: err.column(),
    })?;
    if event.event_id.is_empty() || event.vin.is_empty() {
        return Err(ParseError::MissingKey { line: line_no });
    }
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIN: &str = "TESTVIN0000000001";

    #[test]
    fn parses_ota_event() {
        let line = format!(
            r#"{{"event_id":"e1","vin":"{VIN}","event_type":"ota_status","ts_ms":10,"campaign_id":"c1","software_version":"1.2.0","status":"installing"}}"#
        );
        let event = parse_line(&line, 1).unwrap();
        assert_eq!(event.event_type, EventType::OtaStatus);
        assert_eq!(event.vin.expose(), VIN);
        assert_eq!(event.status.as_deref(), Some("installing"));
        assert_eq!(event.location, None);
    }

    #[test]
    fn unknown_event_type_maps_to_other() {
        let line =
            format!(r#"{{"event_id":"e1","vin":"{VIN}","event_type":"door_open","ts_ms":1}}"#);
        assert_eq!(parse_line(&line, 1).unwrap().event_type, EventType::Other);
    }

    #[test]
    fn errors_do_not_contain_input() {
        let line =
            format!(r#"{{"event_id":"e1","vin":"{VIN}","event_type":"ota_status","ts_ms":"x"}}"#);
        let err = parse_line(&line, 7).unwrap_err();
        assert!(matches!(err, ParseError::Malformed { line: 7, .. }));
        assert!(!err.to_string().contains(VIN));
        assert!(!format!("{err:?}").contains(VIN));
    }

    #[test]
    fn rejects_empty_keys() {
        let line = r#"{"event_id":"","vin":"V","event_type":"ota_status","ts_ms":1}"#;
        assert_eq!(parse_line(line, 3), Err(ParseError::MissingKey { line: 3 }));
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
