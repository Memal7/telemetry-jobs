//! Shared building blocks for telemetry processors: event parsing,
//! de-duplication by `event_id`, per-VIN ordering and PII redaction.

pub mod dedup;
pub mod event;
pub mod ordering;
pub mod redact;

pub use dedup::{dedup_by_event_id, Deduplicator};
pub use event::{parse_line, EventType, ParseError, VehicleEvent};
pub use ordering::group_by_vin_ordered;
pub use redact::{Location, Redacted, Vin};
