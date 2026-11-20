//! De-duplication by `event_id`: the first occurrence wins.

use std::collections::HashSet;

use crate::event::VehicleEvent;

/// Remembers seen event ids across calls, e.g. across input batches.
#[derive(Debug, Default)]
pub struct Deduplicator {
    seen: HashSet<String>,
}

impl Deduplicator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` the first time an `event_id` is seen.
    pub fn first_seen(&mut self, event: &VehicleEvent) -> bool {
        self.seen.insert(event.event_id.clone())
    }
}

/// Drops events whose `event_id` already appeared earlier in `events`.
pub fn dedup_by_event_id(events: impl IntoIterator<Item = VehicleEvent>) -> Vec<VehicleEvent> {
    let mut dedup = Deduplicator::new();
    events.into_iter().filter(|e| dedup.first_seen(e)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::parse_line;

    fn event(id: &str, ts: i64) -> VehicleEvent {
        let line =
            format!(r#"{{"event_id":"{id}","vin":"V1","event_type":"ota_status","ts_ms":{ts}}}"#);
        parse_line(&line, 1).unwrap()
    }

    #[test]
    fn keeps_first_occurrence() {
        let out = dedup_by_event_id([event("a", 1), event("b", 2), event("a", 3)]);
        let kept: Vec<_> = out.iter().map(|e| (e.event_id.as_str(), e.ts_ms)).collect();
        assert_eq!(kept, [("a", 1), ("b", 2)]);
    }

    #[test]
    fn remembers_across_batches() {
        let mut dedup = Deduplicator::new();
        assert!(dedup.first_seen(&event("a", 1)));
        assert!(!dedup.first_seen(&event("a", 1)));
    }
}
