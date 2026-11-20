//! Per-VIN event ordering.

use std::collections::BTreeMap;

use crate::event::VehicleEvent;
use crate::redact::Vin;

/// Groups events by VIN and sorts each group in event order: by `ts_ms`,
/// then by `event_id` to break ties deterministically. Arrival order is
/// irrelevant. VINs iterate in ascending order.
pub fn group_by_vin_ordered(
    events: impl IntoIterator<Item = VehicleEvent>,
) -> BTreeMap<Vin, Vec<VehicleEvent>> {
    let mut groups: BTreeMap<Vin, Vec<VehicleEvent>> = BTreeMap::new();
    for event in events {
        groups.entry(event.vin.clone()).or_default().push(event);
    }
    for group in groups.values_mut() {
        group.sort_by(|a, b| {
            a.ts_ms
                .cmp(&b.ts_ms)
                .then_with(|| a.event_id.cmp(&b.event_id))
        });
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::parse_line;

    fn event(id: &str, vin: &str, ts: i64) -> VehicleEvent {
        let line = format!(
            r#"{{"event_id":"{id}","vin":"{vin}","event_type":"ota_status","ts_ms":{ts}}}"#
        );
        parse_line(&line, 1).unwrap()
    }

    #[test]
    fn orders_each_vin_by_time_then_id() {
        let groups = group_by_vin_ordered([
            event("c", "V2", 5),
            event("b", "V1", 20),
            event("z", "V1", 10),
            event("a", "V1", 10),
        ]);
        assert!(
            groups.keys().map(Vin::expose).eq(["V1", "V2"]),
            "VINs not in ascending order"
        );
        let v1: Vec<_> = groups[&Vin::new("V1")]
            .iter()
            .map(|e| e.event_id.as_str())
            .collect();
        assert_eq!(v1, ["a", "z", "b"]);
    }
}
