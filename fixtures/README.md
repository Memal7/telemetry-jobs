# Fixtures

Synthetic vehicle events, one JSON object per line, in Kafka arrival order.
Expected outputs are the final state of each legacy job after replaying the
input, one record per line, sorted (`ota-status` by `vin`;
`charging-session` by `vin`, `start_ts_ms`).

Event envelope: `event_id`, `vin`, `event_type`, `ts_ms` (epoch ms).
Payload: `campaign_id`, `software_version`, `status` (OTA);
`soc_pct`, `energy_kwh`, `location {lat, lon}` (charging).

Parsing follows Spark `from_json` in permissive mode: a payload field with the
wrong type is treated as absent instead of dropping the event, and string
fields accept any JSON value as its text.

## ota-status

| VIN | Case covered |
| --- | --- |
| `…001` | Out-of-order arrival, duplicate `e-ota-003`, a charging event to ignore |
| `…002` | Older `failed` event arrives after the newer one and must not win; duplicate `e-ota-011` |
| `…003` | Two events with the same `ts_ms`; the greater `event_id` wins |
| `…004` | `ota_status` without `status`; dropped, so no output |
| `…005` | Truncated JSON line skipped; one valid event |
| `…006` | `location` empty or missing `lon`; events still count |
| `…007` | Unused payload fields with the wrong type (`soc_pct`, `energy_kwh`, `location`); treated as absent |
| `…008` | Numeric `software_version` becomes the string `"5"`; non-numeric `lat` |

## charging-session

| VIN | Case covered |
| --- | --- |
| `…001` | Stop arrives before its start; duplicate stop; two sessions; an OTA event to ignore |
| `…002` | Stop without a start ignored; a second start replaces the first; duplicate start |
| `…003` | Line with a non-numeric `ts_ms` skipped; start without stop yields no session |
| `…004` | Stop 25 h after its start: no session, and the start is consumed so the next stop is ignored |
| `…005` | Start with `location: {}`; session keeps the empty location |
