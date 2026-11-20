# telemetry-jobs

Small sample of a telemetry pipeline migration: Spark Structured Streaming
jobs in Scala (`legacy/`) being ported to Rust (`processors/`), with shared
synthetic fixtures (`fixtures/`) and parity tests. All data is synthetic.

| Job | Legacy (Scala) | Rust port | Fixtures |
| --- | --- | --- | --- |
| Latest OTA status per VIN | `legacy/.../OtaStatusJob.scala` | `processors/ota-status` | `fixtures/ota-status` |
| Charging sessions | `legacy/.../ChargingSessionJob.scala` | `processors/charging-session` | `fixtures/charging-session` |

## Rust

```sh
cd processors
cargo test --workspace
cargo run -q -p ota-status < ../fixtures/ota-status/input.jsonl
cargo run -q -p charging-session < ../fixtures/charging-session/input.jsonl
```

## Legacy

Needs sbt and a Spark 3.5 runtime. Jobs read `vehicle-events` from Kafka and
write JSON to an output topic; see `legacy/README.md`.

See [AGENTS.md](AGENTS.md) for the porting rules.
