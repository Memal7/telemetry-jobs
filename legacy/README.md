# Legacy Spark jobs

Spark 3.5 Structured Streaming jobs (Scala 2.12) reading vehicle events as
JSON from Kafka. These are the behaviour to preserve; see `../AGENTS.md`.

| Job | Output topic | Mode | Semantics |
| --- | --- | --- | --- |
| `OtaStatusJob` | `ota-status-latest`, keyed by VIN | update | Latest `ota_status` event per VIN by (`ts_ms`, `event_id`), duplicates by `event_id` dropped |
| `ChargingSessionJob` | `charging-sessions`, keyed by VIN | append | One record per start/stop pair, in event order per VIN, duplicates by `event_id` dropped |

Configuration (environment): `KAFKA_BOOTSTRAP_SERVERS`, `INPUT_TOPIC`,
`OUTPUT_TOPIC`, `CHECKPOINT_LOCATION`, `WATERMARK` (default `10 minutes`).

```sh
sbt package
spark-submit --class com.example.telemetry.OtaStatusJob target/scala-2.12/telemetry-legacy-jobs_2.12-0.1.0-SNAPSHOT.jar
```

`FixtureReplay` runs a job's transform over a fixture as a bounded batch and
prints the final output, which is how `fixtures/*/expected.jsonl` is produced:

```sh
sbt "runMain com.example.telemetry.FixtureReplay ota-status ../fixtures/ota-status/input.jsonl"
```

`sbt test` runs `ChargingSessionJob` as a real streaming query (memory source
and sink) to cover event-time state, watermarks and the 24 h session limit.
