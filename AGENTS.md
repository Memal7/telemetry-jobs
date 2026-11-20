# AGENTS.md

Sample of a telemetry pipeline migration from Spark Structured Streaming
(Scala, `legacy/`) to Rust (`processors/`). Demo material: all data is
synthetic.

## Layout

- `legacy/` — Spark jobs being migrated: `OtaStatusJob`, `ChargingSessionJob`.
- `processors/` — Rust workspace.
  - `common/` — event parsing, de-duplication by `event_id`, per-VIN
    ordering, `Vin`/`Location`/`Redacted` types that never print their value.
  - `ota-status/` — Rust port of `OtaStatusJob`; the reference port.
- `fixtures/<job>/` — `input.jsonl` (synthetic events, incl. out-of-order,
  duplicate and malformed lines) and `expected.jsonl` (legacy job output).

## Commands

Run from `processors/`; CI runs the same:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Porting rules

1. **Follow `ota-status`.** Same structure: a library with a pure transform
   and a `run(input, output)` function, a thin `main.rs`, a
   `tests/parity.rs`. Use `common` for parsing, de-duplication and ordering;
   do not re-implement them.
2. **Keep per-VIN order.** Process each VIN's events in event order
   (`ts_ms`, then `event_id`) via `common::group_by_vin_ordered`, never in
   arrival order.
3. **De-duplicate by `event_id`.** Use `common::dedup_by_event_id` or
   `common::Deduplicator`; the first occurrence wins.
4. **Never log VIN or location.** Keep them in `common::Vin` and
   `common::Location` (or `common::Redacted`) end to end. Call `expose()`
   only to build output records, never in `println!`/`eprintln!`/log macros,
   error messages, panics or test assertion messages. Log counters only.
   In tests, compare exposed values with `assert!(a == b, "fixed message")`,
   not `assert_eq!`, which prints both values when it fails.
5. **A port is done only when its parity test passes.** The parity test
   replays `fixtures/<job>/input.jsonl` and compares the output with
   `fixtures/<job>/expected.jsonl`. Do not edit `expected.jsonl` to make a
   port pass; it is the legacy job's behaviour.
6. **The pull request lists every field read and written** and how VIN and
   location are handled (see `.github/pull_request_template.md`).

## Fixtures

Synthetic only: VINs look like `SYNTHVIN000000001`, locations are public
city-centre coordinates. Never add real vehicle data.
