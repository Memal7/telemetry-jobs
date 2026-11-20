## Summary

<!-- What changed and why. For a port: which legacy job it replaces. -->

## Fields read and written

<!-- Required for every port. List every input field read and every output field written. -->

| Direction | Field | Notes |
| --- | --- | --- |
| read | | |
| written | | |

## VIN and location handling

<!-- Where VIN and location are read, whether they are written, and how they are kept out of logs and error messages. -->

## Parity

- [ ] Parity test replays `fixtures/<job>/input.jsonl` and matches `fixtures/<job>/expected.jsonl`
- [ ] `cargo fmt --check`, `cargo clippy -D warnings` and `cargo test` pass
