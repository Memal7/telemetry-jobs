//! Parity test: replays the shared fixture through the Rust port and compares
//! the result with the legacy job's expected output, record by record.

use std::fs::{self, File};
use std::io::BufReader;
use std::path::PathBuf;

use serde_json::Value;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/ota-status")
        .join(name)
}

fn parse_jsonl(text: &str) -> Vec<Value> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("expected output must be valid JSON"))
        .collect()
}

#[test]
fn ota_status_matches_legacy_expected_output() {
    let input = BufReader::new(File::open(fixture("input.jsonl")).expect("input fixture"));
    let mut out = Vec::new();
    ota_status::run(input, &mut out).expect("run ota-status");

    let actual = parse_jsonl(&String::from_utf8(out).unwrap());
    let expected = parse_jsonl(&fs::read_to_string(fixture("expected.jsonl")).unwrap());

    assert!(!expected.is_empty(), "expected output fixture is empty");
    assert_eq!(actual.len(), expected.len(), "record count differs");
    for (i, (a, e)) in actual.iter().zip(&expected).enumerate() {
        assert!(
            a == e,
            "record {i} differs in fields {:?}",
            differing_fields(a, e)
        );
    }
}

/// Names of top-level fields that differ. Values are not printed so that a
/// failing test never logs VIN or location.
fn differing_fields(a: &Value, e: &Value) -> Vec<String> {
    let (Some(a), Some(e)) = (a.as_object(), e.as_object()) else {
        return vec!["<not an object>".to_string()];
    };
    let mut keys: Vec<&String> = a.keys().chain(e.keys()).collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter(|k| a.get(*k) != e.get(*k))
        .cloned()
        .collect()
}
