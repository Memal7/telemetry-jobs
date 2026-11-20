//! Reads vehicle events as JSON lines on stdin and writes the latest OTA
//! status per VIN as JSON lines on stdout. Logs only counters.

use std::io::{self, BufWriter};
use std::process::ExitCode;

fn main() -> ExitCode {
    let stdin = io::stdin().lock();
    let stdout = BufWriter::new(io::stdout().lock());
    match ota_status::run(stdin, stdout) {
        Ok(stats) => {
            eprintln!(
                "ota-status: lines={} malformed={} ignored={} duplicates={} vins={}",
                stats.lines, stats.malformed, stats.ignored, stats.duplicates, stats.vins
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("ota-status: I/O error: {}", err.kind());
            ExitCode::FAILURE
        }
    }
}
