//! Reads vehicle events as JSON lines on stdin and writes one charging
//! session per line on stdout. Logs only counters.

use std::io::{self, BufWriter};
use std::process::ExitCode;

fn main() -> ExitCode {
    let stdin = io::stdin().lock();
    let stdout = BufWriter::new(io::stdout().lock());
    match charging_session::run(stdin, stdout) {
        Ok(s) => {
            eprintln!(
                "charging-session: lines={} malformed={} ignored={} duplicates={} vins={} \
                 sessions={} too_long={} unmatched_stops={} open_starts={}",
                s.lines,
                s.malformed,
                s.ignored,
                s.duplicates,
                s.vins,
                s.sessions,
                s.too_long,
                s.unmatched_stops,
                s.open_starts
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("charging-session: I/O error: {}", err.kind());
            ExitCode::FAILURE
        }
    }
}
