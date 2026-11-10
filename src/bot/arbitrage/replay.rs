//! Local replay and proxy driver for the btc-arb decision core.
//!
//! The driver reads an explicit event stream on an explicit clock and calls the
//! same [`DecisionCore`](super::DecisionCore) the live tick calls. It never
//! opens a socket, reads a trading config, or signs anything: the only inputs
//! are the files the caller names.
//!
//! Two modes share the core:
//!
//! - `replay` walks market identity, state, book, spot, tick, result and label
//!   events and decides entries the way the live tick would.
//! - `proxy` walks closed bars only and produces model valuations. It has no
//!   market, no book and no execution, so it can never make a trade.
//!
//! This file is the public facade: it decodes the JSONL stream, drives the
//! state machine in [`driver`](self::driver) and writes the decision rows.
//! The wire types live in [`types`](self::types).

mod driver;
mod types;

#[cfg(test)]
mod tests;

pub use types::*;

use driver::Driver;

// ----------------------------------------------------------------- driver ---

/// Walk an event stream and produce the decision rows it implies.
///
/// Events are processed in `[from_ms, to_ms)`; anything outside that window is
/// not part of this run. The same input always produces the same output.
pub fn run<R: std::io::BufRead, W: std::io::Write>(
    config: ReplayConfig,
    mode: ReplayMode,
    from_ms: i64,
    to_ms: i64,
    input: R,
    mut output: W,
) -> Result<ReplaySummary, ReplayError> {
    let mut summary = ReplaySummary::new(mode);
    let mut rows: Vec<OutputRow> = Vec::new();
    let mut driver = Driver::new(config, mode);

    let mut line_number: u64 = 0;
    let mut buffer = String::new();
    let mut reader = input;
    loop {
        buffer.clear();
        let read = reader.read_line(&mut buffer).map_err(|_| ReplayError {
            exit_code: EXIT_IO,
            code: "io_failure".to_string(),
            line: line_number,
            partial: summary.clone(),
        })?;
        if read == 0 {
            break;
        }
        line_number += 1;
        let line = buffer.trim_end_matches(['\n', '\r']);
        if line.trim().is_empty() {
            return Err(failure(
                EXIT_CONTRACT,
                "json_invalid",
                line_number,
                "empty line",
                summary,
                rows.len(),
            ));
        }
        let envelope: EventEnvelope = match serde_json::from_str(line) {
            Ok(envelope) => envelope,
            Err(err) => {
                return Err(failure(
                    EXIT_CONTRACT,
                    "json_invalid",
                    line_number,
                    &err.to_string(),
                    summary,
                    rows.len(),
                ))
            }
        };
        summary.event_count += 1;
        if envelope.available_at_ms < from_ms || envelope.available_at_ms >= to_ms {
            // Outside this run's window: not part of the stream.
            continue;
        }
        match driver.step(envelope, line_number, &mut summary) {
            Ok(Some(row)) => rows.push(row),
            Ok(None) => {}
            Err(err) => {
                return Err(failure(
                    err.exit_code,
                    &err.code,
                    line_number,
                    &err.message,
                    summary,
                    rows.len(),
                ))
            }
        }
    }

    driver.finish(&mut summary);
    write_rows(&mut output, &rows).map_err(|_| ReplayError {
        exit_code: EXIT_IO,
        code: "io_failure".to_string(),
        line: line_number,
        partial: summary.clone(),
    })?;
    summary.output_count = rows.len() as u64;
    summary.completed = true;
    Ok(summary)
}

/// Build a failure with what the run had produced.
fn failure(
    exit_code: u8,
    code: &str,
    line: u64,
    message: &str,
    mut summary: ReplaySummary,
    output_count: usize,
) -> ReplayError {
    summary.output_count = output_count as u64;
    summary.completed = false;
    summary.exit_code = exit_code;
    let _ = message;
    ReplayError {
        exit_code,
        code: code.to_string(),
        line,
        partial: summary,
    }
}

/// Write the decision rows, one JSON object per line.
fn write_rows<W: std::io::Write>(
    output: &mut W,
    rows: &[OutputRow],
) -> std::io::Result<()> {
    for row in rows {
        serde_json::to_writer(&mut *output, row)?;
        output.write_all(b"\n")?;
    }
    output.flush()
}
