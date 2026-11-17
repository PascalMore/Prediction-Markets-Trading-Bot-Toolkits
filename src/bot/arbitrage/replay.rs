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
//! Two wire versions share the driver:
//!
//! - `btc-arb-replay/1` is the original cash-fee version.
//! - `btc-arb-replay/2` is the net-share version. It carries the elapsed
//!   clock and the acquisition clock of every input, and it runs the same core
//!   under the net-share economics.
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

#[cfg(test)]
pub(crate) use driver::Driver as FixtureDriver;

// ----------------------------------------------------------------- driver ---

/// Walk an event stream and produce the decision rows it implies.
///
/// Events are processed in `[from_ms, to_ms)`; anything outside that window is
/// not part of this run. The same input always produces the same output.
///
/// The configuration chooses the wire version, and the version is checked
/// before the input is read: an unknown version, a mixed stream or the
/// net-share version in proxy mode is refused before anything is decided.
pub fn run<R: std::io::BufRead, W: std::io::Write>(
    config: ReplayConfig,
    mode: ReplayMode,
    from_ms: i64,
    to_ms: i64,
    input: R,
    mut output: W,
) -> Result<ReplaySummary, ReplayError> {
    let version = config.version().unwrap_or(ReplayVersion::V1);
    let mut summary = ReplaySummary::new(mode, version);
    config.validate_for(mode).map_err(|message| {
        failure(
            EXIT_CONTRACT,
            "config_invalid",
            0,
            &message,
            summary.clone(),
            0,
        )
    })?;
    let mut rows: Vec<OutputRow> = Vec::new();
    let mut decisions: Vec<Option<DecisionDiagnostic>> = Vec::new();
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
        // Decode only the version selected by the validated configuration.
        let event = match version {
            ReplayVersion::V1 => decode_v1(line, line_number, &summary, rows.len()),
            ReplayVersion::V2 => decode_v2(line, line_number, &summary, rows.len()),
        };
        let event = match event {
            Ok(event) => event,
            Err(error) => return Err(error),
        };
        summary.event_count += 1;
        if event.available_at_ms < from_ms || event.available_at_ms >= to_ms {
            // Outside this run's window: not part of the stream.
            continue;
        }
        match driver.step(event, line_number, &mut summary) {
            Ok(Some(row)) => {
                rows.push(row.base);
                decisions.push(row.decision);
            }
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
    write_rows(&mut output, &rows, &decisions, version).map_err(|_| ReplayError {
        exit_code: EXIT_IO,
        code: "io_failure".to_string(),
        line: line_number,
        partial: summary.clone(),
    })?;
    summary.output_count = rows.len() as u64;
    summary.completed = true;
    Ok(summary)
}

/// Decode one row of the original version.
fn decode_v1(
    line: &str,
    line_number: u64,
    summary: &ReplaySummary,
    output_count: usize,
) -> Result<StreamEvent, ReplayError> {
    match serde_json::from_str::<EventEnvelope>(line) {
        Ok(envelope) => Ok(envelope.normalize()),
        Err(err) => Err(failure(
            EXIT_CONTRACT,
            "json_invalid",
            line_number,
            &err.to_string(),
            summary.clone(),
            output_count,
        )),
    }
}

/// Decode one row of the net-share version.
fn decode_v2(
    line: &str,
    line_number: u64,
    summary: &ReplaySummary,
    output_count: usize,
) -> Result<StreamEvent, ReplayError> {
    let envelope = match serde_json::from_str::<EventEnvelopeV2>(line) {
        Ok(envelope) => envelope,
        Err(err) => {
            return Err(failure(
                EXIT_CONTRACT,
                "json_invalid",
                line_number,
                &err.to_string(),
                summary.clone(),
                output_count,
            ))
        }
    };
    if envelope.schema_version != SCHEMA_VERSION_V2 {
        return Err(failure(
            EXIT_CONTRACT,
            "schema_invalid",
            line_number,
            "mixed replay version",
            summary.clone(),
            output_count,
        ));
    }
    envelope.normalize().map_err(|message| {
        failure(
            EXIT_CONTRACT,
            "value_invalid",
            line_number,
            &message,
            summary.clone(),
            output_count,
        )
    })
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
///
/// The original version writes the row alone. The net-share version adds the
/// decision snapshot of the row and the strategy contract; a row that is not
/// a decision carries `null` for the snapshot.
fn write_rows<W: std::io::Write>(
    output: &mut W,
    rows: &[OutputRow],
    decisions: &[Option<DecisionDiagnostic>],
    version: ReplayVersion,
) -> std::io::Result<()> {
    for (index, row) in rows.iter().enumerate() {
        match version {
            ReplayVersion::V1 => serde_json::to_writer(&mut *output, row)?,
            ReplayVersion::V2 => {
                let wrapped = OutputRowV2 {
                    base: row.clone(),
                    decision: decisions.get(index).cloned().flatten(),
                    strategy_contract: Some(StrategyContract::Phase1NetShares),
                };
                serde_json::to_writer(&mut *output, &wrapped)?;
            }
        }
        output.write_all(b"\n")?;
    }
    output.flush()
}
