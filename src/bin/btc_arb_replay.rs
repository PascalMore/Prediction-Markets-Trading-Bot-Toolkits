//! Local replay CLI for the btc-arb decision core.
//!
//! It reads a local event file and writes local decision rows. It has no
//! live, network, venue, RPC or wallet option: the only inputs are the files
//! named on the command line, and the only outputs are the files it is told to
//! create. Passing an unknown option fails with exit code 64.

use polymarket_toolkits::bot::arbitrage::replay::{
    run, ReplayConfig, ReplayError, ReplayMode, SCHEMA_VERSION,
};
use serde_json::json;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Exit code for a usage, path or time-range error.
const EXIT_USAGE: u8 = 64;

const USAGE: &str = "\
btc_arb_replay — replay local BTC 5m round events against the shared decision core

USAGE:
    btc_arb_replay --mode <replay|proxy> --config <PATH> --input <PATH|-> \\
                   --output <PATH|-> --summary <PATH> --from-ms <I64> --to-ms <I64>

OPTIONS:
    --mode <replay|proxy>   Which stream to walk
    --config <PATH>        A local JSON configuration file
    --input <PATH|->       A local JSONL event file, or - for standard input
    --output <PATH|->      Where to write the decision rows, or - for standard output
    --summary <PATH>       Where to write the run summary
    --from-ms <I64>        The first event time to process, in UTC milliseconds
    --to-ms <I64>          The exclusive last event time, in UTC milliseconds
    -h, --help             Print this help
    -V, --version          Print the version

EXIT CODES:
    0   The run completed with no unexplained missing evidence
    2   The input violated the schema, identity, time or result contract
    3   The input was legal but lacked the evidence to decide
    5   An output or input I/O failure
   64   A usage, path or time-range error
";

/// The parsed command line.
#[derive(Debug)]
struct Args {
    mode: ReplayMode,
    config: PathBuf,
    input: PathBuf,
    output: PathBuf,
    summary: PathBuf,
    from_ms: i64,
    to_ms: i64,
}

/// A usage failure, before any work is done.
struct UsageError(String);

fn main() {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(Some(args)) => args,
        Ok(None) => return,
        Err(UsageError(message)) => {
            eprintln!("usage_invalid: {message}");
            eprint!("{USAGE}");
            std::process::exit(EXIT_USAGE as i32);
        }
    };

    let config = match read_config(&args.config) {
        Ok(config) => config,
        Err(UsageError(message)) => {
            eprintln!("usage_invalid: {message}");
            std::process::exit(EXIT_USAGE as i32);
        }
    };
    if let Err(message) = config.validate() {
        eprintln!("usage_invalid: {message}");
        std::process::exit(EXIT_USAGE as i32);
    }
    let paths = [
        (&args.config, "config"),
        (&args.input, "input"),
        (&args.output, "output"),
        (&args.summary, "summary"),
    ];
    for (path, name) in paths {
        if path.as_os_str() == "-" && name != "input" && name != "output" {
            eprintln!("usage_invalid: {name} must be a real file path");
            std::process::exit(EXIT_USAGE as i32);
        }
    }
    let distinct = [
        (&args.config, "config"),
        (&args.input, "input"),
        (&args.output, "output"),
        (&args.summary, "summary"),
    ];
    for (index, (left, left_name)) in distinct.iter().enumerate() {
        for (right, right_name) in distinct.iter().skip(index + 1) {
            if left.as_os_str() == "-" || right.as_os_str() == "-" {
                continue;
            }
            if same_path(left, right) {
                eprintln!(
                    "usage_invalid: {left_name} and {right_name} must not be the same path"
                );
                std::process::exit(EXIT_USAGE as i32);
            }
        }
    }

    let input: Box<dyn Read> = if args.input.as_os_str() == "-" {
        Box::new(std::io::stdin())
    } else {
        match File::open(&args.input) {
            Ok(file) => Box::new(file),
            Err(err) => {
                eprintln!("usage_invalid: input: {err}");
                std::process::exit(EXIT_USAGE as i32);
            }
        }
    };

    // Outputs are created, never overwritten: an existing target is a stop.
    // Both are opened before the run so a bad path fails before any work.
    let summary_file = match open_new(&args.summary) {
        Ok(file) => file,
        Err(message) => {
            eprintln!("usage_invalid: summary: {message}");
            std::process::exit(EXIT_USAGE as i32);
        }
    };
    let output_file = match open_new(&args.output) {
        Ok(file) => file,
        Err(message) => {
            eprintln!("usage_invalid: output: {message}");
            std::process::exit(EXIT_USAGE as i32);
        }
    };
    let mut output: Box<dyn Write> = match output_file {
        Some(file) => Box::new(file),
        None => Box::new(std::io::stdout()),
    };

    let result = run(
        config,
        args.mode,
        args.from_ms,
        args.to_ms,
        BufReader::new(input),
        &mut output,
    );

    let summary = match result {
        Ok(summary) => summary,
        Err(error) => {
            eprintln!("{}: line {}", error.code, error.line);
            let _ = write_summary(summary_file, &error.partial);
            std::process::exit(error.exit_code as i32);
        }
    };
    if let Err(err) = output.flush() {
        eprintln!("io_failure: {err}");
        std::process::exit(5);
    }
    if let Err(err) = write_summary(summary_file, &summary) {
        eprintln!("io_failure: {err}");
        std::process::exit(5);
    }
    std::process::exit(summary.exit_code as i32);
}

/// Write the summary file, as strict JSON.
fn write_summary(
    file: Option<File>,
    summary: &polymarket_toolkits::bot::arbitrage::replay::ReplaySummary,
) -> Result<(), std::io::Error> {
    let file = match file {
        Some(file) => file,
        None => return Ok(()),
    };
    let mut writer = BufWriter::new(file);
    let value = serde_json::to_value(summary).unwrap_or_else(|_| json!({}));
    serde_json::to_writer(&mut writer, &value)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// Open a file for writing, refusing to overwrite anything that exists.
fn open_new(path: &Path) -> Result<Option<File>, String> {
    if path.as_os_str() == "-" {
        return Ok(None);
    }
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() {
            return Err("a symbolic link is not an acceptable output".to_string());
        }
        return Err("the file already exists".to_string());
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err("the parent directory does not exist".to_string());
    }
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => Ok(Some(file)),
        Err(err) => Err(err.to_string()),
    }
}

/// Whether two paths name the same file.
fn same_path(left: &Path, right: &Path) -> bool {
    match (std::fs::canonicalize(left), std::fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

/// Read and parse the configuration file.
fn read_config(path: &Path) -> Result<ReplayConfig, UsageError> {
    let text = std::fs::read_to_string(path)
        .map_err(|err| UsageError(format!("config: {err}")))?;
    serde_json::from_str::<ReplayConfig>(&text).map_err(|err| UsageError(format!("config: {err}")))
}

/// Parse the command line.
fn parse_args(
    args: impl Iterator<Item = String>,
) -> Result<Option<Args>, UsageError> {
    let mut mode: Option<ReplayMode> = None;
    let mut config: Option<PathBuf> = None;
    let mut input: Option<PathBuf> = None;
    let mut output: Option<PathBuf> = None;
    let mut summary: Option<PathBuf> = None;
    let mut from_ms: Option<i64> = None;
    let mut to_ms: Option<i64> = None;

    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(None);
            }
            "-V" | "--version" => {
                println!("btc_arb_replay {SCHEMA_VERSION}");
                return Ok(None);
            }
            "--mode" => {
                let value = take_value(&mut args, "--mode")?;
                mode = Some(match value.as_str() {
                    "replay" => ReplayMode::Replay,
                    "proxy" => ReplayMode::Proxy,
                    other => {
                        return Err(UsageError(format!(
                            "--mode must be replay or proxy, got {other}"
                        )))
                    }
                });
            }
            "--config" => config = Some(PathBuf::from(take_value(&mut args, "--config")?)),
            "--input" => input = Some(PathBuf::from(take_value(&mut args, "--input")?)),
            "--output" => output = Some(PathBuf::from(take_value(&mut args, "--output")?)),
            "--summary" => summary = Some(PathBuf::from(take_value(&mut args, "--summary")?)),
            "--from-ms" => {
                let value = take_value(&mut args, "--from-ms")?;
                from_ms = Some(value.parse::<i64>().map_err(|_| {
                    UsageError(format!("--from-ms must be an integer, got {value}"))
                })?);
            }
            "--to-ms" => {
                let value = take_value(&mut args, "--to-ms")?;
                to_ms = Some(value.parse::<i64>().map_err(|_| {
                    UsageError(format!("--to-ms must be an integer, got {value}"))
                })?);
            }
            other => {
                return Err(UsageError(format!("unknown option {other}")));
            }
        }
    }

    let from_ms = from_ms.ok_or_else(|| UsageError("--from-ms is required".to_string()))?;
    let to_ms = to_ms.ok_or_else(|| UsageError("--to-ms is required".to_string()))?;
    validate_range(from_ms, to_ms)?;
    Ok(Some(Args {
        mode: mode.ok_or_else(|| UsageError("--mode is required".to_string()))?,
        config: config.ok_or_else(|| UsageError("--config is required".to_string()))?,
        input: input.ok_or_else(|| UsageError("--input is required".to_string()))?,
        output: output.ok_or_else(|| UsageError("--output is required".to_string()))?,
        summary: summary.ok_or_else(|| UsageError("--summary is required".to_string()))?,
        from_ms,
        to_ms,
    }))
}

/// Reject invalid UTC ranges before configuration, input or output I/O.
fn validate_range(from_ms: i64, to_ms: i64) -> Result<(), UsageError> {
    if from_ms >= to_ms {
        return Err(UsageError("--from-ms must be before --to-ms".to_string()));
    }
    if chrono::DateTime::<chrono::Utc>::from_timestamp_millis(from_ms).is_none()
        || chrono::DateTime::<chrono::Utc>::from_timestamp_millis(to_ms).is_none()
    {
        return Err(UsageError("--from-ms and --to-ms must be UTC instants".to_string()));
    }
    Ok(())
}

/// Take the value that follows an option.
fn take_value(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<String, UsageError> {
    args.next()
        .ok_or_else(|| UsageError(format!("{name} needs a value")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    fn range(from: i64, to: i64) -> Result<Option<Args>, UsageError> {
        parse_args([
            "--mode", "replay", "--config", "config.json", "--input", "events.jsonl",
            "--output", "output.jsonl", "--summary", "summary.json", "--from-ms",
            &from.to_string(), "--to-ms", &to.to_string(),
        ].into_iter().map(str::to_string))
    }

    #[test]
    fn fr04_rejects_unrepresentable_utc_endpoints() {
        for (from, to) in [(i64::MIN, 0), (0, i64::MAX), (i64::MIN, i64::MAX)] {
            assert!(range(from, to).is_err(), "accepted {from}..{to}");
        }
    }

    #[test]
    fn fr04_valid_utc_boundaries_and_ordering() {
        let first = DateTime::<Utc>::MIN_UTC.timestamp_millis();
        let last = DateTime::<Utc>::MAX_UTC.timestamp_millis();
        for (from, to) in [(0, 1), (1_762_636_800_000, 1_762_637_100_000),
            (first, first + 1), (last - 1, last), (first, last)] {
            assert!(range(from, to).is_ok());
        }
        for (from, to) in [(1, 0), (0, 0), (first - 1, 0), (0, last + 1)] {
            assert!(range(from, to).is_err(), "accepted {from}..{to}");
        }
    }
}

/// The error type the CLI reports.
impl From<ReplayError> for UsageError {
    fn from(error: ReplayError) -> Self {
        UsageError(error.to_string())
    }
}
