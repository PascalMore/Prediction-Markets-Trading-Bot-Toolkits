//! Local-only label-free research CLI; never constructs a broker or AppConfig.
use alloy_primitives::keccak256;
use polymarket_toolkits::bot::arbitrage::phase1_history::{Config, Event, History};
use serde_json::json;
use std::{collections::HashMap, fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write}, path::Path};

type Failure = (i32, &'static str);
const FLAGS: [&str; 6] = ["--config", "--input", "--output", "--summary", "--from-us", "--to-us"];
const MAX_LINE: usize = 65_536;

fn main() {
    let code = match execute(std::env::args().skip(1)) {
        Ok(code) => code, Err((code, message)) => { eprintln!("{message}"); code }
    };
    std::process::exit(code);
}

fn options(args: impl Iterator<Item = String>) -> Result<HashMap<String, String>, Failure> {
    let mut args = args;
    let mut result = HashMap::new();
    while let Some(key) = args.next() {
        if !FLAGS.contains(&key.as_str()) || result.contains_key(&key) {
            return Err((64, "unknown/duplicate option"));
        }
        result.insert(key, args.next().ok_or((64, "missing option value"))?);
    }
    if result.len() != FLAGS.len() { return Err((64, "missing required option")); }
    Ok(result)
}

fn execute(args: impl Iterator<Item = String>) -> Result<i32, Failure> {
    let args: Vec<_> = args.collect();
    if args == ["--help"] {
        println!("btc_arb_phase1_history --config PATH --input PATH|- --output PATH --summary PATH --from-us I64 --to-us I64");
        return Ok(0);
    }
    let args = options(args.into_iter())?;
    let config_path = &args["--config"];
    let input_path = &args["--input"];
    let output_path = &args["--output"];
    let summary_path = &args["--summary"];
    let from: i64 = args["--from-us"].parse().map_err(|_| (64, "invalid from clock"))?;
    let to: i64 = args["--to-us"].parse().map_err(|_| (64, "invalid to clock"))?;
    if [config_path, output_path, summary_path].iter().any(|path| path.as_str() == "-")
        || output_path == summary_path || output_path == input_path || output_path == config_path
        || summary_path == input_path || summary_path == config_path {
        return Err((64, "invalid path combination"));
    }
    for path in [config_path, input_path, output_path, summary_path] {
        if path != "-" { reject_symlinks(Path::new(path))?; }
    }
    for path in [output_path, summary_path] {
        if Path::new(path).try_exists().map_err(io_failure)? {
            return Err((64, "output already exists"));
        }
    }
    let config_file = File::open(config_path).map_err(io_failure)?;
    let config: Config = serde_json::from_reader(config_file.take(MAX_LINE as u64 + 1))
        .map_err(|_| (64, "invalid config"))?;
    if from != config.window_start_us || to != config.window_end_us {
        return Err((64, "CLI/config window mismatch"));
    }
    let mut state = History::new(config).map_err(|_| (64, "invalid research config"))?;
    let input: Box<dyn BufRead> = if input_path == "-" { Box::new(BufReader::new(io::stdin())) }
        else { Box::new(BufReader::new(File::open(input_path).map_err(io_failure)?)) };
    let mut summary = OpenOptions::new().write(true).create_new(true).open(summary_path).map_err(io_failure)?;
    let mut output = BufWriter::new(OpenOptions::new().write(true).create_new(true)
        .open(output_path).map_err(io_failure)?);
    let mut spots = 0_u64;
    let mut values = 0_u64;
    let mut chain = [0_u8; 32];
    let result = consume(input, &mut state, &mut output, &mut spots, &mut values, &mut chain);
    let code = result.as_ref().err().map_or(0, |failure| failure.0);
    let count = spots + values;
    serde_json::to_writer(&mut summary, &json!({
        "schema_version":"btc-arb-phase1-summary/1", "native_exit":code,
        "spots":spots, "values":values, "complete":result.is_ok(),
        "error":result.as_ref().err().map(|failure| failure.1),
        "accepted_events":count, "last_seq":count.checked_sub(1),
        "input_chain_keccak256":hex::encode(chain), "max_active_rounds":state.max_active_rounds()
    })).map_err(|_| (5, "summary encoding failed"))?;
    summary.flush().map_err(io_failure)?;
    result?;
    Ok(0)
}

fn consume(mut input: Box<dyn BufRead>, state: &mut History, output: &mut impl Write,
    spots: &mut u64, values: &mut u64, chain: &mut [u8; 32]) -> Result<(), Failure> {
    loop {
        // A malicious unterminated line cannot grow memory beyond the wire limit.
        let mut line = Vec::with_capacity(1024);
        let count = (&mut input).take(MAX_LINE as u64 + 1).read_until(b'\n', &mut line).map_err(io_failure)?;
        if count == 0 { break; }
        if count > MAX_LINE || line.last() != Some(&b'\n') || line.contains(&b'\r') {
            return Err((2, "invalid line framing/size"));
        }
        let event: Event = serde_json::from_slice(&line).map_err(|_| (2, "invalid event schema"))?;
        let value = state.apply(event).map_err(|_| (2, "event contract rejected"))?;
        let mut chained = Vec::with_capacity(32 + count);
        chained.extend_from_slice(chain); chained.extend_from_slice(&line);
        *chain = keccak256(&chained).into();
        if let Some(value) = value {
            // accepted means apply succeeded, even if the output subsequently fails.
            *values += 1;
            serde_json::to_writer(&mut *output, &value).map_err(|_| (5, "value encoding failed"))?;
            output.write_all(b"\n").map_err(io_failure)?;
        } else { *spots += 1; }
    }
    output.flush().map_err(io_failure)
}

fn reject_symlinks(path: &Path) -> Result<(), Failure> {
    let mut current = std::path::PathBuf::new();
    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) { return Err((64, "parent traversal forbidden")); }
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Err((64, "symlink forbidden")),
            Ok(_) => {},
            Err(error) if error.kind() == io::ErrorKind::NotFound => {},
            Err(error) => return Err(io_failure(error)),
        }
    }
    Ok(())
}
fn io_failure(_: io::Error) -> Failure { (5, "local I/O failed") }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn args_are_complete_unique_and_closed() {
        let args = ["--config", "c", "--input", "-", "--output", "o", "--summary", "s", "--from-us", "0", "--to-us", "1"];
        assert!(options(args.iter().map(|arg| arg.to_string())).is_ok());
        assert_eq!(options(["--live".into(), "yes".into()].into_iter()).unwrap_err().0, 64);
        assert_eq!(options(["--input".into(), "-".into(), "--input".into(), "-".into()].into_iter()).unwrap_err().0, 64);
        assert_eq!(options(["--input".into()].into_iter()).unwrap_err().0, 64);
    }
    #[test]
    fn framing_is_bounded_and_partial_summary_counts_are_real() {
        let config: Config = serde_json::from_str(r#"{"schema_version":"btc-arb-phase1-research-config/1","symbol":"BTCUSDT","window_start_us":1780000200000000,"window_end_us":1780086600000000,"as_of_us":1794927600000000,"sample_seconds":5,"half_life_secs":300,"min_vol_samples":20,"vol_floor":0.00005,"clock_basis":"event_time_proxy"}"#).unwrap();
        for line in [vec![b'a'; MAX_LINE + 1], b"{}".to_vec(), b"{}\r\n".to_vec(), b"{}\n".to_vec()] {
            let mut state = History::new(config.clone()).unwrap();
            let (mut spots, mut values, mut chain) = (0, 0, [0; 32]);
            let result = consume(Box::new(io::Cursor::new(line)), &mut state,
                &mut Vec::new(), &mut spots, &mut values, &mut chain);
            assert_eq!(result.unwrap_err().0, 2);
            assert_eq!((spots, values, chain), (0, 0, [0; 32]));
        }
    }
}
