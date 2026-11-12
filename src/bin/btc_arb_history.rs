//! Explicit local-only history CLI. No application config, network or venue construction.
use polymarket_toolkits::bot::arbitrage::{history::{Event, History}, replay::ReplayConfig};
use serde_json::json;
use std::{collections::HashMap, fs::{File, OpenOptions}, io::{BufRead, BufReader, BufWriter, Write}};

fn main() {
    let code = execute().unwrap_or_else(|(code, message)| { eprintln!("{message}"); code });
    std::process::exit(code);
}
fn execute() -> Result<i32, (i32, String)> {
    let mut args = std::env::args().skip(1);
    let mut options = HashMap::new();
    while let Some(key) = args.next() {
        if key == "--help" { println!("btc_arb_history --config PATH --input PATH|- --output PATH|- --summary PATH --from-ms I64 --to-ms I64"); return Ok(0); }
        if !["--config","--input","--output","--summary","--from-ms","--to-ms"].contains(&key.as_str()) || options.contains_key(&key) {
            return Err((64, "unknown/duplicate argument".into()));
        }
        options.insert(key, args.next().ok_or((64, "missing argument value".into()))?);
    }
    let get = |name: &str| options.get(name).ok_or((64, format!("missing {name}")));
    let from: i64 = get("--from-ms")?.parse().map_err(|_| (64,"invalid from clock".into()))?;
    let to: i64 = get("--to-ms")?.parse().map_err(|_| (64,"invalid to clock".into()))?;
    if from < 0 || from >= to { return Err((64,"invalid window".into())); }
    let config_path = get("--config")?;
    let input_path = get("--input")?;
    let output_path = get("--output")?;
    let summary_path = get("--summary")?;
    if config_path == "-" || summary_path == "-" { return Err((64,"config/summary require paths".into())); }
    let config: ReplayConfig = serde_json::from_reader(File::open(config_path).map_err(io_error)?).map_err(|_| (64,"invalid config".into()))?;
    let mut state = History::new(&config).map_err(|message| (64,message))?;
    let input: Box<dyn BufRead> = if input_path == "-" { Box::new(BufReader::new(std::io::stdin())) }
        else { Box::new(BufReader::new(File::open(input_path).map_err(io_error)?)) };
    let mut summary = OpenOptions::new().write(true).create_new(true).open(summary_path).map_err(io_error)?;
    let output: Box<dyn Write> = if output_path == "-" { Box::new(std::io::stdout()) }
        else { Box::new(OpenOptions::new().write(true).create_new(true).open(output_path).map_err(io_error)?) };
    let mut output = BufWriter::new(output);
    let mut spots = 0_u64;
    let mut values = 0_u64;
    let result = (|| -> Result<(), (i32,String)> {
        for line in input.lines() {
            let line = line.map_err(io_error)?;
            let event: Event = serde_json::from_str(&line).map_err(|error| (2, format!("invalid event: {error}")))?;
            // A candle opened before the exclusive upper bound closes exactly at it.
            if event.at_ms < from || event.at_ms > to { return Err((2,"event outside window".into())); }
            match state.apply(event).map_err(|message| (2,message))? {
                Some(value) => { serde_json::to_writer(&mut output, &value).map_err(|e| (5,e.to_string()))?;
                    output.write_all(b"\n").map_err(io_error)?; values += 1; }
                None => spots += 1,
            }
        }
        output.flush().map_err(io_error)?;
        Ok(())
    })();
    let code = result.as_ref().err().map_or(0, |(code,_)| *code);
    serde_json::to_writer(&mut summary, &json!({"native_exit":code,"spots":spots,"values":values,
        "complete":result.is_ok(),"error":result.as_ref().err().map(|(_,m)|m)})).map_err(|e| (5,e.to_string()))?;
    summary.flush().map_err(io_error)?;
    result?;
    Ok(0)
}
fn io_error(error: std::io::Error) -> (i32,String) { (5,error.to_string()) }
