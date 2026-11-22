//! Frozen BTC5m public money and duration. No credentials, IO, or float-to-atoms conversion.
use alloy_primitives::U256;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Opt-in public run settings; legacy modes have none.
///
/// `budget_usdt` may be a JSON number (up to six decimals) or a decimal string
/// with up to eighteen decimals. A string is the only way to state a budget at
/// atom precision (for example a post-claim balance of
/// `98.148096491228070176`), so the declared C0 can equal the observed
/// collateral atom-for-atom.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunSettings {
    pub budget_usdt: serde_json::Value,
    pub duration_seconds: u64,
    pub operations_mode: String,
}

impl RunSettings {
    /// The budget's exact atoms: a decimal string to eighteen places, or a
    /// number to the historical six.
    pub fn budget_atoms(&self) -> Result<U256> {
        match &self.budget_usdt {
            serde_json::Value::String(text) => budget_atoms_text(text),
            serde_json::Value::Number(_) => atoms_of_token(&self.budget_usdt.to_string()),
            _ => bail!("PUBLIC_CONFIG: MONEY"),
        }
    }
}

/// Exact original-token money, frozen for the lifetime of one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunParameters {
    pub budget: U256,
    pub target: U256,
    pub order: U256,
    pub market: U256,
    pub total: U256,
    pub loss: U256,
}

impl RunParameters {
    /// Historical fixture defaults only; new live entry points require public settings.
    pub fn legacy() -> Self {
        let unit = U256::from(1_000_000_000_000_000_000u128);
        Self { budget: unit * U256::from(98), target: unit, order: unit, market: unit,
            total: unit * U256::from(10), loss: unit * U256::from(20) }
    }

    /// Whether the public money relation holds: target≤order≤market≤total≤budget.
    pub fn ordered(&self) -> bool {
        self.target <= self.order && self.order <= self.market
            && self.market <= self.total && self.total <= self.budget
    }

    /// The public settings a file declares, when it is the parameterized entry.
    pub fn declared_settings(raw: &str) -> Result<Option<RunSettings>> {
        let value: serde_json::Value = serde_json::from_str(raw)?;
        match value.get("controlled_run") {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(_) => Ok(Some(serde_json::from_value(value["controlled_run"].clone())?)),
        }
    }

    /// Read the six money tokens at their exact JSON paths after strict JSON validation.
    pub fn from_raw(raw: &str) -> Result<Self> {
        let value: serde_json::Value = serde_json::from_str(raw)?;
        let settings: RunSettings = serde_json::from_value(value["controlled_run"].clone())?;
        let mode = match settings.operations_mode.as_str() {
            "supervised" => super::controlled_live::OperationsMode::Supervised,
            "unattended" => super::controlled_live::OperationsMode::Unattended,
            _ => bail!("PUBLIC_CONFIG: CONTROLLED_RUN"),
        };
        if settings.duration_seconds > 86_400 || !mode.allows_duration(
            chrono::Duration::seconds(settings.duration_seconds as i64)) {
            bail!("PUBLIC_CONFIG: DURATION");
        }
        let mut tokens = HashMap::new();
        collect_tokens(raw.as_bytes(), &mut 0, "", &mut tokens)?;
        let amount = |path: &str| -> Result<U256> {
            money_atoms(tokens.get(path).ok_or_else(|| anyhow::anyhow!("PUBLIC_CONFIG: MONEY"))?)
        };
        // A string budget is read exactly at up to eighteen decimals; a numeric
        // budget keeps the original token and the historical six-decimal cap.
        let budget = match &settings.budget_usdt {
            serde_json::Value::String(text) => budget_atoms_text(text)?,
            _ => amount("/controlled_run/budget_usdt")?,
        };
        let params = Self {
            budget,
            target: amount("/strategies/btc_arb/order_usd")?,
            order: amount("/strategies/limits/max_order_usd")?,
            market: amount("/strategies/limits/max_market_usd")?,
            total: amount("/strategies/limits/max_total_usd")?,
            loss: amount("/strategies/limits/max_loss_usd")?,
        };
        if !params.ordered() {
            bail!("PUBLIC_CONFIG: MONEY_RELATION");
        }
        Ok(params)
    }
}

/// The atoms of a configuration float, only when its shortest round-trip
/// decimal text is itself an exact public token. This compares a parsed value
/// against a frozen amount; it never creates a spendable one.
pub fn atoms_of_float(value: f64) -> Result<U256> {
    if !value.is_finite() || value <= 0.0 { bail!("PUBLIC_CONFIG: MONEY"); }
    money_atoms(&format!("{value}"))
}

/// The atoms of one public JSON number token, read exactly.
pub fn atoms_of_token(token: &str) -> Result<U256> {
    money_atoms(token)
}

/// The atoms of a budget given as a plain decimal USDT string.
///
/// A budget may be stated to eighteen decimal places so it can equal the
/// observed collateral atom-for-atom (for example a claimed payout). Only a
/// plain `digits[.digits]` form is accepted: no sign, no exponent, no grouping.
/// More than eighteen fractional digits is a refusal, never a truncation.
pub fn budget_atoms_text(text: &str) -> Result<U256> {
    let invalid = || anyhow::anyhow!("PUBLIC_CONFIG: MONEY_PRECISION");
    if text.is_empty() {
        return Err(invalid());
    }
    let mut digits = String::with_capacity(text.len());
    let mut fraction = 0u32;
    let mut seen_dot = false;
    for ch in text.chars() {
        match ch {
            '0'..='9' => {
                digits.push(ch);
                if seen_dot {
                    fraction += 1;
                }
            }
            '.' if !seen_dot => seen_dot = true,
            _ => return Err(invalid()),
        }
    }
    if digits.is_empty() || fraction > 18 || (seen_dot && fraction == 0) {
        return Err(invalid());
    }
    let value: U256 = digits.parse().map_err(|_| invalid())?;
    let factor = U256::from(10u128.pow(18 - fraction));
    let atoms = value.checked_mul(factor).ok_or_else(invalid)?;
    // At most 1e9 USDT, the same public ceiling the numeric path enforces.
    if atoms.is_zero() || atoms > U256::from(1_000_000_000u128) * U256::from(1_000_000_000_000_000_000u128) {
        return Err(invalid());
    }
    Ok(atoms)
}

/// At most six decimal places and 1e9 USDT: micro-USDT stays distinguishable by
/// the existing float strategy inputs. Cash/risk atoms are derived only here.
fn money_atoms(token: &str) -> Result<U256> {
    let invalid = || anyhow::anyhow!("PUBLIC_CONFIG: MONEY_PRECISION");
    let (mantissa, exponent) = match token.find(['e', 'E']) {
        Some(index) => (&token[..index], token[index + 1..].parse::<i32>().map_err(|_| invalid())?),
        None => (token, 0),
    };
    if mantissa.starts_with('-') || exponent.abs() > 100 { return Err(invalid()); }
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    let mut micros = digits.parse::<u128>().map_err(|_| invalid())?;
    let power = 6i32.checked_add(exponent).and_then(|n| n.checked_sub(fraction.len() as i32))
        .ok_or_else(invalid)?;
    if power >= 0 {
        micros = micros.checked_mul(10u128.checked_pow(power as u32).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
    } else {
        let divisor = 10u128.checked_pow(power.unsigned_abs()).ok_or_else(invalid)?;
        if micros % divisor != 0 { return Err(invalid()); }
        micros /= divisor;
    }
    if micros == 0 || micros > 1_000_000_000_000_000 { return Err(invalid()); }
    Ok(U256::from(micros) * U256::from(1_000_000_000_000u64))
}

// This small lexical walk only locates original numeric tokens. serde/StrictJson
// owns grammar, duplicates and types; keys are decoded by serde (including escapes).
fn collect_tokens(raw: &[u8], cursor: &mut usize, path: &str,
    tokens: &mut HashMap<String, String>) -> Result<()> {
    fn whitespace(raw: &[u8], cursor: &mut usize) {
        while raw.get(*cursor).is_some_and(u8::is_ascii_whitespace) { *cursor += 1; }
    }
    fn string(raw: &[u8], cursor: &mut usize) -> Result<String> {
        let start = *cursor;
        *cursor += 1;
        while *cursor < raw.len() {
            match raw[*cursor] {
                b'\\' => *cursor += 2,
                b'"' => { *cursor += 1; return Ok(serde_json::from_slice(&raw[start..*cursor])?); }
                _ => *cursor += 1,
            }
        }
        bail!("PUBLIC_CONFIG: JSON")
    }
    whitespace(raw, cursor);
    match raw.get(*cursor) {
        Some(b'{') => {
            *cursor += 1;
            loop {
                whitespace(raw, cursor);
                if raw.get(*cursor) == Some(&b'}') { *cursor += 1; break; }
                let key = string(raw, cursor)?;
                whitespace(raw, cursor);
                *cursor += 1; // colon, already checked by serde
                collect_tokens(raw, cursor, &format!("{path}/{key}"), tokens)?;
                whitespace(raw, cursor);
                if raw.get(*cursor) == Some(&b',') { *cursor += 1; }
            }
        }
        Some(b'[') => {
            *cursor += 1;
            loop {
                whitespace(raw, cursor);
                if raw.get(*cursor) == Some(&b']') { *cursor += 1; break; }
                collect_tokens(raw, cursor, &format!("{path}/[]"), tokens)?;
                whitespace(raw, cursor);
                if raw.get(*cursor) == Some(&b',') { *cursor += 1; }
            }
        }
        Some(b'"') => { string(raw, cursor)?; }
        Some(_) => {
            let start = *cursor;
            while raw.get(*cursor).is_some_and(|c| !c.is_ascii_whitespace() && !b",]}".contains(c)) {
                *cursor += 1;
            }
            let token = std::str::from_utf8(&raw[start..*cursor])?;
            if token.starts_with(|c: char| c.is_ascii_digit() || c == '-') {
                tokens.insert(path.to_owned(), token.to_owned());
            }
        }
        None => bail!("PUBLIC_CONFIG: JSON"),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
