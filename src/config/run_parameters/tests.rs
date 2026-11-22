use super::*;

#[test]
fn original_tokens_are_exact_and_bad_precision_is_refused() {
    assert_eq!(money_atoms("97.422").unwrap(), U256::from(97_422_000_000_000_000_000u128));
    assert_eq!(money_atoms("1e-6").unwrap(), U256::from(1_000_000_000_000u64));
    for token in ["true", "0", "-1", "NaN", "1e99", "1e-7", "1.0000001", "1000000001"] {
        assert!(money_atoms(token).is_err(), "{token}");
    }
}

#[test]
fn a_budget_string_is_exact_to_the_atom() {
    // The claimed-payout balance the operator gave: eighteen decimals, exact.
    assert_eq!(
        budget_atoms_text("98.148096491228070176").unwrap(),
        U256::from(98_148_096_491_228_070_176u128)
    );
    // The observed collateral of run-013: four decimals, exact.
    assert_eq!(
        budget_atoms_text("96.4245").unwrap(),
        U256::from(96_424_500_000_000_000_000u128)
    );
    // An integer budget is one whole asset.
    assert_eq!(budget_atoms_text("97").unwrap(), U256::from(97u64) * U256::from(1_000_000_000_000_000_000u128));
    // Sub-atom precision, a sign, an exponent or grouping is refused, never
    // truncated or rounded.
    for text in ["98.1480964912280701761", "-1", "1e3", "9,8", "0", "1.", ""] {
        assert!(budget_atoms_text(text).is_err(), "{text}");
    }
}

#[test]
fn a_string_budget_reaches_from_raw_unchanged() {
    let raw = r#"{
        "controlled_run": {"budget_usdt": "98.148096491228070176",
            "duration_seconds": 86400, "operations_mode": "unattended"},
        "strategies": {"btc_arb": {"order_usd": 1},
            "limits": {"max_order_usd": 1, "max_market_usd": 1,
                "max_total_usd": 10, "max_loss_usd": 20}}
    }"#;
    let params = RunParameters::from_raw(raw).unwrap();
    assert_eq!(params.budget, U256::from(98_148_096_491_228_070_176u128));
    assert!(params.ordered());
}
