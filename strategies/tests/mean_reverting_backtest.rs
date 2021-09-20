#[cfg(feature = "backtests")]
#[actix::test]
async fn complete_backtest_backtest() -> Result<()> {
    use chrono::{TimeZone, Utc};

    use backtest::{BacktestConfig, Result};
    use coinnect_rt::exchange::Exchange;
    use strategies::mean_reverting::options::Options;
    use strategies::StrategySettings;
    use util::test::{data_cache_dir, test_results_dir};

    pub fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    static PAIR: &str = "BTC_USDT";

    init();
    let conf = BacktestConfig::builder()
        .db_path(util::test::test_dir().into_path())
        .strat(StrategySettings::MeanReverting(Options {
            pair: PAIR.into(),
            threshold_long: -0.01,
            threshold_short: 0.01,
            threshold_eval_freq: Some(1),
            dynamic_threshold: Some(true),
            threshold_window_size: Some(10000),
            stop_loss: -0.1,
            stop_gain: 0.075,
            initial_cap: 100.0,
            dry_mode: Some(true),
            short_window_size: 100,
            long_window_size: 1000,
            sample_freq: "1min".to_string(),
            exchange: Exchange::Binance,
            order_mode: None,
            execution_instruction: None,
            order_asset_type: None,
        }))
        .fees(0.001)
        .period(::backtest::Period::Interval {
            from: Utc.ymd(2021, 8, 1).naive_utc(),
            to: Some(Utc.ymd(2021, 8, 9).naive_utc()),
        })
        .input_sample_rate("1min".to_string())
        .data_dir(data_cache_dir())
        .use_generic(false)
        .build();
    let bt = ::backtest::Backtest::try_new(&conf)?;
    bt.run().await?;
    let _test_results_dir = test_results_dir(module_path!());

    /*write_ema_values(&test_results_dir, &model_values);
    crate::test_util::log::write_trade_events(&test_results_dir, &trade_events);
    write_thresholds(&test_results_dir, &strategy_logs);

    // Find that latest operations are correct
    let mut positions = strat.get_operations();
    positions.sort_by(|p1, p2| p1.pos.time.cmp(&p2.pos.time));
    let last_position = positions.last();
    assert!(last_position.is_some(), "No position found in operations");
    // Output SVG graphs
    let out_file = draw_line_plot(strategy_logs).expect("Should have drawn plots from strategy logs");
    let copied = std::fs::copy(&out_file, "graphs/mean_reverting_plot_latest.svg");
    assert!(copied.is_ok(), "{}", format!("{:?} : {}", copied, out_file));*/

    /*assert_eq!(Some(7308.47998046875), last_position.map(|p| p.pos.price));
    assert_eq!(Some(90.11699784066761), last_position.map(|p| p.value()));*/
    Ok(())
}
