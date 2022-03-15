#[cfg(test)]
mod test {
    use crate::rsistoch_strategy::{Options, StochRsiStrategy};
    use backtest::DatasetCatalog;
    use brokers::exchange::Exchange;
    use brokers::types::SecurityType;
    use chrono::{DateTime, NaiveDate, Utc};
    use stats::kline::{Resolution, TimeUnit};
    use std::sync::Arc;
    use strategy::driver::StratProviderRef;
    use util::time::DateRange;

    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    #[test]
    fn backtest() {
        init();
        actix::System::with_tokio_rt(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Default Tokio runtime could not be created.")
        })
        .block_on(async {
            let resolution = Resolution::new(TimeUnit::Minute, 1);
            let provider: StratProviderRef = Arc::new(move |_ctx| {
                Box::new(
                    StochRsiStrategy::try_new(
                        &Options {
                            exchange: Exchange::Binance,
                            pair: "BTC_USDT".into(),
                            resolution,
                            stop_loss: Some(-0.01),
                            trailing_stop_start: Some(0.01),
                            trailing_stop_loss: Some(0.002),
                            security_type: SecurityType::Future,
                            ..Options::default()
                        },
                        None,
                    )
                    .unwrap(),
                )
            });
            let mut report = backtest::backtest_with_range(
                "rsistoch_btc",
                provider,
                DateRange::by_day(
                    DateTime::from_utc(NaiveDate::from_ymd(2022, 2, 16).and_hms(0, 0, 0), Utc),
                    DateTime::from_utc(NaiveDate::from_ymd(2022, 2, 16).and_hms(3, 0, 0), Utc),
                ),
                Some(10000.0),
                Some(0.0004),
                Some(DatasetCatalog::default_prod()),
            )
            .await
            .unwrap();
            report.write_html();
            for table in &["snapshots", "models", "candles", "events"] {
                report.events_df(table).unwrap();
            }
        });
    }
}
