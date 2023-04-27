use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use tokio::time::Duration;

use brokers::prelude::*;
use strategy::driver::{StratProviderRef, Strategy, StrategyDriver, StrategyInitContext};
use strategy::event::trades_history;
use strategy::prelude::{GenericDriver, GenericDriverOptions, PortfolioOptions};
use strategy::query::{DataQuery, DataResult, PortfolioSnapshot};
use trading::engine::mock_engine;
use trading::position::Position;
use util::test::test_results_dir;
use util::time::{utc_at_midnight, DateRange};

use crate::draw::{draw_line_plot, StrategyEntryFnRef};
use crate::fs::copy_file;
use crate::log::{write_models, write_trade_events, StrategyLog};
use crate::{input, test_db_with_path};

/// # Panics
///
/// if creating the strat or any loop breaks
#[allow(clippy::too_many_lines)]
pub async fn generic_backtest<'a, S2>(
    test_name: &'a str,
    provider: StratProviderRef,
    draw_entries: &'a [(S2, StrategyEntryFnRef<StrategyLog, S2>)],
    range: DateRange,
    exchanges: &'a [Exchange],
    starting_cash: f64,
    fees_rate: f64,
) -> Vec<Position>
where
    S2: ToString,
{
    util::test::init_test_env();
    //setup_opentelemetry();
    let path = util::test::test_dir();
    let engine = Arc::new(mock_engine(path.path(), exchanges));
    let test_results_dir = test_results_dir(test_name);
    let db = test_db_with_path(path);
    let strat = provider(StrategyInitContext {
        engine: engine.clone(),
        db: db.clone(),
    });
    let generic_options = GenericDriverOptions {
        portfolio: PortfolioOptions {
            fees_rate,
            initial_quote_cash: starting_cash,
        },
        start_trading: None,
        dry_mode: None,
    };
    let mut driver = GenericDriver::try_new(
        <dyn Strategy>::channels(strat.as_ref()),
        db,
        &generic_options,
        strat,
        engine,
        None,
    )
    .unwrap();
    let mut elapsed = 0_u128;

    let mut events: Vec<MarketEventEnvelope> = vec![];
    for c in &driver.channels() {
        events.extend(
            input::load_csv_events(
                utc_at_midnight(range.0),
                utc_at_midnight(range.1),
                vec![c.pair().as_ref()],
                &c.exchange().to_string(),
                c.name(),
            )
            .await,
        );
    }
    let (events, count) = events.into_iter().tee();
    let num_records = count.count();
    let mut event_logs: Vec<StrategyLog> = Vec::new();

    let before_evals = Instant::now();
    for event in events {
        let now = Instant::now();
        let event_time = event.e.time();
        util::time::set_mock_time(event_time);
        driver.on_market_event(&event).await.unwrap();
        let mut tries = 0;
        loop {
            if tries > 5 {
                break;
            }
            if driver
                .ctx()
                .portfolio
                .is_locked(&(event.symbol.xch, event.symbol.value.clone()))
            {
                driver.resolve_orders().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
                tries += 1;
            } else {
                break;
            }
        }
        if let MarketEvent::Orderbook(ob) = &event.e {
            let models = driver
                .query(DataQuery::Models)
                .await
                .map(|dr| match dr {
                    DataResult::Models(v) => v,
                    _ => vec![],
                })
                .unwrap();
            let portfolio = driver.ctx().portfolio;
            let nominal_positions = driver
                .ctx()
                .portfolio
                .open_positions()
                .values()
                .map(|v| ((v.exchange.to_string(), v.symbol.to_string()), v.quantity))
                .collect();
            event_logs.push(StrategyLog::new(
                event_time,
                hashmap! { (event.symbol.xch.to_string(), event.symbol.value.to_string()) => ob.vwap().unwrap() },
                models,
                PortfolioSnapshot {
                    value: portfolio.value(),
                    pnl: portfolio.pnl(),
                    current_return: portfolio.current_return(),
                },
                nominal_positions,
            ));
        }

        elapsed += now.elapsed().as_nanos();
    }
    info!(
        "For {} records, evals took {}ms, each iteration took {} ns on avg",
        num_records,
        before_evals.elapsed().as_millis(),
        elapsed / num_records as u128
    );

    let data = Arc::new(event_logs);

    write_models(&test_results_dir, data.as_ref());
    write_trade_events(
        &test_results_dir,
        &trades_history(&driver.ctx().portfolio.positions_history().unwrap()),
    );

    let out_file = draw_line_plot(test_results_dir.as_str(), data.as_ref(), draw_entries)
        .expect("Should have drawn plots from strategy logs");
    copy_file(&out_file, &format!("{}/plot_latest.html", &test_results_dir));

    let mut positions = driver.ctx().portfolio.positions_history().unwrap();
    positions.sort_by(|p1, p2| p1.meta.close_at.cmp(&p2.meta.close_at));
    //insta::assert_debug_snapshot!(positions.last());
    positions
}
