use std::sync::Arc;
use std::time::Instant;

use chrono::{Date, Utc};
use itertools::Itertools;
use tokio::time::Duration;

use coinnect_rt::prelude::*;
use db::Storage;
use strategy::driver::{Strategy, StrategyDriver};
use strategy::event::trades_history;
use strategy::prelude::{GenericDriver, GenericDriverOptions, PortfolioOptions};
use strategy::query::{DataQuery, DataResult, PortfolioSnapshot};
use strategy::trading::engine::{mock_engine, TradingEngine};
use strategy::trading::position::Position;
use util::test::test_results_dir;

use crate::draw::{draw_line_plot, StrategyEntry};
use crate::fs::copy_file;
use crate::init;
use crate::log::{write_models, write_trade_events, StrategyLog};
use crate::{input, test_db_with_path};

pub struct GenericTestContext {
    pub engine: Arc<TradingEngine>,
    pub db: Arc<dyn Storage>,
}

pub struct BacktestRange {
    from: Date<Utc>,
    to: Date<Utc>,
}

impl BacktestRange {
    pub fn new(from: Date<Utc>, to: Date<Utc>) -> Self { Self { from, to } }
}

pub type BacktestStratProvider = fn(GenericTestContext) -> Box<dyn Strategy>;

pub async fn generic_backtest(
    test_name: &str,
    provider: BacktestStratProvider,
    draw_entries: &[StrategyEntry<'_, StrategyLog>],
    range: &BacktestRange,
    exchanges: &[Exchange],
    starting_cash: f64,
    fees_rate: f64,
) -> Vec<Position> {
    init();
    //setup_opentelemetry();
    let path = util::test::test_dir();
    let engine = Arc::new(mock_engine(path.path(), exchanges));
    let test_results_dir = test_results_dir(test_name);
    let db = test_db_with_path(path);
    let embedded = provider(GenericTestContext {
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
    let mut strat = GenericDriver::try_new(
        <dyn Strategy>::channels(embedded.as_ref()),
        db,
        &generic_options,
        embedded,
        engine,
        None,
    )
    .unwrap();
    let mut elapsed = 0_u128;

    let mut events: Vec<MarketEventEnvelope> = vec![];
    for c in &strat.channels() {
        events.extend(
            input::load_csv_events(
                range.from,
                range.to,
                vec![c.pair().as_ref()],
                &c.exchange().capitalized(),
                c.name(),
            )
            .await,
        );
    }
    let (events, count) = events.iter().tee();
    let num_records = count.count();
    // align data
    let mut strategy_logs: Vec<StrategyLog> = Vec::new();

    let before_evals = Instant::now();
    for event in events {
        let now = Instant::now();
        let event_time = event.e.time();
        util::time::set_current_time(event_time);
        strat.add_event(event).await.unwrap();
        let mut tries = 0;
        loop {
            if tries > 5 {
                break;
            }
            if strat.ctx().portfolio.is_locked(&(event.xch, event.pair.clone())) {
                strat.resolve_orders().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
                tries += 1;
            } else {
                break;
            }
        }
        if let Ok(DataResult::Models(models)) = strat.data(DataQuery::Models).await {
            let portfolio = strat.ctx().portfolio;
            if let MarketEvent::Orderbook(ob) = &event.e {
                let nominal_positions = strat
                    .ctx()
                    .portfolio
                    .open_positions()
                    .values()
                    .map(|v| ((v.exchange.to_string(), v.symbol.to_string()), v.quantity))
                    .collect();
                strategy_logs.push(StrategyLog::new(
                    event_time,
                    hashmap! { (event.xch.to_string(), event.pair.to_string()) => ob.avg_price().unwrap() },
                    models,
                    PortfolioSnapshot {
                        value: portfolio.value(),
                        pnl: portfolio.pnl(),
                        current_return: portfolio.current_return(),
                    },
                    nominal_positions,
                ));
            }
        }

        elapsed += now.elapsed().as_nanos();
    }
    info!(
        "For {} records, evals took {}ms, each iteration took {} ns on avg",
        num_records,
        before_evals.elapsed().as_millis(),
        elapsed / num_records as u128
    );

    write_models(&test_results_dir, &strategy_logs);
    write_trade_events(
        &test_results_dir,
        &trades_history(&strat.ctx().portfolio.positions_history().unwrap()),
    );

    let out_file = draw_line_plot(test_results_dir.as_str(), strategy_logs, draw_entries)
        .expect("Should have drawn plots from strategy logs");
    copy_file(&out_file, &format!("{}/plot_latest.html", &test_results_dir));

    let mut positions = strat.ctx().portfolio.positions_history().unwrap();
    positions.sort_by(|p1, p2| p1.meta.close_at.cmp(&p2.meta.close_at));
    //insta::assert_debug_snapshot!(positions.last());
    positions
}
