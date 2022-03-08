use backtest::report::{draw_lines, BacktestReport, TimeWrap};
use chrono::Duration;
use plotly::common::{Side, Title};
use plotly::layout::ShapeLine;
use plotly::Bar;
use serde_json::Value;
use stats::kline::Resolution;
use strategy::types::{PositionSummary, StratEvent};
use trading::position::{OperationKind, PositionKind};

pub fn edit_report(report: &mut BacktestReport, resolution: Resolution) {
    use plotly::common::{Marker, Mode, Position};
    use plotly::layout::{Axis, LayoutGrid, RangeSlider, Shape, ShapeType};
    use plotly::{Layout, NamedColor, Scatter};

    let mut plot = report.tradeview_plot();
    let mut layout = Layout::new()
        .grid(
            LayoutGrid::new()
                .columns(1)
                .rows(6)
                .sub_plots(vec!["volume".to_string()]),
        )
        .x_axis(Axis::new().range_slider(RangeSlider::new().visible(false)))
        .y_axis2(
            Axis::new()
                .title(Title::new("volume"))
                .overlaying("y")
                .side(Side::Right),
        );
    // VOLUME SUBPLOT
    let skipped_data = report.candles().unwrap();
    let time: Vec<TimeWrap> = skipped_data.iter().map(|x| TimeWrap(x.ts)).collect();
    let volume: Vec<f64> = skipped_data.iter().map(|td| td.value.quote_volume).collect();
    let trace = Bar::new(time, volume).name("volume").y_axis("y2").x_axis("x");
    plot.add_trace(trace);

    // PLOT TRADES
    let mut long_entries_time = vec![];
    let mut long_entries_price = vec![];
    let mut short_entries_time = vec![];
    let mut short_entries_price = vec![];
    let mut long_exits_time = vec![];
    let mut long_exits_price = vec![];
    let mut short_exits_time = vec![];
    let mut short_exits_price = vec![];
    for strat_event in report.strat_events().unwrap() {
        match strat_event.value {
            StratEvent::PositionSummary(PositionSummary { op, trade }) => match (op.op, op.pos) {
                (OperationKind::Open, PositionKind::Long) => {
                    long_entries_price.push(trade.price);
                    long_entries_time.push(op.at);
                }
                (OperationKind::Close, PositionKind::Long) => {
                    long_exits_price.push(trade.price);
                    long_exits_time.push(op.at);
                }
                (OperationKind::Open, PositionKind::Short) => {
                    short_entries_price.push(trade.price);
                    short_entries_time.push(op.at);
                }
                (OperationKind::Close, PositionKind::Short) => {
                    short_exits_price.push(trade.price);
                    short_exits_time.push(op.at);
                }
            },
            StratEvent::OpenPosition(p) => {
                let order = p.open_order.unwrap();
                match p.kind {
                    PositionKind::Short => {
                        short_entries_price.push(order.price.unwrap());
                        short_entries_time.push(order.open_at.unwrap());
                    }
                    PositionKind::Long => {
                        long_entries_price.push(order.price.unwrap());
                        long_entries_time.push(order.open_at.unwrap());
                    }
                }
            }
            StratEvent::ClosePosition(p) => {
                let order = p.close_order.unwrap();
                match p.kind {
                    PositionKind::Short => {
                        short_exits_price.push(order.price.unwrap());
                        short_exits_time.push(order.open_at.unwrap());
                    }
                    PositionKind::Long => {
                        long_exits_price.push(order.price.unwrap());
                        long_exits_time.push(order.open_at.unwrap());
                    }
                }
            }
            _ => {}
        }
    }
    let long_entries_text = long_entries_price.iter().map(|p| format!("Len {}", p)).collect();
    let long_entries_trace = Scatter::new(long_entries_time, long_entries_price)
        .name("long_exits")
        .marker(Marker::new().color(NamedColor::LightSkyBlue).size(10))
        .mode(Mode::MarkersText)
        .text_position(Position::BottomCenter)
        .text_array(long_entries_text);
    plot.add_trace(long_entries_trace);
    let short_entries_text = short_entries_price.iter().map(|p| format!("Sen {}", p)).collect();
    let short_entries_trace = Scatter::new(short_entries_time, short_entries_price)
        .name("short_entries")
        .marker(Marker::new().color(NamedColor::LightCoral).size(10))
        .mode(Mode::MarkersText)
        .text_position(Position::BottomCenter)
        .text_array(short_entries_text);
    plot.add_trace(short_entries_trace);
    let long_exit_text = long_exits_price.iter().map(|p| format!("Lex {}", p)).collect();
    let long_exits_trace = Scatter::new(long_exits_time, long_exits_price)
        .name("long_exits")
        .marker(Marker::new().color(NamedColor::DeepSkyBlue).size(10))
        .mode(Mode::MarkersText)
        .text_position(Position::TopCenter)
        .text_array(long_exit_text);
    plot.add_trace(long_exits_trace);
    let short_exit_text = short_exits_price.iter().map(|p| format!("Sex {}", p)).collect();
    let short_exits_trace = Scatter::new(short_exits_time, short_exits_price)
        .name("short_exits")
        .marker(Marker::new().color(NamedColor::Coral).size(10))
        .mode(Mode::MarkersText)
        .text_position(Position::TopCenter)
        .text_array(short_exit_text);
    plot.add_trace(short_exits_trace);

    // PLOT MODELS
    let signal_plot_offset = 3;
    let models = report.models().unwrap();
    for (model_key, offset) in &[("rsi", signal_plot_offset), ("stoch", signal_plot_offset), ("macd", 4)] {
        let mut models_time = vec![];
        let mut models_values = vec![];
        for timed_value in models.iter() {
            if let Some(Some(v)) = timed_value.value.get(*model_key) {
                models_time.push(timed_value.ts);
                models_values.push(Value::as_f64(v).unwrap());
            }
        }
        let trace = Scatter::new(models_time, models_values)
            .name(model_key)
            .x_axis("x")
            .y_axis(&format!("y{}", offset));
        plot.add_trace(trace);
    }

    {
        let rect_draw_offset = resolution.as_millis();
        let x_id = format!("x{}", signal_plot_offset);
        let y_id = format!("y{}", signal_plot_offset);
        let mut sell_signal_time = vec![];
        let mut sell_signal_values = vec![];
        let mut buy_signal_values = vec![];
        let mut buy_signal_time = vec![];
        for timed_value in models.into_iter() {
            let value = timed_value.value;
            let rect_time_base = timed_value.ts.timestamp_millis() - Duration::hours(1).num_milliseconds();
            if let Some(Some(main_signal)) = value.get("main_signal") {
                if main_signal.as_i64().unwrap() == 1 {
                    layout.add_shape(
                        Shape::new()
                            .shape_type(ShapeType::Rect)
                            .x0(rect_time_base)
                            .x1(rect_time_base + rect_draw_offset)
                            .y0(0_i32)
                            .y1(1_i32)
                            .fill_color(NamedColor::Lime)
                            .x_ref("x")
                            .y_ref(&y_id)
                            .opacity(0.6)
                            .line(ShapeLine::new().width(1.0)),
                    );
                    buy_signal_values.push(1);
                    buy_signal_time.push(timed_value.ts);
                } else if main_signal.as_i64().unwrap() == -1 {
                    layout.add_shape(
                        Shape::new()
                            .shape_type(ShapeType::Rect)
                            .x0(rect_time_base)
                            .x1(rect_time_base + rect_draw_offset)
                            .y0(0_i32)
                            .y1(1_i32)
                            .fill_color(NamedColor::Red)
                            .x_ref("x")
                            .y_ref(&y_id)
                            .opacity(0.6)
                            .line(ShapeLine::new().width(1.0)),
                    );
                    sell_signal_values.push(1);
                    sell_signal_time.push(timed_value.ts);
                }
            }
        }
    }

    if let Ok(snapshots) = report.snapshots() {
        draw_lines(&mut plot, 5, snapshots.as_slice(), vec![
            ("pnl", vec![|i| i.pnl]),
            ("return", vec![|i| i.current_return]),
        ]);
    }

    plot.set_layout(layout);
    report.write_plot(plot, "tradeview_alt.html");
}
