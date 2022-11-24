use binance::account::OrderRequest;
use binance::bool_to_string;
use binance::errors::Error as BinanceError;
use binance::rest_model::{Balance as BinanceBalance, Fill, IsolatedMarginAccountAsset, IsolatedMarginAccountDetails,
                          MarginAccountDetails as BinanceMarginAccountDetails, MarginOrder, MarginOrderResult,
                          MarginOrderState, Order as BinanceOrder, OrderResponse, OrderSide,
                          OrderStatus as BinanceOrderStatus, OrderType as BinanceOrderType,
                          SideEffectType as BinanceSideEffectType, TimeInForce, Transaction as BinanceTransaction,
                          UserAsset};
use binance::ws_model::{OrderUpdate as BinanceOrderUpdate, WebsocketEvent};
use broker_core::error::Error;
use chrono::{TimeZone, Utc};

use broker_core::pair::{symbol_to_pair, PairConf};
use broker_core::prelude::*;
use broker_core::types::*;

#[derive(Serialize, Deserialize, Debug, Message)]
#[rtype(result = "()")]
pub struct Subscription {
    method: String,
    pub params: Vec<String>,
    id: i32,
}

pub fn subscription(c: &MarketChannel, currency_pairs: &[String], id: i32, depth: Option<u16>) -> Subscription {
    let channel_str = match c.r#type {
        MarketChannelType::Trades => "trade".to_string(),
        MarketChannelType::QuotesCandles | MarketChannelType::Quotes => {
            format!("depth{}@100ms", depth.unwrap_or(10))
        }
        MarketChannelType::Orderbooks => "depth@100ms".to_string(),
        MarketChannelType::Candles => {
            // TODO : user proper binance channel
            "ticks"
        }
        _ => unimplemented!("oi does not exist on binance"),
    };
    Subscription {
        method: String::from("SUBSCRIBE"),
        params: currency_pairs
            .iter()
            .map(|cp| format!("{}@{}", cp.to_lowercase(), channel_str))
            .collect(),
        id,
    }
}

pub fn from_binance_order_update(e: BinanceOrderUpdate) -> OrderUpdate {
    OrderUpdate {
        enforcement: from_binance_time_in_force(e.time_in_force),
        side: from_binance_order_side(e.side),
        orig_order_id: e.client_order_id,
        order_id: e.order_id,
        symbol: e.symbol,
        timestamp: e.event_time,
        new_status: from_binance_order_status(e.current_order_status),
        orig_status: from_binance_order_status(e.execution_type),
        is_on_the_book: e.is_order_on_the_book,
        // Order base
        qty: e.qty,
        quote_qty: e.quote_order_qty,
        price: e.price,
        stop_price: e.stop_price,
        iceberg_qty: e.iceberg_qty,
        // Commission
        commission: e.commission,
        commission_asset: e.commission_asset,
        // Last executed
        last_executed_qty: e.qty_last_executed,
        cummulative_filled_qty: e.cumulative_filled_qty,
        last_executed_price: e.last_executed_price,
        // Quote quantities
        cummulative_quote_asset_transacted_qty: e.cumulative_quote_asset_transacted_qty,
        last_quote_asset_transacted_qty: e.last_quote_asset_transacted_qty,
        quote_order_qty: e.quote_order_qty,
        rejection_reason: Some(e.order_reject_reason),
    }
}

pub fn from_binance_order(o: BinanceOrder) -> Order {
    let symbol = o.symbol.into();
    Order {
        xch: Exchange::Binance,
        symbol: symbol_to_pair(&Exchange::Binance, &symbol).unwrap_or(symbol),
        order_id: o.order_id.to_string(),
        orig_order_id: o.client_order_id,
        price: o.price,
        orig_qty: o.orig_qty,
        executed_qty: o.executed_qty,
        cumulative_quote_qty: o.cummulative_quote_qty,
        status: from_binance_order_status(o.status),
        enforcement: from_binance_time_in_force(o.time_in_force),
        order_type: from_binance_order_type(o.order_type),
        side: from_binance_order_side(o.side),
        stop_price: o.stop_price,
        iceberg_qty: o.iceberg_qty,
        orig_time: o.time,
        last_event_time: o.update_time,
        is_in_transaction: o.is_working,
        orig_quote_order_qty: o.orig_quote_order_qty,
        asset_type: AssetType::Spot,
    }
}

pub fn from_binance_margin_order_state(o: MarginOrderState) -> Order {
    let symbol = o.symbol.into();
    Order {
        xch: Exchange::Binance,
        symbol: symbol_to_pair(&Exchange::Binance, &symbol).unwrap_or(symbol),
        order_id: o.order_id.to_string(),
        orig_order_id: o.client_order_id,
        price: o.price,
        orig_qty: o.orig_qty,
        executed_qty: o.executed_qty,
        cumulative_quote_qty: o.cummulative_quote_qty,
        status: from_binance_order_status(o.status),
        enforcement: from_binance_time_in_force(o.time_in_force),
        order_type: from_binance_order_type(o.order_type),
        side: from_binance_order_side(o.side),
        stop_price: o.stop_price,
        iceberg_qty: o.iceberg_qty,
        orig_time: o.time as u64,
        last_event_time: o.update_time as u64,
        is_in_transaction: o.is_working,
        orig_quote_order_qty: o.orig_qty * o.price,
        asset_type: margin_asset_type(o.is_isolated),
    }
}

#[allow(clippy::cast_possible_wrap)]
pub fn from_binance_account_event(we: WebsocketEvent) -> AccountEvent {
    match we {
        WebsocketEvent::OrderUpdate(e) => AccountEvent::OrderUpdate(from_binance_order_update(*e)),
        WebsocketEvent::BalanceUpdate(e) => AccountEvent::BalanceUpdate(BalanceUpdate {
            symbol: e.asset,
            delta: e.delta,
            server_time: Utc::now(),
            event_time: Utc.timestamp_millis_opt(e.event_time as i64).unwrap(),
            clear_time: Utc.timestamp_millis_opt(e.clear_time as i64).unwrap(),
        }),
        WebsocketEvent::AccountPositionUpdate(e) => AccountEvent::AccountPositionUpdate(AccountPosition {
            balances: e
                .balances
                .iter()
                .map(|b| {
                    (b.asset.clone().into(), Balance {
                        free: b.free,
                        locked: b.locked,
                    })
                })
                .collect(),
            update_time: Utc.timestamp_millis_opt(e.last_update_time as i64).unwrap(),
        }),
        _ => AccountEvent::Noop,
    }
}

pub fn from_binance_order_side(o: OrderSide) -> TradeType {
    match o {
        OrderSide::Buy => TradeType::Buy,
        OrderSide::Sell => TradeType::Sell,
    }
}

pub fn from_binance_order_status(o: BinanceOrderStatus) -> OrderStatus {
    match o {
        BinanceOrderStatus::New => OrderStatus::New,
        BinanceOrderStatus::Rejected => OrderStatus::Rejected,
        BinanceOrderStatus::Canceled => OrderStatus::Canceled,
        BinanceOrderStatus::Expired => OrderStatus::Expired,
        BinanceOrderStatus::Filled => OrderStatus::Filled,
        BinanceOrderStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        BinanceOrderStatus::PendingCancel => OrderStatus::PendingCancel,
        BinanceOrderStatus::Trade => OrderStatus::Traded,
    }
}

pub fn from_binance_time_in_force(t: TimeInForce) -> OrderEnforcement {
    match t {
        TimeInForce::GTC => OrderEnforcement::GTC,
        TimeInForce::FOK => OrderEnforcement::FOK,
        TimeInForce::IOC => OrderEnforcement::IOC,
        TimeInForce::GTX => OrderEnforcement::GTX,
        TimeInForce::Other => panic!("unknown time in force {:?}", t),
    }
}

pub fn to_binance_time_in_force(t: OrderEnforcement) -> TimeInForce {
    match t {
        OrderEnforcement::GTC => TimeInForce::GTC,
        OrderEnforcement::FOK => TimeInForce::FOK,
        OrderEnforcement::IOC => TimeInForce::IOC,
        OrderEnforcement::GTX => TimeInForce::GTX,
    }
}

pub fn to_binance_order_side(tt: TradeType) -> OrderSide {
    match tt {
        TradeType::Buy => OrderSide::Buy,
        TradeType::Sell => OrderSide::Sell,
    }
}

pub fn to_binance_order_type(ot: OrderType) -> BinanceOrderType {
    match ot {
        OrderType::Limit => BinanceOrderType::Limit,
        OrderType::Market => BinanceOrderType::Market,
        OrderType::LimitMaker => BinanceOrderType::LimitMaker,
        OrderType::StopLoss => BinanceOrderType::StopLoss,
        OrderType::StopLossLimit => BinanceOrderType::StopLossLimit,
        OrderType::TakeProfit => BinanceOrderType::TakeProfit,
        OrderType::TakeProfitLimit => BinanceOrderType::TakeProfitLimit,
    }
}

pub fn from_binance_order_type(ot: BinanceOrderType) -> OrderType {
    match ot {
        BinanceOrderType::Limit => OrderType::Limit,
        BinanceOrderType::Market => OrderType::Market,
        BinanceOrderType::LimitMaker => OrderType::LimitMaker,
        BinanceOrderType::StopLoss => OrderType::StopLoss,
        BinanceOrderType::StopLossLimit => OrderType::StopLossLimit,
        BinanceOrderType::TakeProfit => OrderType::TakeProfit,
        BinanceOrderType::TakeProfitLimit => OrderType::TakeProfitLimit,
        BinanceOrderType::Other => panic!("unknown order type {:?}", ot),
    }
}

#[allow(dead_code)]
pub fn from_binance_side_effect(ot: BinanceSideEffectType) -> MarginSideEffect {
    match ot {
        BinanceSideEffectType::Other | BinanceSideEffectType::NoSideEffect => MarginSideEffect::NoSideEffect,
        BinanceSideEffectType::MarginBuy => MarginSideEffect::MarginBuy,
        BinanceSideEffectType::AutoRepay => MarginSideEffect::AutoRepay,
    }
}

pub fn to_binance_margin_side_effect(ot: MarginSideEffect) -> BinanceSideEffectType {
    match ot {
        MarginSideEffect::NoSideEffect => BinanceSideEffectType::NoSideEffect,
        MarginSideEffect::MarginBuy => BinanceSideEffectType::MarginBuy,
        MarginSideEffect::AutoRepay => BinanceSideEffectType::AutoRepay,
    }
}

pub fn to_binance_order_request(request: &AddOrderRequest, pair_conf: &PairConf) -> OrderRequest {
    OrderRequest {
        quantity: request.quantity,
        price: request.price.filter(|_| request.order_type != OrderType::Market),
        side: to_binance_order_side(request.side),
        order_type: to_binance_order_type(request.order_type),
        symbol: pair_conf.symbol.to_string(),
        time_in_force: request.enforcement.map(to_binance_time_in_force),
        iceberg_qty: request.iceberg_qty,
        recv_window: None,
        new_client_order_id: Some(request.order_id.clone()),
        new_order_resp_type: Some(OrderResponse::Full),
        quote_order_qty: request.quote_order_qty,
        stop_price: request.stop_price,
    }
}

pub fn to_binance_margin_order(request: &AddOrderRequest, pair_conf: &PairConf, asset_type: AssetType) -> MarginOrder {
    MarginOrder {
        quantity: request.quantity,
        price: request.price.filter(|_| request.order_type != OrderType::Market),
        side: to_binance_order_side(request.side),
        order_type: to_binance_order_type(request.order_type),
        symbol: pair_conf.symbol.to_string(),
        time_in_force: request.enforcement.map(to_binance_time_in_force),
        is_isolated: Some(is_isolated_margin_str(asset_type)),
        iceberg_qty: request.iceberg_qty,
        new_client_order_id: Some(request.order_id.clone()),
        new_order_resp_type: OrderResponse::Full,
        quote_order_qty: request.quote_order_qty,
        stop_price: request.stop_price,
        side_effect_type: to_binance_margin_side_effect(
            request.side_effect_type.unwrap_or(MarginSideEffect::NoSideEffect),
        ),
    }
}

#[allow(clippy::cast_possible_wrap)]
pub fn from_binance_transaction(bt: BinanceTransaction) -> OrderSubmission {
    OrderSubmission {
        timestamp: bt.transact_time as i64,
        id: bt.order_id.to_string(),
        pair: Pair::default(),
        client_id: bt.client_order_id,
        price: bt.price,
        qty: bt.orig_qty,
        executed_qty: bt.executed_qty,
        cummulative_quote_qty: bt.cummulative_quote_qty,
        status: from_binance_order_status(bt.status),
        enforcement: from_binance_time_in_force(bt.time_in_force),
        order_type: from_binance_order_type(bt.order_type),
        side: from_binance_order_side(bt.side),
        asset_type: AssetType::Spot,
        trades: bt.fills.into_iter().map(from_binance_fill).collect(),
        borrowed_amount: None,
        borrow_asset: None,
    }
}

#[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
pub fn from_binance_margin_order_result(mor: MarginOrderResult) -> OrderSubmission {
    OrderSubmission {
        timestamp: mor.transact_time as i64,
        id: mor.order_id.to_string(),
        pair: Pair::default(),
        client_id: mor.client_order_id,
        price: mor.price,
        qty: mor.orig_qty,
        executed_qty: mor.executed_qty,
        cummulative_quote_qty: mor.cummulative_quote_qty,
        status: from_binance_order_status(mor.status),
        enforcement: from_binance_time_in_force(mor.time_in_force),
        order_type: from_binance_order_type(mor.order_type),
        side: from_binance_order_side(mor.side),
        asset_type: margin_asset_type(mor.is_isolated),
        trades: mor.fills.into_iter().map(from_binance_fill).collect(),
        borrow_asset: mor.margin_buy_borrow_asset,
        borrowed_amount: mor.margin_buy_borrow_amount,
    }
}

pub fn from_binance_fill(t: Fill) -> OrderFill {
    OrderFill {
        id: None,
        price: t.price,
        qty: t.qty,
        fee: t.commission,
        fee_asset: t.commission_asset.into(),
    }
}

pub fn from_binance_user_asset(ua: UserAsset) -> MarginAsset {
    MarginAsset {
        asset: ua.asset,
        borrowed: ua.borrowed,
        free: ua.free,
        interest: ua.interest,
        locked: ua.locked,
        net_asset: ua.net_asset,
    }
}

pub fn from_binance_margin_account_details(bmd: BinanceMarginAccountDetails) -> MarginAccountDetails {
    MarginAccountDetails {
        summary: MarginAssetSummary {
            asset: "BTC".to_string(),
            margin_level: bmd.margin_level,
            total: bmd.total_asset_of_btc,
            total_liability: bmd.total_liability_of_btc,
            total_net: bmd.total_net_asset_of_btc,
        },
        borrow_enabled: bmd.borrow_enabled,
        trade_enabled: bmd.trade_enabled,
        transfer_enabled: bmd.transfer_enabled,
        margin_assets: bmd.user_assets.into_iter().map(from_binance_user_asset).collect(),
    }
}

pub fn from_binance_isolated_margin_account_details(imad: IsolatedMarginAccountDetails) -> MarginAccountDetails {
    MarginAccountDetails {
        summary: MarginAssetSummary {
            asset: "BTC".to_string(),
            margin_level: 0.0,
            total: imad.total_asset_of_btc.unwrap_or(0.0),
            total_liability: imad.total_liability_of_btc.unwrap_or(0.0),
            total_net: imad.total_net_asset_of_btc.unwrap_or(0.0),
        },
        borrow_enabled: false,
        trade_enabled: false,
        transfer_enabled: false,
        margin_assets: imad
            .assets
            .into_iter()
            .flat_map(|ua| {
                vec![
                    from_binance_isolated_margin_account_asset(ua.base_asset),
                    from_binance_isolated_margin_account_asset(ua.quote_asset),
                ]
            })
            .collect(),
    }
}

pub fn from_binance_isolated_margin_account_asset(asset: IsolatedMarginAccountAsset) -> MarginAsset {
    MarginAsset {
        asset: asset.asset,
        borrowed: asset.borrowed,
        free: asset.free,
        interest: asset.interest,
        locked: asset.locked,
        net_asset: asset.net_asset,
    }
}

pub fn is_isolated_margin_str(t: AssetType) -> String { bool_to_string(t == AssetType::IsolatedMargin) }

pub fn margin_asset_type(is_isolated: Option<bool>) -> AssetType {
    if is_isolated.unwrap_or(false) {
        AssetType::IsolatedMargin
    } else {
        AssetType::Margin
    }
}

pub fn from_binance_balance(b: BinanceBalance) -> Balance {
    Balance {
        free: b.free,
        locked: b.locked,
    }
}

pub fn from_binance_error(e: BinanceError) -> broker_core::error::Error {
    match e {
        BinanceError::InvalidPrice => Error::InvalidPrice,
        _ => Error::ExchangeError(format!("{:?}", e)),
    }
}

#[cfg(test)]
mod test {
    use binance::account::OrderRequest;
    use binance::rest_model::MarginOrder;

    use crate::adapters::{to_binance_margin_order, to_binance_order_request};
    use broker_core::pair::PairConf;
    use broker_core::types::AssetType;
    use broker_core::types::{AddOrderRequest, OrderType};

    #[tokio::test]
    async fn test_add_order_request_to_binance_price_erased() {
        let order_request = AddOrderRequest {
            order_type: OrderType::Market,
            price: Some(1.0),
            ..AddOrderRequest::default()
        };
        let binance_request: OrderRequest = to_binance_order_request(&order_request, &PairConf::default());
        assert_eq!(binance_request.price, None);
        let binance_margin_request: MarginOrder =
            to_binance_margin_order(&order_request, &PairConf::default(), AssetType::Margin);
        assert_eq!(binance_margin_request.price, None);
        let binance_margin_request: MarginOrder =
            to_binance_margin_order(&order_request, &PairConf::default(), AssetType::IsolatedMargin);
        assert_eq!(binance_margin_request.price, None);
        let order_request = AddOrderRequest {
            order_type: OrderType::Limit,
            price: Some(1.0),
            ..AddOrderRequest::default()
        };
        let binance_request: OrderRequest = to_binance_order_request(&order_request, &PairConf::default());
        assert_eq!(binance_request.price, Some(1.0));
        let binance_margin_request: MarginOrder =
            to_binance_margin_order(&order_request, &PairConf::default(), AssetType::Margin);
        assert_eq!(binance_margin_request.price, Some(1.0));
        let binance_margin_request: MarginOrder =
            to_binance_margin_order(&order_request, &PairConf::default(), AssetType::IsolatedMargin);
        assert_eq!(binance_margin_request.price, Some(1.0));
    }
}
