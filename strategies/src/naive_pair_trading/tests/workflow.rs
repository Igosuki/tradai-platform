#[cfg(test)]
mod test {
    use coinnect_rt::types::{AddOrderRequest, Pair, TradeType};

    use crate::order_types::{StagedOrder, TransactionStatus};
    use crate::test_util::{binance_account_ws, local_api};

    #[allow(dead_code)]
    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    #[actix_rt::test]
    async fn test_trade_workflow() {
        // System
        // Account stream
        let _account_server = crate::test_util::ws_it_server(binance_account_ws()).await;
        // Binance stream ?
        // Binance API
        let (_server, binance_api) = local_api().await;
        // Order Manager
        let test_dir = util::test::test_dir();
        let om = crate::order_manager::test_util::local_manager(test_dir, binance_api);

        // Workflow :
        // Strategy has reached a certain initial state
        // 1.
        // Strategy makes an open, order is rejected
        // Strategy receives a new event, order is retried and passes, ongoing operation is cleared
        // Strategy closes, order passes, ongoing operation is cleared and pnl is updated
        // 2.
        // Strategy makes an open, order passes and ongoing operation is cleared
        // Strategy closes, order passes, ongoing operation is cleared and pnl is updated
        let pair: Pair = "BTC_USDT".to_string().into();
        let staged_result = om
            .send(StagedOrder {
                request: AddOrderRequest {
                    pair,
                    price: Some(0.1),
                    dry_run: false,
                    quantity: Some(0.1),
                    side: TradeType::Buy,
                    ..AddOrderRequest::default()
                },
            })
            .await;
        // Strategy
        assert!(staged_result.is_ok());
        let res = staged_result.unwrap();
        println!("{:?}", &res);
        assert!(res.is_ok());
        if let Ok(tr) = res {
            assert_ne!(tr.id, "".to_string());
            assert!(!matches!(tr.status, TransactionStatus::Rejected(_)));
        }
    }
}
