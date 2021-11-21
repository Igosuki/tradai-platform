use crate::error::{Error, Result};
use crate::order_manager::types::OrderDetail;
use actix::Addr;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::margin_interest_rates::{GetInterestRate, MarginInterestRateProvider};
use coinnect_rt::types::InterestRate;
use ext::ResultExt;

pub async fn get_interest_rate(
    provider: Addr<MarginInterestRateProvider>,
    exchange: Exchange,
    asset: String,
) -> Result<InterestRate> {
    provider
        .send(GetInterestRate { asset, exchange })
        .await
        .map_err(|_| Error::InterestRateProviderMailboxError)?
        .err_into()
}

pub async fn interest_fees_since(
    provider: Addr<MarginInterestRateProvider>,
    exchange: Exchange,
    order: &OrderDetail,
) -> Result<f64> {
    let i = if order.asset_type.is_margin() && order.borrowed_amount.is_some() {
        let interest_rate = get_interest_rate(provider, exchange, order.base_asset.clone()).await?;
        order.total_interest(interest_rate)
    } else {
        0.0
    };
    Ok(i)
}
