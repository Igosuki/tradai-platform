use actix::Addr;

use coinnect_rt::exchange::Exchange;
use coinnect_rt::margin_interest_rates::{GetInterestRate, MarginInterestRateProvider};
use coinnect_rt::types::InterestRate;
use ext::ResultExt;

use crate::error::*;
use crate::order_manager::types::OrderDetail;
use crate::types::{StopEvent, StratEvent};

#[derive(Debug)]
pub(crate) struct Stopper<T> {
    stop_gain: T,
    stop_loss: T,
}

impl<T: std::cmp::PartialOrd + Copy> Stopper<T> {
    pub(crate) fn new(stop_gain: T, stop_loss: T) -> Self { Self { stop_gain, stop_loss } }

    /// Returns `Some(StopEvent)` if the stop conditions are matched, `None` otherwise
    pub(crate) fn should_stop(&self, ret: T) -> Option<StopEvent> {
        if ret > self.stop_gain {
            Some(StopEvent::Gain)
        } else if ret < self.stop_loss {
            Some(StopEvent::Loss)
        } else {
            None
        }
    }
}

pub fn maybe_log_stop(stop_event: Option<StopEvent>) {
    stop_event.map(|stop| {
        let strat_event = StratEvent::Stop { stop };
        strat_event.log();
        strat_event
    });
}

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
