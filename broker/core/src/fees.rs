use crate::types::{AssetType, OrderType};
use std::fmt::Debug;

pub struct Fee(pub f64, pub String);

pub trait FeeProvider: Debug + Sync + Send {
    /// Gets the unique fees rate for a symbol and an asset type
    fn get_rate(&self, asset_type: Option<AssetType>, order_type: Option<OrderType>) -> Fee;
}

#[derive(Debug, Deserialize)]
pub struct FlatFeeProvider {
    flat_fee: f64,
    symbol: String,
}

impl FeeProvider for FlatFeeProvider {
    fn get_rate(&self, _asset_type: Option<AssetType>, _order_type: Option<OrderType>) -> Fee {
        Fee(self.flat_fee, self.symbol.clone())
    }
}
