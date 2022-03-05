use crate::types::{AssetType, OrderType, Symbol};
use string_cache::DefaultAtom;

pub struct Fee(pub f64, pub String);

pub trait FeeProvider {
    /// Gets the unique fees rate for a symbol and an asset type
    fn get_rate(&self, symbol: Symbol, asset_type: Option<AssetType>, order_type: Option<OrderType>) -> Fee;
}

pub struct FlatFeeProvider(pub f64, pub DefaultAtom);

impl FeeProvider for FlatFeeProvider {
    fn get_rate(&self, _symbol: Symbol, _asset_type: Option<AssetType>, _order_type: Option<OrderType>) -> Fee {
        Fee(self.0, self.1.to_string())
    }
}
