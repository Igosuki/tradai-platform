use broker_core::currency::USDT;
use broker_core::fees::{Fee, FeeProvider};
use broker_core::prelude::AssetType;
use broker_core::types::{OrderType, SecurityType, Symbol};

struct BinanceFeeProvider {
    bnb_burn: bool,
    #[allow(dead_code)]
    vip_level: i8,
}

impl BinanceFeeProvider {
    #[allow(dead_code)]
    fn new(bnb_burn: bool, vip_level: i8) -> Self { Self { bnb_burn, vip_level } }
}

impl Default for BinanceFeeProvider {
    fn default() -> Self {
        Self {
            bnb_burn: false,
            vip_level: 0,
        }
    }
}

impl FeeProvider for BinanceFeeProvider {
    fn get_rate(&self, symbol: Symbol, asset_type: Option<AssetType>, order_type: Option<OrderType>) -> Fee {
        let order_type = order_type.unwrap_or(OrderType::Market);
        let fee_rate = match symbol.r#type {
            SecurityType::Future => match asset_type.unwrap_or(AssetType::PerpetualContract) {
                AssetType::PerpetualContract
                | AssetType::PerpetualSwap
                | AssetType::Futures
                | AssetType::UpsideProfitContract
                | AssetType::DownsideProfitContract
                | AssetType::UsdtMarginedFutures => {
                    let vip_rate = if self.vip_level == 0 {
                        if order_type.is_maker() {
                            0.0002
                        } else {
                            0.0004
                        }
                    } else if order_type.is_maker() {
                        0.0001 * (1.0 - 0.2 * self.vip_level as f64)
                    } else {
                        0.0005 * (1.0 - 0.1 * self.vip_level as f64)
                    };
                    if self.bnb_burn {
                        vip_rate * 0.9
                    } else {
                        vip_rate
                    }
                }
                AssetType::CoinMarginedFutures => {
                    if self.vip_level == 0 {
                        0.001
                    } else if order_type.is_maker() {
                        0.001 * (1.0 - 0.3 * self.vip_level as f64)
                    } else if self.vip_level > 3 {
                        0.001 * (1.0 - 0.1 * (-3 + self.vip_level) as f64)
                    } else {
                        0.001
                    }
                }
                _ => unimplemented!(),
            },
            SecurityType::Crypto => match asset_type.unwrap_or(AssetType::Spot) {
                AssetType::Spot | AssetType::Margin | AssetType::MarginFunding | AssetType::IsolatedMargin => {
                    let vip_rate = if self.vip_level == 0 {
                        0.001
                    } else if order_type.is_maker() {
                        0.001 * (1.0 - 0.1 * self.vip_level as f64)
                    } else {
                        0.001 * (1.0 - 0.1 * self.vip_level as f64)
                    };
                    if self.bnb_burn {
                        vip_rate * 0.75
                    } else {
                        vip_rate
                    }
                }
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        };
        Fee(fee_rate, String::from(USDT.value))
    }
}
