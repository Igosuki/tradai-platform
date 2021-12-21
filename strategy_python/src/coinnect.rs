use strategy::coinnect::prelude::MarketEventEnvelope;

#[pyclass(name = "MarketEvent", module = "strategy", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct PyMarketEvent {
    pub(crate) inner: MarketEventEnvelope,
}

impl From<PyMarketEvent> for MarketEventEnvelope {
    fn from(event: PyMarketEvent) -> MarketEventEnvelope { event.inner }
}

impl From<MarketEventEnvelope> for PyMarketEvent {
    fn from(e: MarketEventEnvelope) -> Self { PyMarketEvent { inner: e } }
}
