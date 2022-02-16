use brokers::prelude::MarketEventEnvelope;

#[pyclass(name = "MarketEvent", module = "strategy", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct PyMarketEvent {
    pub(crate) inner: MarketEventEnvelope,
}

#[pymethods]
impl PyMarketEvent {
    fn debug(&self) {
        info!("{:?}", self);
    }

    pub fn vwap(&self) -> f64 { self.inner.e.vwap() }

    pub fn high(&self) -> f64 { self.inner.e.high() }

    pub fn low(&self) -> f64 { self.inner.e.low() }

    pub fn close(&self) -> f64 { self.inner.e.close() }
}

impl From<PyMarketEvent> for MarketEventEnvelope {
    fn from(event: PyMarketEvent) -> MarketEventEnvelope { event.inner }
}

impl From<MarketEventEnvelope> for PyMarketEvent {
    fn from(e: MarketEventEnvelope) -> Self { PyMarketEvent { inner: e } }
}
