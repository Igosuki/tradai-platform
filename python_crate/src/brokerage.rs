use brokers::prelude::MarketEventEnvelope;
use pyo3::{PyObject, PyResult, Python};

#[pyclass(name = "MarketEvent", module = "tradai", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct PyMarketEvent {
    pub(crate) inner: MarketEventEnvelope,
}

#[pymethods]
impl PyMarketEvent {
    /// Log the event as a debug string
    fn debug(&self) {
        info!("{:?}", self);
    }

    /// Volume Weighted Average Price
    pub fn vwap(&self) -> f64 { self.inner.e.vwap() }

    /// High price for this event
    pub fn high(&self) -> f64 { self.inner.e.high() }

    /// Low price for this event
    pub fn low(&self) -> f64 { self.inner.e.low() }

    /// Close price for this event
    pub fn close(&self) -> f64 { self.inner.e.close() }

    /// Get the event as a python dict
    pub fn as_dict(&self) -> PyResult<PyObject> { Python::with_gil(|py| Ok(pythonize::pythonize(py, &self.inner)?)) }
}

impl From<PyMarketEvent> for MarketEventEnvelope {
    fn from(event: PyMarketEvent) -> MarketEventEnvelope { event.inner }
}

impl From<MarketEventEnvelope> for PyMarketEvent {
    fn from(e: MarketEventEnvelope) -> Self { PyMarketEvent { inner: e } }
}

#[pyclass(name = "MarketEvents", module = "tradai", subclass)]
#[derive(Debug, Clone)]
pub(crate) struct PyMarketEvents {
    pub(crate) inner: Vec<MarketEventEnvelope>,
}

impl From<Vec<MarketEventEnvelope>> for PyMarketEvents {
    fn from(e: Vec<MarketEventEnvelope>) -> Self { PyMarketEvents { inner: e } }
}
