use std::io::{Read, Write};

use serde::Serialize;
use serde_json::Value;

use brokers::types::MarketEventEnvelope;

use crate::error::Result;

pub trait LoadableModel {
    /// Overwrite the current model using the reader
    /// If true, will read the model as snappy compressed
    /// Warning : in case of failure, this may lead to loss of data
    fn import<R: Read>(&mut self, read: R, compressed: bool) -> Result<()>;

    /// Write the current model value,
    /// If true, will write the model as snappy compressed
    fn export<W: Write>(&self, out: W, compressed: bool) -> Result<()>;
}

pub type SerializedModel = Vec<(String, Option<Value>)>;

pub trait IterativeModel {
    type ExportValue: Serialize;

    /// Generate the next model value from the input event
    fn next_model(&mut self, e: &MarketEventEnvelope) -> Result<()>;

    /// Serialize a short, readable version of the model
    fn export_values(&self) -> Result<Self::ExportValue>;
}
