use chrono::{DateTime, Utc};
use std::path::PathBuf;

pub mod file_actor;
pub mod live_event;
mod rotate;

#[derive(Hash, PartialEq, Eq)]
pub struct Partition {
    path: PathBuf,
    expires_at: Option<DateTime<Utc>>,
}

impl Partition {
    fn new(path: PathBuf, expires_at: Option<DateTime<Utc>>) -> Self { Self { path, expires_at } }

    fn is_expired(&self) -> bool { self.expires_at.map(|expiry| expiry < Utc::now()).unwrap_or(false) }
}

pub trait Partitioner<T> {
    fn partition(&self, data: &T) -> Option<Partition>;
}
