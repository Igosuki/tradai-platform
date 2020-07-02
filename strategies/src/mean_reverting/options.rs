use coinnect_rt::types::Pair;

#[derive(Clone, Debug, Deserialize)]
pub struct Options {
    pub pair: Pair,
    pub short_window_size: usize,
    pub long_window_size: usize,
    pub dry_mode: Option<bool>,
}

impl Options {
    pub(super) fn dry_mode(&self) -> bool {
        self.dry_mode.unwrap_or(true)
    }
}
