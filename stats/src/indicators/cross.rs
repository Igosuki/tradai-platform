use ta::Next;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct CrossAbove {
    pub last_delta: f64,
}

impl CrossAbove {
    /// Returns `true` when value1 crosses `value2` timeseries upwards
    /// Otherwise returns `false`
    #[inline]
    pub fn binary(&mut self, value1: f64, value2: f64) -> bool {
        let last_delta = self.last_delta;
        let current_delta = value1 - value2;

        self.last_delta = current_delta;

        last_delta < 0. && current_delta >= 0.
    }
    pub fn new(value: &(f64, f64)) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            last_delta: value.0 - value.1,
        })
    }
}

impl Next<(f64, f64)> for CrossAbove {
    type Output = bool;

    #[inline]
    fn next(&mut self, value: (f64, f64)) -> Self::Output { self.binary(value.0, value.1) }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct CrossUnder {
    pub last_delta: f64,
}

impl CrossUnder {
    /// Returns `true` when value1 crosses `value2` timeseries upwards
    /// Otherwise returns `false`
    #[inline]
    pub fn binary(&mut self, value1: f64, value2: f64) -> bool {
        let last_delta = self.last_delta;
        let current_delta = value1 - value2;

        self.last_delta = current_delta;

        last_delta > 0. && current_delta <= 0.
    }
    pub fn new(value: &(f64, f64)) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            last_delta: value.0 - value.1,
        })
    }
}

impl Next<(f64, f64)> for CrossUnder {
    type Output = bool;

    #[inline]
    fn next(&mut self, value: (f64, f64)) -> Self::Output { self.binary(value.0, value.1) }
}
