use ta::Next;
use yata::core::Window;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Momentum {
    window: Window<f64>,
}

/// Just an alias for [Momentum] method
pub type Change = Momentum;

/// Just an alias for [Momentum] method
pub type MTM = Momentum;

impl Momentum {
    pub fn new(length: u8, value: f64) -> anyhow::Result<Self> {
        match length {
            0 => Err(anyhow!("wrong method parameter")),
            length => Ok(Self {
                window: Window::new(length, value),
            }),
        }
    }
}

impl Next<f64> for Momentum {
    type Output = f64;

    #[inline]
    fn next(&mut self, value: f64) -> Self::Output { value - self.window.push(value) }
}
