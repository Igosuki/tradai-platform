use crate::error::Error;
use crate::{Close, Next, Reset};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub struct ExponentialMovingAverage {
    length: u32,
    k: f64,
    pub current: f64,
    is_new: bool,
}

impl ExponentialMovingAverage {
    pub fn new(smoothing: f64, length: u32) -> anyhow::Result<Self> {
        match length {
            0 => Err(Error::InvalidParameter {
                name: "length".to_string(),
                expected: "!= 0".to_string(),
                found: format!("{}", length),
            }
            .into()),
            _ => {
                let k = smoothing / (length as f64 + 1f64);
                let indicator = Self {
                    length,
                    k,
                    current: 0f64,
                    is_new: true,
                };
                Ok(indicator)
            }
        }
    }

    pub fn length(&self) -> u32 { self.length }
}

impl Next<f64> for ExponentialMovingAverage {
    type Output = f64;

    fn next(&mut self, input: f64) -> Self::Output {
        if self.is_new {
            self.is_new = false;
            self.current = input;
        } else {
            self.current = self.k * input + (1.0 - self.k) * self.current;
        }
        self.current
    }
}

impl<'a, T: Close> Next<&'a T> for ExponentialMovingAverage {
    type Output = f64;

    fn next(&mut self, input: &'a T) -> Self::Output { self.next(input.close()) }
}

impl Reset for ExponentialMovingAverage {
    fn reset(&mut self) {
        self.current = 0.0;
        self.is_new = true;
    }
}

impl Default for ExponentialMovingAverage {
    fn default() -> Self { Self::new(2.0, 9).unwrap() }
}

impl fmt::Display for ExponentialMovingAverage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "EMA({})", self.length) }
}

#[cfg(test)]
mod test {
    use crate::indicators::ema::ExponentialMovingAverage;
    use crate::Next;
    use crate::Reset;

    #[test]
    fn test_new() {
        assert!(ExponentialMovingAverage::new(2.0, 0).is_err());
        assert!(ExponentialMovingAverage::new(2.0, 1).is_ok());
    }

    #[test]
    fn test_next() {
        let mut ema = ExponentialMovingAverage::new(2.0, 3).unwrap();

        assert!(approx_eq!(f64, ema.next(2.0), 2.0));
        assert!(approx_eq!(f64, ema.next(5.0), 3.5));
        assert!(approx_eq!(f64, ema.next(1.0), 2.25));
        assert!(approx_eq!(f64, ema.next(6.25), 4.25));
    }

    #[test]
    fn test_reset() {
        let mut ema = ExponentialMovingAverage::new(2.0, 5).unwrap();

        assert!(approx_eq!(f64, ema.next(4.0), 4.0));
        ema.next(10.0);
        ema.next(15.0);
        ema.next(20.0);
        assert!(!approx_eq!(f64, ema.next(4.0), 4.0));

        ema.reset();
        assert!(approx_eq!(f64, ema.next(4.0), 4.0));
    }

    #[test]
    fn test_default() { ExponentialMovingAverage::default(); }

    #[test]
    fn test_display() {
        let ema = ExponentialMovingAverage::new(2.0, 7).unwrap();
        assert_eq!(format!("{}", ema), "EMA(7)");
    }
}
