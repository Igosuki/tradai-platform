use yata::core::{Error, IndicatorConfig, IndicatorInstance, IndicatorResult, Method, MovingAverageConstructor, Source,
                 OHLCV};
use yata::helpers::MA;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PercentPriceOscillator<M: MovingAverageConstructor = MA> {
    pub long_ma: M,
    pub short_ma: M,
    pub source: Source,
}

impl<M: MovingAverageConstructor> IndicatorConfig for PercentPriceOscillator<M> {
    type Instance = PercentPriceOscillatorInstance<M>;
    const NAME: &'static str = "PercentPriceOscillator";

    fn validate(&self) -> bool { self.long_ma.ma_period() > self.short_ma.ma_period() }

    fn set(&mut self, name: &str, value: String) -> Result<(), yata::core::Error> {
        match name {
            "long_ma" => match value.parse() {
                Err(_) => return Err(Error::ParameterParse(name.to_string(), value.to_string())),
                Ok(value) => self.long_ma = value,
            },
            "short_ma" => match value.parse() {
                Err(_) => return Err(Error::ParameterParse(name.to_string(), value.to_string())),
                Ok(value) => self.short_ma = value,
            },
            "source" => match value.parse() {
                Err(_) => return Err(Error::ParameterParse(name.to_string(), value.to_string())),
                Ok(value) => self.source = value,
            },

            _ => {
                return Err(Error::ParameterParse(name.to_string(), value));
            }
        };
        Ok(())
    }

    fn size(&self) -> (u8, u8) { (1, 0) }

    fn init<T: OHLCV>(self, initial_value: &T) -> Result<Self::Instance, Error> {
        if !self.validate() {
            return Err(Error::WrongConfig);
        }

        let cfg = self;
        let src = initial_value.source(cfg.source);

        Ok(Self::Instance {
            long_ma: cfg.long_ma.init(src)?, // method(cfg.method, cfg.period, src)?,
            short_ma: cfg.short_ma.init(src)?,
            ppo: f64::NAN,
            cfg,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PercentPriceOscillatorInstance<M: MovingAverageConstructor = MA> {
    long_ma: M::Instance,
    short_ma: M::Instance,
    ppo: f64,
    cfg: PercentPriceOscillator<M>,
}

impl<M: MovingAverageConstructor> IndicatorInstance for PercentPriceOscillatorInstance<M> {
    type Config = PercentPriceOscillator<M>;

    fn config(&self) -> &Self::Config { &self.cfg }

    fn next<T: OHLCV>(&mut self, candle: &T) -> IndicatorResult {
        let src = candle.source(self.cfg.source);
        let long_ma: f64 = self.long_ma.next(&src);
        let short_ma: f64 = self.short_ma.next(&src);
        self.ppo = (short_ma - long_ma) / long_ma;

        IndicatorResult::new(&[self.ppo], &[])
    }
}
