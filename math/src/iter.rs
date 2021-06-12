use itertools::Itertools;
use peroxide::statistics::stat::{OrderedStat, QType};

pub trait MeanExt: Iterator {
    fn mean<M>(self) -> M
    where
        M: Mean<Self::Item>,
        Self: Sized,
    {
        M::mean(self)
    }
}

impl<I: Iterator> MeanExt for I {}

pub trait Mean<A = Self> {
    fn mean<I>(iter: I) -> Self
    where
        I: Iterator<Item = A>;
}

impl Mean for f64 {
    fn mean<I>(iter: I) -> Self
    where
        I: Iterator<Item = f64>,
    {
        let mut sum = 0.0;
        let mut count: usize = 0;

        for v in iter {
            sum += v;
            count += 1;
        }

        if count > 0 {
            sum / (count as f64)
        } else {
            0.0
        }
    }
}

impl<'a> Mean<&'a f64> for f64 {
    fn mean<I>(iter: I) -> Self
    where
        I: Iterator<Item = &'a f64>,
    {
        iter.cloned().mean()
    }
}

pub trait VarianceExt: Iterator {
    fn variance<M>(self) -> M
    where
        M: Variance<Self::Item>,
        Self: Sized,
    {
        M::variance(self)
    }
}

impl<I: Iterator> VarianceExt for I {}

pub trait Variance<A = Self> {
    fn variance<I>(iter: I) -> Self
    where
        I: Iterator<Item = A>;
}

impl Variance for f64 {
    fn variance<I>(iter: I) -> Self
    where
        I: Iterator<Item = f64>,
    {
        let mut sum = 0.0;
        let mut count: usize = 0;
        let (for_mean, for_count) = iter.tee();
        let mean: f64 = for_mean.mean();
        for v in for_count {
            sum += (v - mean).powf(2.0);
            count += 1;
        }

        if count > 0 {
            sum / (count as f64)
        } else {
            0.0
        }
    }
}

impl<'a> Variance<&'a f64> for f64 {
    fn variance<I>(iter: I) -> Self
    where
        I: Iterator<Item = &'a f64>,
    {
        iter.cloned().variance()
    }
}

pub trait CovarianceExt: Iterator {
    fn covariance<M, B>(self) -> B
    where
        M: Covariance<B, Self::Item>,
        Self: Sized,
    {
        M::covariance(self)
    }
}

impl<I: Iterator> CovarianceExt for I {}

pub trait Covariance<B, A = Self> {
    fn covariance<I>(iter: I) -> B
    where
        I: Iterator<Item = A>;
}

impl Covariance<f64, (f64, f64)> for (f64, f64) {
    // sum((x[i] - mean(x)) * (y[i] - mean(y) )) / n ?
    fn covariance<I>(iter: I) -> f64
    where
        I: Iterator<Item = (f64, f64)>,
    {
        let mut sum = 0.0;
        let mut count: usize = 0;
        let (for_mean, for_count) = iter.tee();
        let (for_mean_1, for_mean_2) = for_mean.tee();
        let mean_1: f64 = for_mean_1.map(|f| f.0).mean();
        let mean_2: f64 = for_mean_2.map(|f| f.1).mean();
        for v in for_count {
            sum += (v.0 - mean_1) * (v.1 - mean_2);
            count += 1;
        }

        if count > 0 {
            sum / ((count) as f64)
        } else {
            0.0
        }
    }
}
//
// impl<'a> Covariance<&'a f64, &'a (f64, f64)> for (f64, f64) {
//     fn covariance<I>(iter: I) -> &'a f64
//         where I: Iterator<Item = &'a (f64, f64)>
//     {
//         iter.cloned().covariance()
//     }
// }

pub trait QuantileExt: Iterator {
    fn quantile<M>(self, prob: f64) -> M
    where
        M: Quantile<Self::Item>,
        Self: Sized,
    {
        M::quantile(self, prob)
    }
}

impl<I: Iterator> QuantileExt for I {}

pub trait Quantile<A = Self> {
    fn quantile<I>(iter: I, prob: f64) -> Self
    where
        I: Iterator<Item = A>;
}

impl Quantile for f64 {
    #[tracing::instrument(skip(iter), level = "trace")]
    fn quantile<I>(iter: I, prob: f64) -> Self
    where
        I: Iterator<Item = f64>,
    {
        let v: Vec<f64> = iter.collect();
        v.quantile(prob, QType::Type2)
    }
}

impl<'a> Quantile<&'a f64> for f64 {
    fn quantile<I>(iter: I, prob: f64) -> Self
    where
        I: Iterator<Item = &'a f64>,
    {
        iter.cloned().quantile(prob)
    }
}

#[cfg(test)]
mod test {
    use crate::iter::QuantileExt;
    use itertools::Itertools;
    use rand::Rng;
    use test::Bencher;

    #[bench]
    fn bench_test(b: &mut Bencher) {
        let mut rng = rand::thread_rng();
        let vals: Vec<f64> = (0..10000).map(|_| rng.gen_range((0.0..5000.0))).collect();
        b.iter(|| {
            let _: f64 = vals.clone().into_iter().quantile(0.99);
        });
    }
}
