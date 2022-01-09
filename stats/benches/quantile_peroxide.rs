use criterion::{criterion_group, criterion_main, Criterion};
use peroxide::fuga::Type2;
use peroxide::prelude::OrderedStat;

pub fn criterion_benchmark_quantile_peroxide(c: &mut Criterion) {
    let num_receivers = 10000;
    let mut x = vec![];
    for _i in 0..num_receivers {
        x.push(rand::random::<f64>());
    }

    c.bench_function("measure quantile peroxide separate", |b| {
        b.iter(|| {
            x.quantile(0.99, Type2);
            x.quantile(0.01, Type2);
        });
    });

    c.bench_function("measure quantile peroxide vec", |b| {
        b.iter(|| {
            x.quantiles(vec![0.01, 0.99], Type2);
        });
    });
}

criterion_group!(benches, criterion_benchmark_quantile_peroxide);
criterion_main!(benches);
