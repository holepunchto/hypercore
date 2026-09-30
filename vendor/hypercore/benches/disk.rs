use std::time::{Duration, Instant};

#[cfg(feature = "async-std")]
use criterion::async_executor::AsyncStdExecutor;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use hypercore::{Hypercore, HypercoreBuilder, HypercoreError, Storage};
use tempfile::Builder as TempfileBuilder;

fn bench_create_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("slow_call");
    group.measurement_time(Duration::from_secs(20));

    #[cfg(feature = "async-std")]
    group.bench_function("create_disk", move |b| {
        b.to_async(AsyncStdExecutor)
            .iter(|| create_hypercore("create"));
    });
    #[cfg(feature = "tokio")]
    group.bench_function("create_disk", move |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter(|| create_hypercore("create"));
    });
}

#[cfg(feature = "cache")]
async fn create_hypercore(name: &str) -> Result<Hypercore, HypercoreError> {
    let dir = TempfileBuilder::new()
        .prefix(name)
        .tempdir()
        .unwrap()
        .keep();
    let storage = Storage::new_disk(&dir, true).await?;
    HypercoreBuilder::new(storage)
        .node_cache_options(hypercore::CacheOptionsBuilder::new())
        .build()
        .await
}

#[cfg(not(feature = "cache"))]
async fn create_hypercore(name: &str) -> Result<Hypercore, HypercoreError> {
    let dir = TempfileBuilder::new()
        .prefix(name)
        .tempdir()
        .unwrap()
        .keep();
    let storage = Storage::new_disk(&dir, true).await?;
    HypercoreBuilder::new(storage).build().await
}

fn bench_write_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("slow_call");
    group.measurement_time(Duration::from_secs(20));

    #[cfg(feature = "async-std")]
    group.bench_function("write disk", |b| {
        b.to_async(AsyncStdExecutor).iter_custom(write_disk);
    });
    #[cfg(feature = "tokio")]
    group.bench_function("write disk", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(write_disk);
    });
}

async fn write_disk(iters: u64) -> Duration {
    let mut hypercore = create_hypercore("write").await.unwrap();
    let data = Vec::from("hello");
    let start = Instant::now();
    for _ in 0..iters {
        black_box(hypercore.append(&data).await.unwrap());
    }
    start.elapsed()
}

fn bench_read_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("slow_call");
    group.measurement_time(Duration::from_secs(20));

    #[cfg(feature = "async-std")]
    group.bench_function("read disk", |b| {
        b.to_async(AsyncStdExecutor).iter_custom(read_disk);
    });
    #[cfg(feature = "tokio")]
    group.bench_function("read disk", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(read_disk);
    });
}

async fn read_disk(iters: u64) -> Duration {
    let mut hypercore = create_hypercore("read").await.unwrap();
    let data = Vec::from("hello");
    for _ in 0..iters {
        hypercore.append(&data).await.unwrap();
    }
    let start = Instant::now();
    for i in 0..iters {
        black_box(hypercore.get(i).await.unwrap());
    }
    start.elapsed()
}

fn bench_clear_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("slow_call");
    group.measurement_time(Duration::from_secs(20));

    #[cfg(feature = "async-std")]
    group.bench_function("clear disk", |b| {
        b.to_async(AsyncStdExecutor).iter_custom(clear_disk);
    });
    #[cfg(feature = "tokio")]
    group.bench_function("clear disk", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(clear_disk);
    });
}

#[allow(clippy::unit_arg)]
async fn clear_disk(iters: u64) -> Duration {
    let mut hypercore = create_hypercore("clear").await.unwrap();
    let data = Vec::from("hello");
    for _ in 0..iters {
        hypercore.append(&data).await.unwrap();
    }
    let start = Instant::now();
    for i in 0..iters {
        black_box(hypercore.clear(i, 1).await.unwrap());
    }
    start.elapsed()
}

criterion_group!(
    benches,
    bench_create_disk,
    bench_write_disk,
    bench_read_disk,
    bench_clear_disk
);
criterion_main!(benches);
