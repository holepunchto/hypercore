use std::time::{Duration, Instant};

#[cfg(feature = "async-std")]
use criterion::async_executor::AsyncStdExecutor;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use hypercore::{Hypercore, HypercoreBuilder, HypercoreError, Storage};
use random_access_memory::RandomAccessMemory;

fn bench_create_memory(c: &mut Criterion) {
    #[cfg(feature = "async-std")]
    c.bench_function("create memory", |b| {
        b.to_async(AsyncStdExecutor).iter(|| create_hypercore(1024));
    });
    #[cfg(feature = "tokio")]
    c.bench_function("create memory", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter(|| create_hypercore(1024));
    });
}

#[cfg(feature = "cache")]
async fn create_hypercore(page_size: usize) -> Result<Hypercore, HypercoreError> {
    use hypercore::StorageTraits;

    let storage = Storage::open(
        |_| {
            Box::pin(async move {
                Ok(Box::new(RandomAccessMemory::new(page_size)) as Box<dyn StorageTraits + Send>)
            })
        },
        false,
    )
    .await?;
    HypercoreBuilder::new(storage)
        .node_cache_options(hypercore::CacheOptionsBuilder::new())
        .build()
        .await
}

#[cfg(not(feature = "cache"))]
async fn create_hypercore(page_size: usize) -> Result<Hypercore, HypercoreError> {
    use hypercore::StorageTraits;

    let storage = Storage::open(
        |_| {
            Box::pin(async move {
                Ok(Box::new(RandomAccessMemory::new(page_size)) as Box<dyn StorageTraits + Send>)
            })
        },
        false,
    )
    .await?;
    HypercoreBuilder::new(storage).build().await
}

fn bench_write_memory(c: &mut Criterion) {
    #[cfg(feature = "async-std")]
    c.bench_function("write memory", |b| {
        b.to_async(AsyncStdExecutor).iter_custom(write_memory);
    });
    #[cfg(feature = "tokio")]
    c.bench_function("write memory", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(write_memory);
    });
}

async fn write_memory(iters: u64) -> Duration {
    let mut hypercore = create_hypercore(1024).await.unwrap();
    let data = Vec::from("hello");
    let start = Instant::now();
    for _ in 0..iters {
        black_box(hypercore.append(&data).await.unwrap());
    }
    start.elapsed()
}

fn bench_read_memory(c: &mut Criterion) {
    #[cfg(feature = "async-std")]
    c.bench_function("read memory", |b| {
        b.to_async(AsyncStdExecutor).iter_custom(read_memory);
    });
    #[cfg(feature = "tokio")]
    c.bench_function("read memory", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(read_memory);
    });
}

async fn read_memory(iters: u64) -> Duration {
    let mut hypercore = create_hypercore(1024).await.unwrap();
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

fn bench_clear_memory(c: &mut Criterion) {
    #[cfg(feature = "async-std")]
    c.bench_function("clear memory", |b| {
        b.to_async(AsyncStdExecutor).iter_custom(clear_memory);
    });
    #[cfg(feature = "tokio")]
    c.bench_function("clear memory", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter_custom(clear_memory);
    });
}

#[allow(clippy::unit_arg)]
async fn clear_memory(iters: u64) -> Duration {
    let mut hypercore = create_hypercore(1024).await.unwrap();
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
    bench_create_memory,
    bench_write_memory,
    bench_read_memory,
    bench_clear_memory
);
criterion_main!(benches);
