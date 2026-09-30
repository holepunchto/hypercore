pub mod common;

use anyhow::Result;
use common::{create_hypercore, get_test_key_pair, open_hypercore, storage_contains_data};
use hypercore::{HypercoreBuilder, Storage};
use tempfile::Builder;
use test_log::test;

#[cfg(feature = "async-std")]
use async_std::test as async_test;
#[cfg(feature = "tokio")]
use tokio::test as async_test;

#[test(async_test)]
async fn hypercore_new() -> Result<()> {
    let storage = Storage::new_memory().await?;
    let _hypercore = HypercoreBuilder::new(storage).build();
    Ok(())
}

#[test(async_test)]
async fn hypercore_new_with_key_pair() -> Result<()> {
    let storage = Storage::new_memory().await?;
    let key_pair = get_test_key_pair();
    let _hypercore = HypercoreBuilder::new(storage)
        .key_pair(key_pair)
        .build()
        .await?;
    Ok(())
}

#[test(async_test)]
async fn hypercore_open_with_key_pair_error() -> Result<()> {
    let storage = Storage::new_memory().await?;
    let key_pair = get_test_key_pair();
    assert!(
        HypercoreBuilder::new(storage)
            .key_pair(key_pair)
            .open(true)
            .build()
            .await
            .is_err()
    );
    Ok(())
}

#[test(async_test)]
async fn hypercore_make_read_only() -> Result<()> {
    let dir = Builder::new()
        .prefix("hypercore_make_read_only")
        .tempdir()
        .unwrap();
    let write_key_pair = {
        let mut hypercore = create_hypercore(&dir.path().to_string_lossy()).await?;
        hypercore.append(b"Hello").await?;
        hypercore.append(b"World!").await?;
        hypercore.key_pair().clone()
    };
    assert!(storage_contains_data(
        dir.path(),
        &write_key_pair.secret.as_ref().unwrap().to_bytes()
    ));
    assert!(write_key_pair.secret.is_some());
    let read_key_pair = {
        let mut hypercore = open_hypercore(&dir.path().to_string_lossy()).await?;
        assert_eq!(&hypercore.get(0).await?.unwrap(), b"Hello");
        assert_eq!(&hypercore.get(1).await?.unwrap(), b"World!");
        assert!(hypercore.make_read_only().await?);
        hypercore.key_pair().clone()
    };
    assert!(read_key_pair.secret.is_none());
    assert!(!storage_contains_data(
        dir.path(),
        &write_key_pair.secret.as_ref().unwrap().to_bytes()[16..],
    ));

    let mut hypercore = open_hypercore(&dir.path().to_string_lossy()).await?;
    assert_eq!(&hypercore.get(0).await?.unwrap(), b"Hello");
    assert_eq!(&hypercore.get(1).await?.unwrap(), b"World!");
    Ok(())
}
