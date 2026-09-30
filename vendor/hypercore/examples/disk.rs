#[cfg(feature = "async-std")]
use async_std::main as async_main;
use hypercore::{HypercoreBuilder, HypercoreError, Storage};
use tempfile::Builder;
#[cfg(feature = "tokio")]
use tokio::main as async_main;

/// Example about using an in-memory hypercore.
#[async_main]
async fn main() {
    // For the purposes of this example, first create a
    // temporary directory to hold hypercore.
    let dir = Builder::new()
        .prefix("examples_disk")
        .tempdir()
        .unwrap()
        .keep();

    // Create a disk storage, overwriting existing values.
    let overwrite = true;
    let storage = Storage::new_disk(&dir, overwrite)
        .await
        .expect("Could not create disk storage");

    // Build a new disk hypercore
    let mut hypercore = HypercoreBuilder::new(storage)
        .build()
        .await
        .expect("Could not create disk hypercore");

    // Append values to the hypercore
    hypercore.append(b"Hello, ").await.unwrap();
    hypercore.append(b"from ").await.unwrap();

    // Close hypercore
    drop(hypercore);

    // Open hypercore again from same directory, not
    // overwriting.
    let overwrite = false;
    let storage = Storage::new_disk(&dir, overwrite)
        .await
        .expect("Could not open existing disk storage");
    let mut hypercore = HypercoreBuilder::new(storage)
        .open(true)
        .build()
        .await
        .expect("Could not open disk hypercore");

    // Append new values to the hypercore
    hypercore.append(b"disk hypercore!").await.unwrap();

    // Add three values and clear the first two
    let batch: &[&[u8]] = &[
        b"first value to clear",
        b"second value to clear",
        b"third value to keep",
    ];
    let new_length = hypercore.append_batch(batch).await.unwrap().length;
    hypercore
        .clear(new_length - 3, new_length - 1)
        .await
        .unwrap();

    // The two values return None, but the last one returns correctly
    assert!(hypercore.get(3).await.unwrap().is_none());
    assert!(hypercore.get(4).await.unwrap().is_none());
    assert_eq!(
        hypercore.get(5).await.unwrap().unwrap(),
        b"third value to keep"
    );

    // Print the first three values, converting binary back to string
    println!(
        "{}{}{}",
        format_res(hypercore.get(0).await),
        format_res(hypercore.get(1).await),
        format_res(hypercore.get(2).await)
    ); // prints "Hello, from disk hypercore!"
}

fn format_res(res: Result<Option<Vec<u8>>, HypercoreError>) -> String {
    match res {
        Ok(Some(bytes)) => String::from_utf8(bytes).expect("Shouldn't fail in example"),
        Ok(None) => "Got None in feed".to_string(),
        Err(e) => format!("Error getting value from feed, reason = {e:?}"),
    }
}
