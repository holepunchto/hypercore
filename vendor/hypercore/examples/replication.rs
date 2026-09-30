#[cfg(feature = "async-std")]
use async_std::main as async_main;
use hypercore::{Hypercore, HypercoreBuilder, HypercoreError, PartialKeypair, Storage};
use hypercore_schema::{RequestBlock, RequestUpgrade};
use tempfile::Builder;
#[cfg(feature = "tokio")]
use tokio::main as async_main;

/// Example on how to replicate a (disk) hypercore to another (memory) hypercore.
/// NB: The replication functions used here are low-level, built for use in the wire
/// protocol.
#[async_main]
async fn main() {
    // For the purposes of this example, first create a
    // temporary directory to hold hypercore.
    let dir = Builder::new()
        .prefix("examples_replication")
        .tempdir()
        .unwrap()
        .keep();

    // Create a disk storage, overwriting existing values.
    let overwrite = true;
    let storage = Storage::new_disk(&dir, overwrite)
        .await
        .expect("Could not create disk storage");

    // Build a new disk hypercore
    let mut origin_hypercore = HypercoreBuilder::new(storage)
        .build()
        .await
        .expect("Could not create disk hypercore");

    // Append values to the hypercore
    let batch: &[&[u8]] = &[b"Hello, ", b"from ", b"replicated ", b"hypercore!"];
    origin_hypercore.append_batch(batch).await.unwrap();

    // Store the public key
    let origin_public_key = origin_hypercore.key_pair().public;

    // Create a peer of the origin hypercore using the public key
    let mut replicated_hypercore = HypercoreBuilder::new(
        Storage::new_memory()
            .await
            .expect("Could not create memory storage"),
    )
    .key_pair(PartialKeypair {
        public: origin_public_key,
        secret: None,
    })
    .build()
    .await
    .expect("Could not create memory hypercore");

    // Replicate the four values in random order
    replicate_index(&mut origin_hypercore, &mut replicated_hypercore, 3).await;
    replicate_index(&mut origin_hypercore, &mut replicated_hypercore, 0).await;
    replicate_index(&mut origin_hypercore, &mut replicated_hypercore, 2).await;
    replicate_index(&mut origin_hypercore, &mut replicated_hypercore, 1).await;

    // Print values from replicated hypercore, converting binary back to string
    println!(
        "{}{}{}{}",
        format_res(replicated_hypercore.get(0).await),
        format_res(replicated_hypercore.get(1).await),
        format_res(replicated_hypercore.get(2).await),
        format_res(replicated_hypercore.get(3).await)
    ); // prints "Hello, from replicated hypercore!"
}

async fn replicate_index(
    origin_hypercore: &mut Hypercore,
    replicated_hypercore: &mut Hypercore,
    request_index: u64,
) {
    let missing_nodes = origin_hypercore
        .missing_nodes(request_index)
        .await
        .expect("Could not get missing nodes");
    let upgrade_start = replicated_hypercore.info().contiguous_length;
    let upgrade_length = origin_hypercore.info().contiguous_length - upgrade_start;

    let proof = origin_hypercore
        .create_proof(
            Some(RequestBlock {
                index: request_index,
                nodes: missing_nodes,
            }),
            None,
            None,
            Some(RequestUpgrade {
                start: upgrade_start,
                length: upgrade_length,
            }),
        )
        .await
        .expect("Creating proof error")
        .expect("Could not get proof");
    // Then the proof is verified and applied to the replicated party.
    assert!(
        replicated_hypercore
            .verify_and_apply_proof(&proof)
            .await
            .expect("Verifying and applying proof failed")
    );
}

fn format_res(res: Result<Option<Vec<u8>>, HypercoreError>) -> String {
    match res {
        Ok(Some(bytes)) => String::from_utf8(bytes).expect("Shouldn't fail in example"),
        Ok(None) => "Got None in feed".to_string(),
        Err(e) => format!("Error getting value from feed, reason = {e:?}"),
    }
}
