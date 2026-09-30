//! Actual JS11-generated compat:true fixture, not an invented interoperability claim.
use shadw_core::{Core, ReplicationBundle};
use tempfile::TempDir;

const TRUSTED_TEST_KEY: &str = "ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c";
fn fixture() -> (ReplicationBundle, Vec<Vec<u8>>) {
    let value: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/rust/js11-compat.json")).unwrap();
    let bundle = serde_json::from_value(value["bundle"].clone()).unwrap();
    let blocks = value["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| hex::decode(v.as_str().unwrap()).unwrap())
        .collect();
    (bundle, blocks)
}
fn trusted_key() -> [u8; 32] {
    hex::decode(TRUSTED_TEST_KEY).unwrap().try_into().unwrap()
}

#[tokio::test]
async fn actual_js11_compat_signed_bundle_verifies_in_rust() {
    let tmp = TempDir::new().unwrap();
    let (bundle, blocks) = fixture();
    let path = tmp.path().join("js-replica");
    let mut reader = Core::create_replica(&path, trusted_key()).await.unwrap();
    assert_eq!(
        reader.import_bundle(&bundle).await.unwrap().imported_blocks,
        5
    );
    for (index, expected) in blocks.iter().enumerate() {
        assert_eq!(
            reader.get(index as u64).await.unwrap().as_ref(),
            Some(expected)
        );
    }
    let audit = reader.audit().await.unwrap();
    assert_eq!(audit.verified_blocks, 5);
    assert!(audit.signed_head_verified);
    assert_eq!(audit.missing_blocks, 0);
    drop(reader);
    let mut reader = Core::open(&path).await.unwrap();
    assert_eq!(reader.audit().await.unwrap().verified_blocks, 5);
}

#[tokio::test]
async fn actual_js11_compat_sparse_fill_and_tampering() {
    let tmp = TempDir::new().unwrap();
    let (bundle, blocks) = fixture();
    let mut reader = Core::create_replica(&tmp.path().join("reader"), trusted_key())
        .await
        .unwrap();
    let mut corrupt = bundle.clone();
    corrupt.blocks[1].block.as_mut().unwrap().value = "ff".into();
    assert!(reader.import_bundle(&corrupt).await.is_err());
    assert_eq!(reader.info().length, 0);
    let mut partial = bundle.clone();
    partial.blocks = vec![bundle.blocks[4].clone()];
    reader.import_bundle(&partial).await.unwrap();
    assert_eq!(reader.get(4).await.unwrap(), Some(blocks[4].clone()));
    assert_eq!(reader.audit().await.unwrap().missing_blocks, 4);
    reader.import_bundle(&bundle).await.unwrap();
    assert_eq!(reader.audit().await.unwrap().verified_blocks, 5);
}
