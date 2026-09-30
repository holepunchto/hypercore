use shadw_core::{Core, ReplicationBundle};
use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::TempDir;

#[tokio::test]
async fn persistence_verified_reads_batches_and_lock() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("writer");
    let mut writer = Core::create(&path).await.unwrap();
    assert!(Core::open(&path).await.is_err());
    assert_eq!(writer.append(b"hello").await.unwrap(), 0);
    assert_eq!(
        writer
            .append_batch(&[b"world".to_vec(), Vec::new()])
            .await
            .unwrap(),
        vec![1, 2]
    );
    assert_eq!(writer.get(0).await.unwrap().unwrap(), b"hello");
    assert_eq!(writer.get(2).await.unwrap().unwrap(), b"");
    assert!(writer.get(9).await.unwrap().is_none());
    let key = writer.public_key();
    drop(writer);
    let mut reopened = Core::open(&path).await.unwrap();
    assert_eq!(reopened.public_key(), key);
    assert_eq!(reopened.audit().await.unwrap().verified_blocks, 3);
    assert!(Core::create(&path).await.is_err());
}

#[tokio::test]
async fn sparse_replication_fill_duplicate_and_extension() {
    let tmp = TempDir::new().unwrap();
    let mut writer = Core::create(&tmp.path().join("w")).await.unwrap();
    writer
        .append_batch(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()])
        .await
        .unwrap();
    let mut reader = Core::create_replica(&tmp.path().join("r"), writer.public_key())
        .await
        .unwrap();
    assert!(reader.append(b"no").await.is_err());
    let bundle = writer.export_bundle(&[3]).await.unwrap();
    assert_eq!(
        reader.import_bundle(&bundle).await.unwrap().imported_blocks,
        1
    );
    assert_eq!(reader.info().length, 4);
    assert_eq!(reader.info().contiguous_length, 0);
    assert_eq!(reader.get(3).await.unwrap().unwrap(), b"d");
    assert!(reader.get(0).await.unwrap().is_none());
    assert_eq!(reader.audit().await.unwrap().missing_blocks, 3);
    assert!(reader.import_bundle(&bundle).await.is_err());
    reader
        .import_bundle(&writer.export_bundle(&[0, 1, 2]).await.unwrap())
        .await
        .unwrap();
    assert_eq!(reader.info().contiguous_length, 4);
    writer.append(b"e").await.unwrap();
    reader
        .import_bundle(&writer.export_bundle(&[0, 1, 2, 3, 4]).await.unwrap())
        .await
        .unwrap();
    assert_eq!(reader.get(4).await.unwrap().unwrap(), b"e");
    assert!(reader.import_bundle(&bundle).await.is_err());
    drop(reader);
    let mut reopened = Core::open(&tmp.path().join("r")).await.unwrap();
    assert!(!reopened.info().writable);
    assert_eq!(reopened.audit().await.unwrap().verified_blocks, 5);
}

#[tokio::test]
async fn invalid_multi_proof_is_rejected_before_any_mutation() {
    let tmp = TempDir::new().unwrap();
    let mut writer = Core::create(&tmp.path().join("w")).await.unwrap();
    writer
        .append_batch(&[b"good".to_vec(), b"also good".to_vec()])
        .await
        .unwrap();
    let mut reader = Core::create_replica(&tmp.path().join("r"), writer.public_key())
        .await
        .unwrap();
    let bundle = writer.export_bundle(&[0, 1]).await.unwrap();
    let mut corrupted = bundle.clone();
    corrupted.blocks[1].block.as_mut().unwrap().value = "00".into();
    assert!(reader.import_bundle(&corrupted).await.is_err());
    assert_eq!(reader.info().length, 0);
    assert!(reader.get(0).await.unwrap().is_none());
    let mut signature = bundle.clone();
    signature.blocks[0].upgrade.signature = "00".repeat(64);
    assert!(reader.import_bundle(&signature).await.is_err());
    assert_eq!(reader.info().length, 0);
    let mut duplicate = bundle.clone();
    duplicate.blocks.push(duplicate.blocks[0].clone());
    assert!(reader.import_bundle(&duplicate).await.is_err());
    reader.import_bundle(&bundle).await.unwrap();
    assert_eq!(reader.audit().await.unwrap().verified_blocks, 2);
}

#[tokio::test]
async fn wrong_key_and_json_roundtrip_and_bounded_nodes() {
    let tmp = TempDir::new().unwrap();
    let mut writer = Core::create(&tmp.path().join("w")).await.unwrap();
    writer.append(b"entry").await.unwrap();
    let other = Core::create(&tmp.path().join("other")).await.unwrap();
    let mut reader = Core::create_replica(&tmp.path().join("r"), other.public_key())
        .await
        .unwrap();
    let original = writer.export_bundle(&[0]).await.unwrap();
    let encoded = serde_json::to_vec(&original).unwrap();
    let bundle: ReplicationBundle = serde_json::from_slice(&encoded).unwrap();
    assert!(reader.import_bundle(&bundle).await.is_err());
    let mut forged = bundle;
    forged.public_key = hex::encode(other.public_key());
    assert!(reader.import_bundle(&forged).await.is_err());
    assert_eq!(reader.info().length, 0);
    let mut invalid = original;
    invalid.head.as_mut().unwrap().upgrade.nodes[0].index = u64::MAX;
    assert!(reader.import_bundle(&invalid).await.is_err());
}

#[tokio::test]
async fn actual_disk_corruption_is_detected_by_get_audit_and_export() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("w");
    let mut writer = Core::create(&path).await.unwrap();
    writer.append(b"original").await.unwrap();
    drop(writer);
    let mut data = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join("data"))
        .unwrap();
    let mut value = [0];
    data.read_exact(&mut value).unwrap();
    data.seek(SeekFrom::Start(0)).unwrap();
    data.write_all(&[value[0] ^ 1]).unwrap();
    data.sync_all().unwrap();
    drop(data);
    let mut writer = Core::open(&path).await.unwrap();
    assert!(writer.get(0).await.is_err());
    assert!(writer.audit().await.is_err());
    assert!(writer.export_bundle(&[0]).await.is_err());
}

#[tokio::test]
async fn empty_and_foreign_stores_are_not_overwritten() {
    let tmp = TempDir::new().unwrap();
    let missing = tmp.path().join("missing");
    assert!(Core::open(&missing).await.is_err());
    assert!(!missing.exists());
    let foreign = tmp.path().join("js-store");
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("CURRENT"), b"rocksdb").unwrap();
    assert!(Core::open(&foreign).await.is_err());
    assert!(!foreign.join("shadw.lock").exists());
    assert!(Core::create(&foreign).await.is_err());
    assert!(
        Core::create_replica(&tmp.path().join("weak-key"), [0; 32])
            .await
            .is_err()
    );
    assert!(!tmp.path().join("weak-key").exists());
    let mut writer = Core::create(&tmp.path().join("w")).await.unwrap();
    let report = writer.audit().await.unwrap();
    assert!(!report.signed_head_verified);
    assert_eq!(report.verified_blocks, 0);
    assert_eq!(writer.append_batch(&[]).await.unwrap(), Vec::<u64>::new());
    let bundle = writer.export_bundle(&[]).await.unwrap();
    let mut reader = Core::create_replica(&tmp.path().join("r"), writer.public_key())
        .await
        .unwrap();
    assert_eq!(reader.import_bundle(&bundle).await.unwrap().length, 0);
    assert!(writer.export_bundle(&[0]).await.is_err());
}

#[tokio::test]
async fn conflicting_same_key_history_is_rejected() {
    let tmp = TempDir::new().unwrap();
    let original = tmp.path().join("a");
    let fork = tmp.path().join("b");
    let writer = Core::create(&original).await.unwrap();
    let key = writer.public_key();
    drop(writer);
    std::fs::create_dir(&fork).unwrap();
    for entry in std::fs::read_dir(&original).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), fork.join(entry.file_name())).unwrap();
    }
    let mut a = Core::open(&original).await.unwrap();
    let mut b = Core::open(&fork).await.unwrap();
    a.append(b"accepted").await.unwrap();
    b.append(b"different").await.unwrap();
    let mut reader = Core::create_replica(&tmp.path().join("reader"), key)
        .await
        .unwrap();
    reader
        .import_bundle(&a.export_bundle(&[0]).await.unwrap())
        .await
        .unwrap();
    assert!(
        reader
            .import_bundle(&b.export_bundle(&[0]).await.unwrap())
            .await
            .is_err()
    );
    assert_eq!(reader.get(0).await.unwrap().unwrap(), b"accepted");
    b.append(b"longer conflicting history").await.unwrap();
    assert!(
        reader
            .import_bundle(&b.export_bundle(&[0, 1]).await.unwrap())
            .await
            .is_err()
    );
    assert_eq!(reader.info().length, 1);
}

#[tokio::test]
async fn non_power_of_two_extension_preserves_prefix() {
    let tmp = TempDir::new().unwrap();
    let mut writer = Core::create(&tmp.path().join("w")).await.unwrap();
    writer
        .append_batch(&[vec![0], vec![1], vec![2]])
        .await
        .unwrap();
    let mut reader = Core::create_replica(&tmp.path().join("r"), writer.public_key())
        .await
        .unwrap();
    reader
        .import_bundle(&writer.export_bundle(&[2]).await.unwrap())
        .await
        .unwrap();
    writer
        .append_batch(&[vec![3], vec![4], vec![5], vec![6]])
        .await
        .unwrap();
    reader
        .import_bundle(&writer.export_bundle(&[0, 1, 2, 3, 4, 5, 6]).await.unwrap())
        .await
        .unwrap();
    assert_eq!(reader.audit().await.unwrap().verified_blocks, 7);
}

#[tokio::test]
async fn malformed_structure_never_advances_reader() {
    let tmp = TempDir::new().unwrap();
    let mut writer = Core::create(&tmp.path().join("w")).await.unwrap();
    writer
        .append_batch(&[vec![1], vec![2], vec![3]])
        .await
        .unwrap();
    let good = writer.export_bundle(&[0, 1, 2]).await.unwrap();
    let mut reader = Core::create_replica(&tmp.path().join("r"), writer.public_key())
        .await
        .unwrap();
    let mut cases = Vec::new();
    let mut b = good.clone();
    b.length = u64::MAX;
    cases.push(b);
    let mut b = good.clone();
    b.head.as_mut().unwrap().fork = 1;
    cases.push(b);
    let mut b = good.clone();
    b.blocks[0].block.as_mut().unwrap().index = u64::MAX;
    cases.push(b);
    let mut b = good.clone();
    b.head.as_mut().unwrap().upgrade.nodes[0].hash = "ff".repeat(32);
    cases.push(b);
    let mut b = good.clone();
    b.head.as_mut().unwrap().upgrade.nodes.clear();
    cases.push(b);
    let mut b = good.clone();
    b.blocks[0].block.as_mut().unwrap().value = "zz".into();
    cases.push(b);
    let mut b = good.clone();
    b.head.as_mut().unwrap().upgrade.signature = "xx".repeat(64);
    cases.push(b);
    let mut b = good.clone();
    b.blocks[0].upgrade.length = 0;
    cases.push(b);
    let mut b = good.clone();
    b.blocks[0].upgrade.nodes[0].hash = "ff".repeat(32);
    cases.push(b);
    let mut b = good.clone();
    b.head.as_mut().unwrap().upgrade.nodes[0].index = 0;
    cases.push(b);
    let mut b = good.clone();
    b.blocks[0].block.as_mut().unwrap().nodes[0].index = 3;
    cases.push(b);
    let mut b = good.clone();
    b.blocks[0].block.as_mut().unwrap().nodes[0].length += 1;
    cases.push(b);
    let mut b = good.clone();
    b.head.as_mut().unwrap().upgrade.nodes.reverse();
    cases.push(b);
    let mut b = good.clone();
    b.blocks[0].block.as_mut().unwrap().nodes.clear();
    cases.push(b);
    let mut b = good.clone();
    b.blocks[0].upgrade.nodes.push(shadw_core::ProofNode {
        index: 5,
        hash: "01".repeat(32),
        length: 1,
    });
    cases.push(b);
    for bad in cases {
        assert!(reader.import_bundle(&bad).await.is_err());
        assert_eq!(reader.info().length, 0);
    }
    reader.import_bundle(&good).await.unwrap();
    assert_eq!(reader.audit().await.unwrap().verified_blocks, 3);
}

#[tokio::test]
async fn append_after_reopen_preserves_all_prior_history() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("w");
    let mut writer = Core::create(&path).await.unwrap();
    for index in 0..3u8 {
        writer.append(&[index]).await.unwrap();
    }
    drop(writer);
    let mut writer = Core::open(&path).await.unwrap();
    for index in 3..7u8 {
        assert_eq!(writer.append(&[index]).await.unwrap(), index as u64);
    }
    drop(writer);
    let mut writer = Core::open(&path).await.unwrap();
    assert_eq!(writer.audit().await.unwrap().verified_blocks, 7);
    for index in 0..7u8 {
        assert_eq!(writer.get(index as u64).await.unwrap(), Some(vec![index]));
    }
}

#[tokio::test]
async fn block_and_export_budgets_fail_before_unbounded_accumulation() {
    let tmp = TempDir::new().unwrap();
    let mut writer = Core::create(&tmp.path().join("w")).await.unwrap();
    assert!(
        writer
            .append(&vec![0; shadw_core::MAX_BLOCK_BYTES + 1])
            .await
            .is_err()
    );
    assert_eq!(writer.info().length, 0);
    let block = vec![42; shadw_core::MAX_BLOCK_BYTES];
    for _ in 0..9 {
        writer.append(&block).await.unwrap();
    }
    assert!(
        writer
            .export_bundle(&(0..9).collect::<Vec<_>>())
            .await
            .is_err()
    );
    assert_eq!(writer.info().length, 9);
    assert_eq!(writer.get(8).await.unwrap(), Some(block));
}
