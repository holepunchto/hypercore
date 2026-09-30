pub mod common;

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use proptest_derive::Arbitrary;

const MAX_FILE_SIZE: u64 = 50000;

#[derive(Clone, Debug, Arbitrary)]
enum Op {
    Get {
        #[proptest(strategy(index_strategy))]
        index: u64,
    },
    Append {
        #[proptest(regex(data_regex))]
        data: Vec<u8>,
    },
    Clear {
        #[proptest(strategy(divisor_strategy))]
        len_divisor_for_start: u8,
        #[proptest(strategy(divisor_strategy))]
        len_divisor_for_length: u8,
    },
}

fn index_strategy() -> impl Strategy<Value = u64> {
    0..MAX_FILE_SIZE
}

fn divisor_strategy() -> impl Strategy<Value = u8> {
    1_u8..17_u8
}

fn data_regex() -> &'static str {
    // Write 0..5000 byte chunks of ASCII characters as dummy data
    "([ -~]{1,1}\n){0,5000}"
}

proptest! {
  #![proptest_config(ProptestConfig {
    failure_persistence: Some(Box::new(FileFailurePersistence::WithSource("regressions"))),
    ..Default::default()
  })]

  #[test]
  #[cfg(feature = "async-std")]
  fn implementation_matches_model(ops: Vec<Op>) {
    assert!(async_std::task::block_on(assert_implementation_matches_model(ops)));
  }

  #[test]
  #[cfg(feature = "tokio")]
  fn implementation_matches_model(ops: Vec<Op>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    assert!(rt.block_on(async {
      assert_implementation_matches_model(ops).await
    }));
  }
}

async fn assert_implementation_matches_model(ops: Vec<Op>) -> bool {
    use hypercore::{HypercoreBuilder, Storage};

    let storage = Storage::new_memory()
        .await
        .expect("Memory storage creation should be successful");
    let mut hypercore = HypercoreBuilder::new(storage)
        .build()
        .await
        .expect("Hypercore creation should be successful");

    let mut model: Vec<Option<Vec<u8>>> = vec![];

    for op in ops {
        match op {
            Op::Append { data } => {
                hypercore
                    .append(&data)
                    .await
                    .expect("Append should be successful");
                model.push(Some(data));
            }
            Op::Get { index } => {
                let data = hypercore
                    .get(index)
                    .await
                    .expect("Get should be successful");
                if index >= hypercore.info().length {
                    assert_eq!(data, None);
                } else {
                    assert_eq!(data, model[index as usize].clone());
                }
            }
            Op::Clear {
                len_divisor_for_start,
                len_divisor_for_length,
            } => {
                let start = {
                    let result = model.len() as u64 / len_divisor_for_start as u64;
                    if result == model.len() as u64 {
                        if !model.is_empty() { result - 1 } else { 0 }
                    } else {
                        result
                    }
                };
                let length = model.len() as u64 / len_divisor_for_length as u64;
                let end = start + length;
                let model_end = if end < model.len() as u64 {
                    end
                } else {
                    model.len() as u64
                };
                hypercore
                    .clear(start, end)
                    .await
                    .expect("Clear should be successful");
                model[start as usize..model_end as usize].fill(None);
            }
        }
    }
    true
}
