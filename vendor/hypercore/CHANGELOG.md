# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

### Added

### Changed

### Removed



## [0.16.0] - 2026-02-18

### Added

* Added some new clippy lints to enforce.

### Changed

* `get_random_access` was renamed to `get_random_access_mut`.
* Bumped to 2024 edition.
* Structs that were shared with `hypercore-protocol` have been moved into a separate crate: `hypercore_schema`.
* Dependencies update
* Some function signatures were changed that were pass-by-value or pass-by-ref that didn't need to be.

### Removed



## [0.15.0] - 2025-12-25

### Added

* More impl's of `CompactEncoding`.

### Changed

* New version of compact-encoding used.

### Removed

* `HypercoreState` struct. It is obsolete with new compact-encoding version.



## [0.14.0] - 2024-10-25

### Commits
- [[`5a1f98f8c7`](https://github.com/datrs/hypercore/commit/5a1f98f8c744a3635e34c95421d67809e154b71d)] fix: error message variable order (Timo Tiuraniemi)
- [[`4f2f5fe3ec`](https://github.com/datrs/hypercore/commit/4f2f5fe3ec3d3e0e2dd012780bbc8948354d76cc)] Use tokio by default (Blake Griffith)
- [[`7f70249ffc`](https://github.com/datrs/hypercore/commit/7f70249ffcd4695c581efdaaa046ce702cdd5512)] Fix tests #144 (Blake Griffith)
- [[`c146362617`](https://github.com/datrs/hypercore/commit/c1463626178683e078dd0cc2af4f5952549f1222)] Add events needed for replication #142 (Blake Griffith)

### Stats

```diff
.github/workflows/ci.yml          |  42 ++++++++++++++++++++++++------------------
 Cargo.toml                        |   6 +++++-
 benches/memory.rs                 |   4 ++--
 src/common/node.rs                |   7 ++++++-
 src/common/peer.rs                |  11 ++++++-----
 src/core.rs                       |  62 ++++++++++++++++++++++++++++++++++++++++++++++++++++++--------
 src/lib.rs                        |   4 +++-
 src/replication/events.rs         | 177 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 src/replication/mod.rs            |  93 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 src/replication/shared_core.rs    | 224 ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 src/storage/mod.rs                |   8 ++------
 src/tree/merkle_tree_changeset.rs |   4 ++--
 tests/js/package.json             |   2 +-
 13 files changed, 599 insertions(+), 45 deletions(-)
```


## 2024-07-04, Version v0.13.0
### Commits
- [[`16acbffc92`](https://github.com/datrs/hypercore/commit/16acbffc9297c645c0740f33eb6fbee06681871e)] Remove generic parameters from Hypercore and Storage #139  (cowlicks)

### Stats
```diff
 benches/disk.rs         |  5 ++---
 benches/memory.rs       | 24 ++++++++++++++++--------
 examples/replication.rs |  6 ++----
 src/builder.rs          | 17 +++++------------
 src/core.rs             | 24 +++++++-----------------
 src/lib.rs              |  2 +-
 src/storage/mod.rs      | 62 +++++++++++++++++++++++++++++++++++---------------------------
 tests/common/mod.rs     |  5 ++---
 8 files changed, 70 insertions(+), 75 deletions(-)
```


## 2023-10-28, Version v0.12.1
### Commits
- [[`60d50a5e76`](https://github.com/datrs/hypercore/commit/60d50a5e7638c60047c722b6cfb7c50e29ecd502)] Fix Oplog decoding failing on bitfied update (Timo Tiuraniemi)

### Stats
```diff
 src/oplog/entry.rs | 2 +-
 1 file changed, 1 insertion(+), 1 deletion(-)
```


## 2023-10-12, Version v0.12.0
### Commits
- [[`fa7d487758`](https://github.com/datrs/hypercore/commit/fa7d4877582023e310a7129b11ebd55eb877a75f)] Merge pull request #138 from datrs/v10 (Timo Tiuraniemi)

### Stats
```diff
 .github/workflows/ci.yml          |  142 ++++
 .gitignore                        |    2 +
 CHANGELOG.md                      |   31 +
 Cargo.toml                        |   83 +-
 README.md                         |   81 +-
 benches/bench.rs                  |   58 --
 benches/disk.rs                   |  140 ++++
 benches/memory.rs                 |  128 +++
 examples/async.rs                 |   29 -
 examples/disk.rs                  |   88 ++
 examples/iter.rs                  |   80 --
 examples/main.rs                  |   29 -
 examples/memory.rs                |   59 ++
 examples/replication.rs           |  116 +++
 src/audit.rs                      |   20 -
 src/bitfield/dynamic.rs           |  403 +++++++++
 src/bitfield/fixed.rs             |  228 ++++++
 src/bitfield/iterator.rs          |  158 ----
 src/bitfield/masks.rs             |  108 ---
 src/bitfield/mod.rs               |  379 +--------
 src/builder.rs                    |  100 +++
 src/common/cache.rs               |   58 ++
 src/common/error.rs               |   78 ++
 src/common/mod.rs                 |   23 +
 src/{storage => common}/node.rs   |   77 +-
 src/common/peer.rs                |  117 +++
 src/common/store.rs               |  155 ++++
 src/core.rs                       | 1136 ++++++++++++++++++++++++++
 src/crypto/hash.rs                |  227 +++++-
 src/crypto/key_pair.rs            |   56 +-
 src/crypto/manifest.rs            |   43 +
 src/crypto/merkle.rs              |   74 --
 src/crypto/mod.rs                 |   10 +-
 src/crypto/root.rs                |   52 --
 src/data/mod.rs                   |   46 ++
 src/encoding.rs                   |  370 +++++++++
 src/event.rs                      |    3 -
 src/feed.rs                       |  676 ----------------
 src/feed_builder.rs               |   89 --
 src/lib.rs                        |  112 ++-
 src/oplog/entry.rs                |  164 ++++
 src/oplog/header.rs               |  325 ++++++++
 src/oplog/mod.rs                  |  495 ++++++++++++
 src/prelude.rs                    |   16 +-
 src/proof.rs                      |   30 -
 src/replicate/message.rs          |    6 -
 src/replicate/mod.rs              |    5 -
 src/replicate/peer.rs             |   40 -
 src/storage/mod.rs                |  578 +++++--------
 src/storage/persist.rs            |   19 -
 src/tree/merkle_tree.rs           | 1616 +++++++++++++++++++++++++++++++++++++
 src/tree/merkle_tree_changeset.rs |  131 +++
 src/tree/mod.rs                   |    5 +
 tests/bitfield.rs                 |  195 -----
 tests/common/mod.rs               |  108 ++-
 tests/compat.rs                   |  178 ----
 tests/core.rs                     |   79 ++
 tests/feed.rs                     |  340 --------
 tests/js/interop.js               |  128 +++
 tests/js/mod.rs                   |   50 ++
 tests/js/package.json             |   10 +
 tests/js_interop.rs               |  192 +++++
 tests/model.rs                    |  175 ++--
 tests/regression.rs               |   18 -
 tests/storage.rs                  |   51 --
 65 files changed, 7558 insertions(+), 3260 deletions(-)
```


## 2020-07-19, Version v0.11.1-beta.10
### Commits
- [[`084f00dd3c`](https://github.com/datrs/hypercore/commit/084f00dd3cd9d201315e43eef44352317f9f9b8b)] (cargo-release) version 0.11.1-beta.10 (Bruno Tavares)
- [[`99eff3db3c`](https://github.com/datrs/hypercore/commit/99eff3db3c0f70aeda8e31594c9e2c401743e4b9)] Fix travis errors - clippy warnings and fmt (Bruno Tavares)
- [[`d6f2c5522f`](https://github.com/datrs/hypercore/commit/d6f2c5522f62dbc1f4df303bbaa199f621e3ab70)] Merge pull request #121 from khodzha/append_fix (Bruno Tavares)
- [[`57bd16444e`](https://github.com/datrs/hypercore/commit/57bd16444e3c4e5576e51ac7787851a145d371e9)] Avoid calling unwrap or expect inside fn that returns Result (Bruno Tavares)
- [[`de9ebae3ce`](https://github.com/datrs/hypercore/commit/de9ebae3ce4b0a1f0e76ee17b710c70475f5c33f)] Pin ed25519-dalek to a version with compatible signature methods (Bruno Tavares)
- [[`f7676d530a`](https://github.com/datrs/hypercore/commit/f7676d530a3f6d4ef18f3c92989cccac1c40c131)] Fix clippy errors (Bruno Tavares)
- [[`cf251468e9`](https://github.com/datrs/hypercore/commit/cf251468e9194500cb3b900cc0bb3c9b4a8bfa84)] fixed saving feed to disk (Shamir Khodzha)
- [[`2c260b1b51`](https://github.com/datrs/hypercore/commit/2c260b1b51a5e2ea48bf806fefbfc3705e7dcef1)] Update changelog (Bruno Tavares)

### Stats
```diff
 .gitignore           |  1 +-
 CHANGELOG.md         | 24 +++++++++++++-
 Cargo.toml           |  4 +-
 benches/bench.rs     |  7 ++--
 examples/main.rs     | 23 +++++++++++--
 src/bitfield/mod.rs  | 97 +++++++++++++++++++++++++++++++++++++++++++++--------
 src/crypto/merkle.rs | 11 ++++++-
 src/feed.rs          | 16 +++++++--
 src/feed_builder.rs  | 42 +++++++++++++++++++----
 src/storage/mod.rs   | 93 ++++++++++++++++++++++++++++++++++++---------------
 tests/bitfield.rs    | 18 ++++------
 tests/common/mod.rs  |  2 +-
 tests/compat.rs      | 12 ++++---
 tests/feed.rs        | 24 +++++++++----
 14 files changed, 295 insertions(+), 79 deletions(-)
```


## 2020-07-09, Version v0.11.1-beta.9
### Commits
- [[`8589bd17a6`](https://github.com/datrs/hypercore/commit/8589bd17a6ed323a3c48844a6ef13d40937899df)] (cargo-release) version 0.11.1-beta.9 (Bruno Tavares)
- [[`2765a010ea`](https://github.com/datrs/hypercore/commit/2765a010ea176190be4aa36c265de1d2f8cb78c0)] Merge pull request #120 from khodzha/path_check (Bruno Tavares)
- [[`8ee485bf62`](https://github.com/datrs/hypercore/commit/8ee485bf62da4ae6d6a57a8a691db448fa87a3b1)] added path is a dir check in Feed::open (Shamir Khodzha)
- [[`62a411ee66`](https://github.com/datrs/hypercore/commit/62a411ee660701927884c5276032fc94dc7bc952)] Merge branch 'dependabot/cargo/bitfield-rle-0.2.0' (Bruno Tavares)
- [[`bac9ba4905`](https://github.com/datrs/hypercore/commit/bac9ba4905b339c3f79408b2f7ac6fe4bfeb8ad8)] Fix cargofmt (Bruno Tavares)
- [[`2a6563b46f`](https://github.com/datrs/hypercore/commit/2a6563b46f7e67efcd3551403ed300e10d822891)] Update bitfield-rle requirement from 0.1.1 to 0.2.0 (dependabot-preview[bot])
- [[`37d2a9cf24`](https://github.com/datrs/hypercore/commit/37d2a9cf24502988ec3ad2108b9ae37c5c1f82f2)] Merge branch 'fix-mask-note' (Bruno Tavares)
- [[`e53afb8d92`](https://github.com/datrs/hypercore/commit/e53afb8d92da4a8f55f54c3ed6f987a3b4bde1bf)] Merge branch 'master' into fix-mask-note (Bruno Tavares)
- [[`999ff75213`](https://github.com/datrs/hypercore/commit/999ff75213cdf4246c096bfb3c7bb6fefc666860)] Merge branch 'FreddieRidell-document-src-feed-rs' (Bruno Tavares)
- [[`6be4441404`](https://github.com/datrs/hypercore/commit/6be44414046a5cb801f2985d381e932c9c06075b)] Merge branch 'document-src-feed-rs' of git://github.com/FreddieRidell/hypercore into FreddieRidell-document-src-feed-rs (Bruno Tavares)

### Stats
```diff
 Cargo.toml            |  4 +--
 src/bitfield/masks.rs |  2 +-
 src/crypto/mod.rs     |  4 ++-
 src/feed.rs           | 73 +++++++++++++++++++++++++++++++++++++++++-----------
 tests/feed.rs         | 30 +++++++++++++++++++++-
 5 files changed, 94 insertions(+), 19 deletions(-)
```


## 2020-03-03, Version 0.11.1-beta.3
### Commits
- [[`b555606bd6`](https://github.com/datrs/hypercore/commit/b555606bd626ae39f338bd6aef4f8976ff0c055e)] (cargo-release) version 0.11.1-beta.3 (Bruno Tavares)
- [[`aaf265b8b8`](https://github.com/datrs/hypercore/commit/aaf265b8b84ee5ba6b975a5503db262e154c14eb)] Fix requirements on ram crates to compile (Bruno Tavares)
- [[`10448df561`](https://github.com/datrs/hypercore/commit/10448df56163c1f2917d4508f57713d635fa2d24)] Update changelog (Bruno Tavares)

### Stats
```diff
 CHANGELOG.md | 24 ++++++++++++++++++++++++
 Cargo.toml   |  6 +++---
 2 files changed, 27 insertions(+), 3 deletions(-)
```


## 2020-03-03, Version 0.11.1-beta.2
### Commits
- [[`3dfd5c8c71`](https://github.com/datrs/hypercore/commit/3dfd5c8c716a439131cf7b9a2b360ef737969335)] (cargo-release) version 0.11.1-beta.2 (Bruno Tavares)
- [[`4136866e01`](https://github.com/datrs/hypercore/commit/4136866e01259825944cff099e59ffa4c8df081c)] Merge pull request #96 from bltavares/bitfield-compress (Bruno Tavares)
- [[`d8beadbbfb`](https://github.com/datrs/hypercore/commit/d8beadbbfb0ff7d2d79e52abc14ffb570570b101)] GH Feedback: add comments on the optional fields (Bruno Tavares)
- [[`9c6812d901`](https://github.com/datrs/hypercore/commit/9c6812d901454a383bee9802e0f5828c3224b515)] Use literals for floats (Bruno Tavares)
- [[`356c90e915`](https://github.com/datrs/hypercore/commit/356c90e915a9a5dcc4edb5bf0fa61eda200f6b9b)] Make test with bigger ranges than page size (Bruno Tavares)
- [[`390e13f9b5`](https://github.com/datrs/hypercore/commit/390e13f9b527845f281b24071bbf579f9a6232eb)] WIP: JS has float numbers on math (Bruno Tavares)
- [[`bd333ba68d`](https://github.com/datrs/hypercore/commit/bd333ba68dc50f6e8bc581d39169ae64f6cba9de)] Compress bitfield and expose it to network code (Bruno Tavares)
- [[`0bdbf6207a`](https://github.com/datrs/hypercore/commit/0bdbf6207af26ca3e3516956db7fa3140679e56e)] Bump dalek and rand (Bruno Tavares)
- [[`ac0f3b6a74`](https://github.com/datrs/hypercore/commit/ac0f3b6a743cae1a8c1b51cabfd5a542ef34361b)] Update changelog (Bruno Tavares)

### Stats
```diff
 CHANGELOG.md        | 40 ++++++++++++++++++++++++++++++++++++++++
 Cargo.toml          |  3 ++-
 src/bitfield/mod.rs | 32 ++++++++++++++++++++++++++++++++
 src/feed.rs         |  5 +++++
 tests/bitfield.rs   | 22 ++++++++++++++++++++++
 tests/model.rs      |  7 +------
 6 files changed, 102 insertions(+), 7 deletions(-)
```


## 2020-03-03, Version 0.11.1-beta.1
### Commits
- [[`e5f071766c`](https://github.com/datrs/hypercore/commit/e5f071766c8b32c875df4872abe89ebb43700f31)] (cargo-release) version 0.11.1-beta.1 (Bruno Tavares)
- [[`f7af79a3c2`](https://github.com/datrs/hypercore/commit/f7af79a3c271b426d0d6638872b0420a341d025e)] Merge pull request #100 from bltavares/bumps (Bruno Tavares)
- [[`51c35d8f42`](https://github.com/datrs/hypercore/commit/51c35d8f42c42e111f2c207f1901288aaee7e500)] Point deps to crates versions (Bruno Tavares)
- [[`f3b421c6ca`](https://github.com/datrs/hypercore/commit/f3b421c6ca76a0b5c5acb267988d97ba97e8a77a)] Fix clippy: rename func to adhere to conventions (Bruno Tavares)
- [[`ba09c27336`](https://github.com/datrs/hypercore/commit/ba09c2733684f0320a7f99ebfa3ec8aae31334fd)] Fix travis: include checks on benchmarks (Bruno Tavares)
- [[`173bc3fda2`](https://github.com/datrs/hypercore/commit/173bc3fda2f079994a38577030142b97c3143b4f)] Move from usize to u64 (Bruno Tavares)
- [[`0678d06687`](https://github.com/datrs/hypercore/commit/0678d066875b7cef8cde3628f7ef91658a40f8c1)] Fix changes on ed25519_dalek and rand (Bruno Tavares)
- [[`7fd467d928`](https://github.com/datrs/hypercore/commit/7fd467d92800e00cff7600fe6e68fbb474c899be)] Fix Travis config (Bruno Tavares)
- [[`c4dc33a69a`](https://github.com/datrs/hypercore/commit/c4dc33a69aeead974d7dbd35d8414016ea3e421b)] Bump versions to latest versions (Bruno Tavares)
- [[`ac3790dd4d`](https://github.com/datrs/hypercore/commit/ac3790dd4da0c72341944f29a75a8bf1fefcae00)] Bump versions to latest versions (Bruno Tavares)
- [[`a3aa858b61`](https://github.com/datrs/hypercore/commit/a3aa858b61f36b30d02f06976eebbb37d823aa81)] Update sparse-bitfield requirement from 0.10.0 to 0.11.0 (dependabot-preview[bot])
- [[`97cf996831`](https://github.com/datrs/hypercore/commit/97cf996831d00626a6ea75cc5267d5974bbca573)] Update changelog (Bruno Tavares)

### Stats
```diff
 .travis.yml              |   8 ++--
 CHANGELOG.md             |  28 +++++++++++++-
 Cargo.toml               |  34 ++++++++--------
 examples/iter.rs         |   6 +--
 src/audit.rs             |   8 ++--
 src/bitfield/iterator.rs |  37 +++++++++---------
 src/bitfield/mod.rs      | 100 ++++++++++++++++++++++++------------------------
 src/crypto/hash.rs       |  12 +++---
 src/crypto/key_pair.rs   |  13 +++---
 src/crypto/root.rs       |  62 +++++++++++++++---------------
 src/feed.rs              |  48 +++++++++++------------
 src/proof.rs             |   4 +-
 src/replicate/message.rs |   4 +-
 src/replicate/peer.rs    |   4 +-
 src/storage/mod.rs       |  42 ++++++++++----------
 src/storage/node.rs      |  16 ++++----
 src/storage/persist.rs   |   4 +-
 tests/bitfield.rs        |   8 ++--
 tests/model.rs           |  12 +++---
 19 files changed, 243 insertions(+), 207 deletions(-)
```


## 2020-02-19, Version 0.11.0
### Commits
- [[`f2baf805d5`](https://github.com/datrs/hypercore/commit/f2baf805d5477c768f32ca2cf7faae4d9d284686)] (cargo-release) version 0.11.0 (Bruno Tavares)
- [[`31dfdd15f2`](https://github.com/datrs/hypercore/commit/31dfdd15f27356780d75fa126bd8a8d464fefc39)] Merge pull request #95 from bltavares/send (Bruno Tavares)
- [[`46be5197a2`](https://github.com/datrs/hypercore/commit/46be5197a2398e04d413ebfa65fcb6f830dedf0f)] Use published version (Bruno Tavares)
- [[`d4905b11cf`](https://github.com/datrs/hypercore/commit/d4905b11cf83871db98c118c373d52626e6b1c78)] Point to merkle-tree-stream that is Send while new version is to be released (Bruno Tavares)
- [[`40caf92ec2`](https://github.com/datrs/hypercore/commit/40caf92ec2c357a08ddeec03f9d4ba34a723eeaf)] Replace all Rc with Arc in code. Needs to update dependencies (Bruno Tavares)
- [[`2dc8008a55`](https://github.com/datrs/hypercore/commit/2dc8008a5542713a2569cfb115a006dee34bbca6)] example to ensure structs are send (Bruno Tavares)
- [[`f77fe7b025`](https://github.com/datrs/hypercore/commit/f77fe7b0257bd5f0e7007c012bc68bc1d75eda05)] fix readme link (#88) (nasa)
- [[`82e48f0c7d`](https://github.com/datrs/hypercore/commit/82e48f0c7d2330f0ed845dac30db46a02d5f7c48)] Update memory-pager requirement from 0.8.0 to 0.9.0 (dependabot-preview[bot])
- [[`580dff64c5`](https://github.com/datrs/hypercore/commit/580dff64c50377e6fc51dbed701c2dc26a2693a2)] Update sparse-bitfield requirement from 0.8.1 to 0.10.0 (dependabot-preview[bot])
- [[`7eda3504d6`](https://github.com/datrs/hypercore/commit/7eda3504d61de0f1423d0efa272587fe8b0a1650)] Merge pull request #81 from bltavares/discovery-key-hash (Szabolcs Berecz)
- [[`1edf42f790`](https://github.com/datrs/hypercore/commit/1edf42f79007924b79e7b1b99a7e9d66abc3b4e9)] Implements discoveryKey from hypercore-crypto (Bruno Tavares)
- [[`aedef0b149`](https://github.com/datrs/hypercore/commit/aedef0b149de042313245c2baab0948da3390aef)] Update changelog (Yoshua Wuyts)

### Stats
```diff
 CHANGELOG.md         | 26 ++++++++++++++++++++++++++
 Cargo.toml           |  9 +++++----
 README.md            |  2 +-
 examples/async.rs    | 30 ++++++++++++++++++++++++++++++
 src/crypto/hash.rs   | 42 +++++++++++++++++++++++++++++-------------
 src/crypto/merkle.rs | 10 +++++-----
 src/feed.rs          |  4 ++--
 7 files changed, 98 insertions(+), 25 deletions(-)
```


## 2018-12-22, Version 0.9.0
### Commits
- [[`9c2b07fca6`](https://github.com/datrs/hypercore/commit/9c2b07fca68bb34046551f0fd152aa7f97a33fb6)] (cargo-release) version 0.9.0 (Yoshua Wuyts)
- [[`86e241f9e0`](https://github.com/datrs/hypercore/commit/86e241f9e02e3583445fcb43fcc28295eae1cd31)] 🙋 Implement feed auditing (#55) (Tim Deeb-Swihart)
- [[`5840a3a6a9`](https://github.com/datrs/hypercore/commit/5840a3a6a90f47ba89662687a374f070f3172c69)] Update rand requirement from 0.5.5 to 0.6.0 (#49) (dependabot[bot])
- [[`1628057868`](https://github.com/datrs/hypercore/commit/162805786831866ea611cfe97e85def690614fa6)] use tree_index functions (#48) (Yoshua Wuyts)
- [[`f66fbb3543`](https://github.com/datrs/hypercore/commit/f66fbb354376681062697ffd2be18da2224cb1b9)] Update merkle-tree-stream requirement from 0.7.0 to 0.8.0 (#46) (dependabot[bot])
- [[`343df6f991`](https://github.com/datrs/hypercore/commit/343df6f991b0fbe5f50a7d95b632b3c60e5dfa54)] Update changelog (Yoshua Wuyts)

### Stats
```diff
 CHANGELOG.md           | 26 +++++++++++++++++++-
 Cargo.toml             |  6 ++--
 src/audit.rs           | 20 +++++++++++++++-
 src/bitfield/mod.rs    | 21 ++++++++++------
 src/crypto/key_pair.rs |  2 +-
 src/crypto/merkle.rs   | 14 ++++++----
 src/feed.rs            | 46 ++++++++++++++++++++++++++++------
 src/lib.rs             |  1 +-
 src/storage/mod.rs     | 10 +++++--
 src/storage/node.rs    |  2 +-
 tests/feed.rs          | 69 +++++++++++++++++++++++++++++++++++++++++++++++++++-
 11 files changed, 191 insertions(+), 26 deletions(-)
```


## 2018-12-22, Version 0.9.0
### Commits
- [[`9c2b07fca6`](https://github.com/datrs/hypercore/commit/9c2b07fca68bb34046551f0fd152aa7f97a33fb6)] (cargo-release) version 0.9.0 (Yoshua Wuyts)
- [[`86e241f9e0`](https://github.com/datrs/hypercore/commit/86e241f9e02e3583445fcb43fcc28295eae1cd31)] 🙋 Implement feed auditing (#55) (Tim Deeb-Swihart)
- [[`5840a3a6a9`](https://github.com/datrs/hypercore/commit/5840a3a6a90f47ba89662687a374f070f3172c69)] Update rand requirement from 0.5.5 to 0.6.0 (#49) (dependabot[bot])
- [[`1628057868`](https://github.com/datrs/hypercore/commit/162805786831866ea611cfe97e85def690614fa6)] use tree_index functions (#48) (Yoshua Wuyts)
- [[`f66fbb3543`](https://github.com/datrs/hypercore/commit/f66fbb354376681062697ffd2be18da2224cb1b9)] Update merkle-tree-stream requirement from 0.7.0 to 0.8.0 (#46) (dependabot[bot])
- [[`343df6f991`](https://github.com/datrs/hypercore/commit/343df6f991b0fbe5f50a7d95b632b3c60e5dfa54)] Update changelog (Yoshua Wuyts)

### Stats
```diff
 CHANGELOG.md           | 26 +++++++++++++++++++-
 Cargo.toml             |  6 ++--
 src/audit.rs           | 20 +++++++++++++++-
 src/bitfield/mod.rs    | 21 ++++++++++------
 src/crypto/key_pair.rs |  2 +-
 src/crypto/merkle.rs   | 14 ++++++----
 src/feed.rs            | 46 ++++++++++++++++++++++++++++------
 src/lib.rs             |  1 +-
 src/storage/mod.rs     | 10 +++++--
 src/storage/node.rs    |  2 +-
 tests/feed.rs          | 69 +++++++++++++++++++++++++++++++++++++++++++++++++++-
 11 files changed, 191 insertions(+), 26 deletions(-)
```


## 2018-10-28, Version 0.8.1
### Commits
- [[`938d2816cc`](https://github.com/datrs/hypercore/commit/938d2816cc63e4dd8964139baa56be2dd28e72d5)] (cargo-release) version 0.8.1 (Yoshua Wuyts)
- [[`79fd7a8141`](https://github.com/datrs/hypercore/commit/79fd7a8141096606b4124c7d59dede2a4021b3fb)] Stricter lints (#45) (Yoshua Wuyts)
- [[`96b3af825d`](https://github.com/datrs/hypercore/commit/96b3af825ddc5c69364fe92c71d8498f4a00a2dc)] use spec compatible constants (#44) (Yoshua Wuyts)
- [[`ac8ef53b0c`](https://github.com/datrs/hypercore/commit/ac8ef53b0cd45f0b935ab83dde4f750eb91a07e8)] Update changelog (Yoshua Wuyts)

### Stats
```diff
 CHANGELOG.md           | 33 +++++++++++++++++++++++++++++++++
 Cargo.toml             | 10 +++++++++-
 src/crypto/hash.rs     |  8 ++++----
 src/crypto/key_pair.rs |  2 +-
 src/crypto/merkle.rs   |  4 ++--
 src/feed.rs            | 20 ++++++++++----------
 src/feed_builder.rs    | 10 +++++-----
 src/lib.rs             | 19 +++++++++++--------
 src/prelude.rs         |  4 ++--
 src/proof.rs           |  4 ++--
 src/storage/mod.rs     |  2 +-
 src/storage/node.rs    |  4 ++--
 src/storage/persist.rs |  2 +-
 13 files changed, 83 insertions(+), 39 deletions(-)
```


## 2018-10-18, Version 0.8.0
### Commits
- [[`048921b077`](https://github.com/datrs/hypercore/commit/048921b077d02963e70a881fa780e6e96c347d50)] (cargo-release) version 0.8.0 (Yoshua Wuyts)
- [[`54ceb55e7b`](https://github.com/datrs/hypercore/commit/54ceb55e7bf6c5c037b3849c53bc082bc57e0ee4)] travis master only builds (Yoshua Wuyts)
- [[`1a06b5862d`](https://github.com/datrs/hypercore/commit/1a06b5862d371120dc2e1695e5d1764721707e29)] upgrade (#43) (Yoshua Wuyts)
- [[`2fda376767`](https://github.com/datrs/hypercore/commit/2fda376767efe3b61fe2f3bc46a431340cf984a2)] tests/helpers -> tests/common (#38) (Yoshua Wuyts)
- [[`d48e5570fa`](https://github.com/datrs/hypercore/commit/d48e5570fa659b38519a54288b6019205cb48276)] Keep up with modern times in clippy invocation (#35) (Szabolcs Berecz)
- [[`a62a21b249`](https://github.com/datrs/hypercore/commit/a62a21b24953f6b1da5cfc902abef6914f0b7950)] Update quickcheck requirement from 0.6.2 to 0.7.1 (#33) (Szabolcs Berecz)
- [[`3bbe87db8d`](https://github.com/datrs/hypercore/commit/3bbe87db8d448e8fbc7a73a99b07ff39ec09c1e9)] Update changelog (Yoshua Wuyts)

### Stats
```diff
 .github/ISSUE_TEMPLATE.md                 | 40 +++---------------------------
 .github/ISSUE_TEMPLATE/bug_report.md      | 23 +++++++++++++++++-
 .github/ISSUE_TEMPLATE/feature_request.md | 43 ++++++++++++++++++++++++++++++++-
 .github/ISSUE_TEMPLATE/question.md        | 18 +++++++++++++-
 .travis.yml                               | 24 +++++++++---------
 CHANGELOG.md                              | 25 +++++++++++++++++++-
 Cargo.toml                                | 28 ++++++++++-----------
 README.md                                 | 23 +++++++++++++++--
 src/feed.rs                               |  2 +-
 src/lib.rs                                | 23 +++++++++++++----
 src/replicate/peer.rs                     |  2 +-
 src/storage/mod.rs                        |  2 +-
 tests/common/mod.rs                       | 15 +++++++++++-
 tests/feed.rs                             | 29 +++++++++++++++++++---
 tests/helpers.rs                          | 34 +-------------------------
 tests/model.rs                            |  6 ++--
 tests/regression.rs                       |  4 +--
 17 files changed, 232 insertions(+), 109 deletions(-)
```


## 2018-09-03, Version 0.7.1
### Commits
- [[`43ad5d3c9a`](https://github.com/datrs/hypercore/commit/43ad5d3c9accd9e4faa63fc5fe35b5c74997d503)] (cargo-release) version 0.7.1 (Yoshua Wuyts)
- [[`cb2cfac275`](https://github.com/datrs/hypercore/commit/cb2cfac2757a50600886251b608ab349bdc6daf4)] Update ed25519_dalek to 0.8 and rand to 0.5 (#30) (Luiz Irber)
- [[`ade97ddfe3`](https://github.com/datrs/hypercore/commit/ade97ddfe3310edbff11057740ebd03ed73075b4)] Update memory-pager requirement from 0.7.0 to 0.8.0 (dependabot[bot])
- [[`420a3b19b0`](https://github.com/datrs/hypercore/commit/420a3b19b0daa7d32d96c3c67045adab10c0f38d)] Upgrade random-access-storage (#26) (Szabolcs Berecz)
- [[`7421f677eb`](https://github.com/datrs/hypercore/commit/7421f677eb200cfa2cceb98c027408e29cc526ee)] update changelog (Yoshua Wuyts)

### Stats
```diff
 CHANGELOG.md           | 26 ++++++++++++++++++++++++++
 Cargo.toml             | 14 +++++++-------
 benches/bench.rs       |  8 +++-----
 src/crypto/key_pair.rs |  4 ++--
 src/feed.rs            | 16 ++++++++--------
 src/feed_builder.rs    |  6 +++---
 src/storage/mod.rs     | 40 ++++++++++++++++++++--------------------
 src/storage/persist.rs |  4 ++--
 tests/compat.rs        |  6 +++---
 tests/feed.rs          | 10 +++++-----
 tests/helpers.rs       |  8 ++++----
 11 files changed, 83 insertions(+), 59 deletions(-)
```


## 2018-08-25, Version 0.7.0
### Commits
- [[`c4c5986191`](https://github.com/datrs/hypercore/commits/c4c5986191ab9dc07443264c65d0f2edc6971439)] (cargo-release) version 0.7.0 (Yoshua Wuyts)
- [[`7d6bde061c`](https://github.com/datrs/hypercore/commits/7d6bde061c6724a216f59ecd90970722b0c0f118)] Storage: implement keypair read/write (#18)
- [[`d027f37ed8`](https://github.com/datrs/hypercore/commits/d027f37ed8aa5c9a487a7e0260fa1ca0cd089011)] Update sparse-bitfield requirement from 0.4.0 to 0.8.0 (#20)
- [[`5d9b05f029`](https://github.com/datrs/hypercore/commits/5d9b05f029f2e1427770c4169794ce1cccd70ec5)] Update memory-pager requirement from 0.4.5 to 0.7.0
- [[`73a3f28e26`](https://github.com/datrs/hypercore/commits/73a3f28e26957c627254ed024092df7ae057d277)] Update sleep-parser requirement from 0.4.0 to 0.6.0
- [[`566b7a1021`](https://github.com/datrs/hypercore/commits/566b7a1021a36e7dc82ca22091ee21df88870d57)] Upgrade to latest random-access-storage (#17)
- [[`e086e60942`](https://github.com/datrs/hypercore/commits/e086e609428d015bc831384ff3e16a8c9a295bc7)] Add rustfmt back to travis (#19)
- [[`eb5edfba43`](https://github.com/datrs/hypercore/commits/eb5edfba438f8617d076f3a3f95636dfd3cc29ad)] (cargo-release) start next development iteration 0.6.1-alpha.0 (Yoshua Wuyts)

### Stats
```diff
 .travis.yml         |  1 +-
 Cargo.toml          | 14 ++++++------
 src/bitfield/mod.rs |  9 +++-----
 src/feed.rs         | 49 +++++++++++++++++++++++++++++--------------
 src/feed_builder.rs |  3 ++-
 src/lib.rs          |  2 +-
 src/storage/mod.rs  | 62 +++++++++++++++++++++++++++++++++++++++++++++++++-----
 tests/compat.rs     |  7 +++---
 tests/feed.rs       | 32 ++++++++++++++++++++++++++++-
 tests/helpers.rs    |  2 +-
 tests/storage.rs    | 54 +++++++++++++++++++++++++++++++++++++++++++++++-
 11 files changed, 197 insertions(+), 38 deletions(-)
```

<!-- next-url -->
[Unreleased]: https://github.com/datrs/hypercore/compare/v0.16.0...HEAD
[0.16.0]: https://github.com/datrs/hypercore/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/datrs/hypercore/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/datrs/hypercore/compare/v0.14.0...v0.13.0
