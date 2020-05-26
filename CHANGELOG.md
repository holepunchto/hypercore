# CHANGELOG

See [UPGRADE.md](UPGRADE.md) for notes on breaking changes for downstream developers.

## Current

## v9.1.0

- Make peer.remoteOpened public ([#268](https://github.com/hypercore-protocol/hypercore/pull/268))

## v9.0.1

- Upgraded standard to v14 with subsequent formatting tweaks
- createReadStream is up to 8x faster now! ([#261](https://github.com/hypercore-protocol/hypercore/pull/261) by [@tinchoz49](https://github.com/tinchoz49))
- Fixed benchmarks ([#266](https://github.com/hypercore-protocol/hypercore/pull/266) by [@fwip](https://github.com/fwip))

## v9.0.0

- Ease of use update to signatures, https://github.com/mafintosh/hypercore/issues/260
- Updates [noise-protocol](https://github.com/emilbayes/noise-protocol) to latest, which uses chacha instead of xchacha and moves avoid from the sodium kx api for better compatability with the rest of the Noise ecosystem.
- Updates [sodium-native](https://github.com/sodium-friends/sodium-native) from 2 to 3 across the board. 3 uses n-api, so no more builds needed when Node is released.
- Update the discovery key to hash in the protocol version for better migration
