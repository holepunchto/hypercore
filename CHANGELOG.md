# CHANGELOG

See [UPGRADE.md](UPGRADE.md) for notes on breaking changes for downstream developers.

## Current

## v9.5.0

- Feed close makes the replication detach the channel used for this particular stream.

## v9.4.0

- feed.get accepts onwait hook, that is called if get is waiting for a network peer.

## v9.3.0

- feed.get returns an id, that can be used to cancel a pending get with feed.cancel(getId)

## v9.2.0

- Add `maxBlockSize` on write streams to auto chunk big blocks being written.

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
