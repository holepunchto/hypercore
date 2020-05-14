# CHANGELOG

See [UPGRADE.md](UPGRADE.md) for notes on breaking changes for downstream developers.

## Current

## v9.0.0

- Ease of use update to signatures, https://github.com/mafintosh/hypercore/issues/260
- Updates [noise-protocol](https://github.com/emilbayes/noise-protocol) to latest, which uses chacha instead of xchacha and moves avoid from the sodium kx api for better compatability with the rest of the Noise ecosystem.
- Updates [sodium-native](https://github.com/sodium-friends/sodium-native) from 2 to 3 across the board. 3 uses n-api, so no more builds needed when Node is released.
