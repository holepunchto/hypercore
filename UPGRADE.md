# Upgrade Notes

Notes for downstream developers who are upgrading their modules to new, breaking versions of hypercore.

## 9.0.0

- The format of signatures [has been changed](https://github.com/mafintosh/hypercore/issues/260). This is backwards-compatible (v9 can read v8 signatures), but forward-incompatible (v8 cannot read v9 signatures). If a v8 peer replicates with a v9 peer, it will emit a "REMOTE SIGNTURE INVALID" error on the replication stream.
- The encryption ([NOISE](https://github.com/emilbayes/noise-protocol)) handshake has been changed in an backwards- and forwards-incompatible way. v8 peers can not handshake with v9 peers, and vice-versa. A NOISE-related error is emitted on the replication stream.
- There is no way (yet) to detect whether a peer is running an incompatible version of hypercore at the replication level. One workaround for downstream developers is to include their own application-level handshake before piping to the replication stream, to communicate a "app protocol version" (maybe "v8" and "v9") and abort the connection if the peer is running an incompatible version.
