# CHANGELOG

## Current

## v9.0.0

- As a convenience include the feed.length in the signature data. The feed.length is already implicitly included in the merkle roots that are hashed together to form the tree hash before it was signed. However this meant that to verify the feed.length using a signature independently of Hypercore required sharing the merkle roots instead of just the treeHash. With this change sharing only the treeHash, feed.length, and signature is needed to verify a version. This is a breaking change, but Hypercore includes backwards compat, ie a v8 Hypercore can be verified by v9 but not vice versa.
- Updates Noise protocol to latest, which introduces a standard DH handshake.
