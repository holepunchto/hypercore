# Hypercore

### [See the full API docs at docs.pears.com](https://docs.pears.com/building-blocks/hypercore)

Hypercore is a secure, distributed append-only log.

Built for sharing large datasets and streams of real time data

## Features

- **Sparse replication.** Only download the data you are interested in.
- **Realtime.** Get the latest updates to the log fast and securely.
- **Performant.** Uses a simple flat file structure to maximize I/O performance.
- **Secure.** Uses signed merkle trees to verify log integrity in real time.
- **Modular.** Hypercore aims to do one thing and one thing well - distributing a stream of data.

Note that the latest release is Hypercore 10, which adds support for truncate and many other things.
Version 10 is not compatible with earlier versions (9 and earlier), but is considered LTS, meaning the storage format and wire protocol is forward compatible with future versions.

## Install

```sh
npm install hypercore
```

## API

#### `const core = new Hypercore(storage, [key], [options])`

Make a new Hypercore instance.

`storage` should be set to a directory where you want to store the data and core metadata.

```js
const core = new Hypercore('./directory') // store data in ./directory
```

Alternatively you can pass a [Hypercore Storage](https://github.com/holepunchto/hypercore-storage) or use a [Corestore](https://github.com/holepunchto/corestore) if you want to make many Hypercores efficiently. Note that `random-access-storage` is no longer supported.

`key` can be set to a Hypercore key which is a hash of Hypercore's internal auth manifest, describing how to validate the Hypercore. If you do not set this, it will be loaded from storage. If nothing is previously stored, a new auth manifest will be generated giving you local write access to it.

`options` include:

```js
{
  createIfMissing: true, // create a new Hypercore key pair if none was present in storage
  overwrite: false, // overwrite any old Hypercore that might already exist
  force: false, // Advanced option. Will force overwrite even if the header's key & the passed key don't match
  valueEncoding: 'json' | 'utf-8' | 'binary', // defaults to binary
  encodeBatch: batch => { ... }, // optionally apply an encoding to complete batches
  keyPair: kp, // optionally pass the public key and secret key as a key pair
  encryption: { key: buffer }, // the block encryption key
  onwait: () => {}, // hook that is called if gets are waiting for download
  timeout: 0, // wait at max some milliseconds (0 means no timeout)
  writable: true, // disable appends and truncates
  inflightRange: null, // Advanced option. Set to [minInflight, maxInflight] to change the min and max inflight blocks per peer when downloading.
  ongc: (session) => { ... }, // A callback called when the session is garbage collected
  onseq: (index, core) => { ... }, // A callback called when core.get(index) is called.
  notDownloadingLinger: 20000, // How many milliseconds to wait after downloading finishes keeping the connection open. Defaults to a random number between 20-40s
  allowFork: true, // Enables updating core when it forks
  userData: { foo: 'bar' }, // An object to assign to the local User Data storage
  manifest: undefined, // Advanced option. Set the manifest when creating the hypercore. See Manifest section for more info
  preload: undefined, // Advanced option. A promise that returns constructor options overrides before the core is opened
  storage: undefined, // An alternative to passing storage as a dedicated argument
  key: null, // An alternative to passing key as a dedicated argument
}
```

You can also set `valueEncoding` to any [compact-encoding](https://github.com/compact-encoding) instance.

`valueEncoding`s will be applied to individual blocks, even if you append batches. If you want to control encoding at the batch-level, you can use the `encodeBatch` option, which is a function that takes a batch and returns a binary-encoded batch. If you provide a custom `valueEncoding`, it will not be applied prior to `encodeBatch`.

The user may provide a custom encryption module as `opts.encryption`, which should satisfy the [HypercoreEncryption](https://github.com/holepunchto/hypercore-encryption) interface.

##### User Data

User Data is a key-value store that is persisted locally and is not replicated with the Hypercore. This is useful as a quick store for data only used by the current peer. For example, `autobase` uses User Data to store information such as encryption keys and connections between a peer's local writer and the Autobase's bootstrap core.

Keys are always strings and values can be strings or buffers.

See [`core.setUserData(key, value)`](#await-coresetuserdatakey-value) and [`core.getUserData(key)`](#const-value--await-coregetuserdatakey) for updating User Data.

##### Manifest

The manifest is metadata about authenticating a hypercore including things like the signers (only one by default) and the prologue. Manifest has the following structure:

```
{
  version: 1,                       // Version of the manifest format
  hash: 'blake2b',                  // Only Blake2b is supported currently
  allowPatch: false,                // Whether the hypercore can be "patched" to change the signers
  quorum: (signers.length / 2) + 1, // How many signers needed to verify a block
  signers,                          // Array of signers for the core
  prologue: null,                   // The tree hash and length of the core
  linked: null,                     // Array of associated core keys. Only supported in versions >= 2
  userData: null                    // Arbitrary buffer for User Data integral to the core. Only supported in versions >= 2
}
```

The `linked` property in the manifest is used to reference other hypercores that are associated with the current core. For example in `autobase` the encryption view is loaded from the `linked` property in the system view core. Note, as with everything in the manifest, changing the `linked` property changes the core's `key`.

Signers are an array of objects with the following structure:

```
{
  signature: 'ed25519',               // The signature method
  namespace: caps.DEFAULT_NAMESPACE,  // A cryptographic namespace for the signature
  publicKey: Buffer                   // Signer's public key
}
```

#### `const { length, byteLength } = await core.append(block, options = {})`

Append a block of data (or an array of blocks) to the core.
Returns the new length and byte length of the core.

```js
// simple call append with a new block of data
await core.append(Buffer.from('I am a block of data'))

// pass an array to append multiple blocks as a batch
await core.append([Buffer.from('batch block 1'), Buffer.from('batch block 2')])
```

`options` include:

```
{
  writable: false // Enabled ignores writable check. Does not override whether core is writable.
  maxLength: undefined // The maximum resulting length of the core after appending
  keyPair: core.keyPair // KeyPair used to sign the block(s)
  signature: null // Set signature for block(s)
}
```

#### `const block = await core.get(index, [options])`

Get a block of data.
If the data is not available locally this method will prioritize and wait for the data to be downloaded.

```js
// get block #42
const block = await core.get(42)

// get block #43, but only wait 5s
const blockIfFast = await core.get(43, { timeout: 5000 })

// get block #44, but only if we have it locally
const blockLocal = await core.get(44, { wait: false })
```

`options` include:

```js
{
  wait: true, // wait for block to be downloaded
  onwait: () => {}, // hook that is called if the get is waiting for download
  timeout: 0, // wait at max some milliseconds (0 means no timeout)
  activeRequests: undefined, // Advanced option. Pass BlockRequest for replicating the block
  valueEncoding: 'json' | 'utf-8' | 'binary', // defaults to the core's valueEncoding
  decrypt: true, // automatically decrypts the block if encrypted
  raw: false, // Return block without decoding
}
```

#### `const has = await core.has(start, [end])`

Check if the core has all blocks between `start` and `end`.

#### `const updated = await core.update([options])`

Waits for initial proof of the new core length until all `findingPeers` calls has finished.

```js
const updated = await core.update()

console.log('core was updated?', updated, 'length is', core.length)
```

`options` include:

```js
{
  wait: false,
  activeRequests: undefined, // Advanced option. Pass requests for replicating blocks
  force: false, // Force an update even if core is writable.
}
```

Use `core.findingPeers()` or `{ wait: true }` to make `await core.update()` blocking.

#### `const [index, relativeOffset] = await core.seek(byteOffset, [options])`

Seek to a byte offset.

Returns `[index, relativeOffset]`, where `index` is the data block the `byteOffset` is contained in and `relativeOffset` is
the relative byte offset in the data block.

```js
await core.append([Buffer.from('abc'), Buffer.from('d'), Buffer.from('efg')])

const first = await core.seek(1) // returns [0, 1]
const second = await core.seek(3) // returns [1, 0]
const third = await core.seek(5) // returns [2, 1]
```

```js
{
  wait: true, // wait for data to be downloaded
  timeout: 0, // wait at max some milliseconds (0 means no timeout)
  activeRequests: undefined // Advanced option. Pass requests for replicating blocks
}
```

#### `const stream = core.createReadStream([options])`

Make a read stream to read a range of data out at once.

```js
// read the full core
const fullStream = core.createReadStream()

// read from block 10-14
const partialStream = core.createReadStream({ start: 10, end: 15 })

// pipe the stream somewhere using the .pipe method on Node.js or consume it as
// an async iterator

for await (const data of fullStream) {
  console.log('data:', data)
}
```

`options` include:

```js
{
  start: 0,
  end: core.length,
  wait: core.wait, // Whether to wait for updates from peers
  timeout: core.timeout, // How long to wait for updates from peers
  live: false, // Wait for next block keeping stream open / live
  snapshot: true // auto set end to core.length on open or update it on every read
}
```

#### `const bs = core.createByteStream([options])`

Make a byte stream to read a range of bytes.

```js
// Read the full core
const fullStream = core.createByteStream()

// Read from byte 3, and from there read 50 bytes
const partialStream = core.createByteStream({ byteOffset: 3, byteLength: 50 })

// Consume it as an async iterator
for await (const data of fullStream) {
  console.log('data:', data)
}

// Or pipe it somewhere like any stream:
partialStream.pipe(process.stdout)
```

`options` include:

```js
{
  byteOffset: 0, // Offset where to start from
  byteLength: core.byteLength - options.byteOffset, // How many bytes to read
  prefetch: 32 // How many bytes to download at a time
}
```

#### `const stream = core.createWriteStream()`

Make a write stream to append chunks as blocks.

```js
const ws = core.createWriteStream()

// Listen for stream finishing
const done = new Promise((resolve) => ws.on('finish', resolve))

for (const data of ['hello', 'world']) ws.write(data)
ws.end()

await done

console.log(await core.get(core.length - 2)) // 'hello'
console.log(await core.get(core.length - 1)) // 'world'
```

#### `const cleared = await core.clear(start, [end], [options])`

Clear stored blocks between `start` and `end`, reclaiming storage when possible.

```js
await core.clear(4) // clear block 4 from your local cache
await core.clear(0, 10) // clear block 0-10 from your local cache
```

The core will also gossip to peers it is connected to, that is no longer has these blocks.

`options` include:

```js
{
  diff: false // Returned `cleared` bytes object is null unless you enable this
}
```

#### `await core.truncate(newLength, [options])`

Truncate the core to a smaller length.

Per default this will update the fork id of the core to `+ 1`, but you can set the fork id you prefer with the option `fork`.
Note that the fork id should be monotonely incrementing.

`options` include:

```js
{
  fork: core.fork + 1, // The new fork id after truncating
  keyPair: core.keyPair, // Key pair used for signing the truncation
  signature: null, // Set signature for truncation
}
```

If `options` is a number, it will be used as the `fork` id.

#### `const hash = await core.treeHash([length])`

Get the Merkle Tree hash of the core at a given length, defaulting to the current length of the core.

#### `const proof = await core.proof(opts)`

Generate a proof (a `TreeProof` instance) for the request `opts`.

`opts` include:

```
{
  block: { index, nodes }, // Block request
  hash: { index, nodes }, // Hash Request
  seek: { bytes, padding }, // Seek Request
  upgrade: { start, length } // Upgrade request
}
```

Note that you cannot seek & provide a block / hash request when upgrading.

#### `const batch = await core.verifyFullyRemote(proof)`

Return the merkle tree batch from the proof. Will throw if the proof cannot be verified.

Note that you cannot seek & provide a block / hash request when upgrading.

#### `const buffer = await core.signable([length], [fork])`

Return a buffer which encodes the core's `key`, tree hash (`core.treeHash()`), `length`, & `fork`. If the `length` & `fork` arguments are not provided, the current values for the core are used by default.

#### `const range = core.download([range])`

Download a range of data.

You can await when the range has been fully downloaded by doing:

```js
await range.done()
```

A range can have the following properties:

```js
{
  start: startIndex,
  end: nonInclusiveEndIndex,
  blocks: [index1, index2, ...],
  linear: false, // download range linearly and not randomly
  activeRequests: undefined // Advanced option. Pass requests for replicating blocks
}
```

To download the full core continuously (often referred to as non sparse mode) do

```js
// Note that this will never be considered downloaded as the range
// will keep waiting for new blocks to be appended.
core.download({ start: 0, end: -1 })
```

To download a discrete range of blocks pass a list of indices.

```js
core.download({ blocks: [4, 9, 7] })
```

To cancel downloading a range simply destroy the range instance.

```js
// will stop downloading now
range.destroy()
```

#### `const ext = core.registerExtension(name, handlers = {})`

Register a custom protocol extension. This is a legacy implementation and is no longer recommended. Creating a [`Protomux`](https://github.com/holepunchto/protomux) protocol is recommended instead.

`handlers` include:

```
{
  encoding: 'json' | 'utf-8' | 'binary', // Compact encoding to use for messages. Defaults to buffer
  onmessage: (message, peer) => { ... } // Callback for when a message for the extension is receive
}
```

##### ext.send(message, peer)

Sends the `message` to a specific `peer`.

##### ext.broadcast(message)

Sends the `message` to all peers.

##### ext.destroy()

Unregister and remove extension from the hypercore.

#### `const session = core.session([options])`

Creates a new Hypercore instance that shares the same underlying core.

You must close any session you make.

Options are inherited from the parent instance, unless they are re-set.

`options` are the same as in the constructor with the follow additions:

```
{
  weak: false // Creates the session as a "weak ref" which closes when all non-weak sessions are closed
  exclusive: false, // Create a session with exclusive access to the core. Creating an exclusive session on a core with other exclusive sessions, will wait for the session with access to close before the next exclusive session is `ready`
  checkout: undefined, // A index to checkout the core at. Checkout sessions must be an atom or a named session
  atom: undefined, // A storage atom for making atomic batch changes across hypercores
  name: null, // Name the session creating a persisted branch of the core. Still beta so may break in the future
}
```

Atoms allow making atomic changes across multiple hypercores. Atoms can be created using a `core`'s `storage` (eg. `const atom = core.state.storage.createAtom()`). Changes made with an atom based session is not persisted until the atom is flushed via `await atom.flush()`, but can be read at any time. When atoms flush, all changes made outside of the atom will be clobbered as the core blocks will now match the atom's blocks. For example:

```js
const core = new Hypercore('./atom-example')
await core.ready()

await core.append('block 1')

const atom = core.state.storage.createAtom()
const atomicSession = core.session({ atom })

await core.append('block 2') // Add blocks not using the atom

await atomicSession.append('atom block 2') // Add different block to atom
await atom.flush()

console.log((await core.get(core.length - 1)).toString()) // prints 'atom block 2' not 'block 2'
```

#### `const { byteLength, length } = core.commit(session, opts = {})`

Attempt to apply blocks from the session to the `core`. `core` must be a default core, aka a non-named session.

Returns `null` if committing failed.

`opts` includes:

```
{
  length: session.length, // the core's length after committing the blocks
  treeLength: core.length, // The expected length of the core's merkle tree prior to commit
  keyPair: core.keyPair, // The keypair to use when committing
  signature: undefined, // The signature for the blocks being committed
}
```

#### `const snapshot = core.snapshot([options])`

Same as [`core.session(options)`](#const-session--coresessionoptions), but backed by a storage snapshot so will not truncate nor append.

#### `const info = await core.info([options])`

Get information about this core, such as its total size in bytes.

The object will look like this:

```js
Info {
  key: Buffer(...),
  discoveryKey: Buffer(...),
  length: 18,
  contiguousLength: 16,
  byteLength: 742,
  fork: 0,
  padding: 8,
  storage: {
    oplog: 8192,
    tree: 4096,
    blocks: 4096,
    bitfield: 4096
  }
}
```

`options` include:

```js
{
  storage: false // get storage estimates in bytes, disabled by default
}
```

#### `await core.close([{ error }])`

Fully close this core. Passing an error via `{ error }` is optional and all pending replicator requests will be rejected with the error.

#### `core.on('close')`

Emitted when the core has been fully closed.

#### `await core.ready()`

Wait for the core to fully open.

After this has called `core.length` and other properties have been set.

In general you do NOT need to wait for `ready`, unless checking a synchronous property,
as all internals await this themself.

#### `core.on('ready')`

Emitted after the core has initially opened all its internal state.

#### `core.writable`

Can we append to or truncate this core?

Populated after `ready` has been emitted. Will be `false` before the event.

#### `core.readable`

Can we read from this core? After closing the core this will be false.

Populated after `ready` has been emitted. Will be `false` before the event.

#### `core.id`

String containing the id (z-base-32 of the public key) identifying this core.

Populated after `ready` has been emitted. Will be `null` before the event.

#### `core.key`

Buffer containing the public key identifying this core.

Populated after `ready` has been emitted. Will be `null` before the event.

#### `core.keyPair`

Object containing buffers of the core's public and secret key

Populated after `ready` has been emitted. Will be `null` before the event.

#### `core.discoveryKey`

Buffer containing a key derived from the core's public key.
In contrast to `core.key` this key does not allow you to verify the data but can be used to announce or look for peers that are sharing the same core, without leaking the core key.

Populated after `ready` has been emitted. Will be `null` before the event.

#### `core.length`

How many blocks of data are available on this core.

Populated after `ready` has been emitted. Will be `0` before the event.

#### `core.signedLength`

How many blocks of data are available on this core that have been signed by a quorum. This is equal to `core.length` for Hypercores's with a single signer.

Populated after `ready` has been emitted. Will be `0` before the event.

#### `core.contiguousLength`

How many blocks are contiguously available starting from the first block of this core.

Populated after `ready` has been emitted. Will be `0` before the event.

#### `core.remoteContiguousLength`

How many blocks are contiguously available starting from the first block of this core on any known remote. This is only updated when a remote thinks it is fully contiguous such that they have all known blocks.

Populated after `ready` has been emitted. Will be `0` before the event.

#### `core.fork`

What is the current fork id of this core?

Populated after `ready` has been emitted. Will be `0` before the event.

#### `core.padding`

How much padding is applied to each block of this core? Will be `0` unless block encryption is enabled.

#### `core.peers`

Array of current peers the core is replicating with.

#### `await core.setEncryption(encryption)`

Set the encryption, which should satisfy the [HypercoreEncryption](https://github.com/holepunchto/hypercore-encryption) interface.

#### `await core.setEncryptionKey(key, [opts])`

Set the encryption key.

`opts` includes:

```
{
  block: false, // Whether the key is for block encryption
}
```

#### `core.setKeyPair(keyPair)`

Update the core's `keyPair`. Advanced as the `keyPair` is used throughout Hypercore, e.g. verifying blocks, identifying the core, etc.

#### `core.setActive(active)`

Set the core to be active or not. A core is considered 'active' if it should linger to download blocks from peers.

When calling `core.setActive(true)` make sure to later call `core.setActive(false)` to mark it as inactive otherwise the core's activity tracking will be inaccurate and keep replication channels open.

#### `await core.setUserData(key, value)`

Set a key in the User Data key-value store.

`key` is a string and `value` can be a string or buffer.

#### `const value = await core.getUserData(key)`

Return the value for a key in the User Data key-value store.

`key` is a string.

#### `const stream = core.replicate(isInitiatorOrReplicationStream, opts = {})`

Create a replication stream. You should pipe this to another Hypercore instance.

The `isInitiator` argument is a boolean indicating whether you are the initiator of the connection (ie the client)
or if you are the passive part (ie the server).

If you are using a P2P swarm like [Hyperswarm](https://github.com/hyperswarm/hyperswarm) you can know this by checking if the swarm connection is a client socket or server socket. In Hyperswarm you can check that using the [client property on the peer details object](https://github.com/hyperswarm/hyperswarm#swarmonconnection-socket-details--)

If you want to multiplex the replication over an existing Hypercore replication stream you can pass
another stream instance instead of the `isInitiator` boolean.

```js
// assuming we have two cores, localCore + remoteCore, sharing the same key
// on a server
const net = require('net')
const server = net.createServer(function (socket) {
  socket.pipe(remoteCore.replicate(false)).pipe(socket)
})

// on a client
const socket = net.connect(...)
socket.pipe(localCore.replicate(true)).pipe(socket)
```

`opts` are same as [`Hypercore.createProtocolStream()`](#const-stream--hypercorecreateprotocolstreamisinitiator-opts--).

#### `const done = core.findingPeers()`

Create a hook that tells Hypercore you are finding peers for this core in the background. Call `done` when your current discovery iteration is done.
If you're using Hyperswarm, you'd normally call this after a `swarm.flush()` finishes.

This allows `core.update` to wait for either the `findingPeers` hook to finish or one peer to appear before deciding whether it should wait for a merkle tree update before returning.

#### `core.on('append')`

Emitted when the core has been appended to (i.e. has a new length / byteLength), either locally or remotely.

#### `core.on('truncate', ancestors, forkId)`

Emitted when the core has been truncated, either locally or remotely.

#### `core.on('peer-add')`

Emitted when a new connection has been established with a peer.

#### `core.on('peer-remove')`

Emitted when a peer's connection has been closed.

#### `core.on('upload', index, byteLength, peer)`

Emitted when a block is uploaded to a peer.

#### `core.on('download', index, byteLength, peer)`

Emitted when a block is downloaded from a peer.

#### `core.on('remote-contiguous-length', length)`

Emitted when the max known contiguous `length` from a remote, ie `core.remoteContiguousLength`, is updated. Note this is not emitted when core is truncated.

#### `Hypercore.MAX_SUGGESTED_BLOCK_SIZE`

The constant for max size (15MB) for blocks appended to Hypercore. This max ensures blocks are replicated smoothly.

#### `const key = Hypercore.key(manifest, options = {})`

Returns the key for a given manifest.

`options` include:

```
{
  compat: false,  // Whether the manifest has a single singer whose public key is the key
  version,        // Manifest version if the manifest argument is the public key of a single singer
  namespace       // The signer namespace if the manifest argument is the public key of a single singer
}
```

#### `const dKey = Hypercore.discoveryKey(key)`

Returns the discovery key for the provided `key`.

#### `const bkey = Hypercore.blockEncryptionKey(key, encryptionKey)`

Returns a block encryption key derived from the `key` and `encryptionKey`.

#### `const mux = Hypercore.getProtocolMuxer(stream)`

Returns a protomux instance from the provided `stream` Hypercore protocol stream.

#### `const core = Hypercore.createCore(storage, opts)`

Returns the internal core using the `storage` and `opts` without creating a full Hypercore instance.

#### `const stream = Hypercore.createProtocolStream(isInitiator, opts = {})`

Create an encrypted noise stream with a protomux instance attached used for Hypercore's replication protocol.

`isInitiator` can be a framed stream, a protomux or a boolean for whether the stream should be the initiator in the noise handshake.

`opts` can include:

```
{
  keepAlive: true, // Whether to keep the stream alive
  ondiscoverykey: () => {}, // A handler for when a discovery key is set over the stream for corestore management
}
```

#### `const storage = Hypercore.defaultStorage(storage, opts = {})`

Returns a default hypercore storage. The `storage` argument can be a path where to create the `hypercore-storage` instance or an existing `hypercore-storage` instance. If an existing instance, it is immediately returned.
