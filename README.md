# Hypercore

### [See the full API docs at docs.holepunch.to](https://docs.holepunch.to/building-blocks/hypercore)

Hypercore is a secure, distributed append-only log.

Built for sharing large datasets and streams of real time data

## Features

* **Sparse replication.** Only download the data you are interested in.
* **Realtime.** Get the latest updates to the log fast and securely.
* **Performant.** Uses a simple flat file structure to maximize I/O performance.
* **Secure.** Uses signed merkle trees to verify log integrity in real time.
* **Modular.** Hypercore aims to do one thing and one thing well - distributing a stream of data.

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

``` js
const core = new Hypercore('./directory') // store data in ./directory
```

Alternatively you can pass a function instead that is called with every filename Hypercore needs to function and return your own [abstract-random-access](https://github.com/random-access-storage/abstract-random-access) instance that is used to store the data.

``` js
const RAM = require('random-access-memory')
const core = new Hypercore((filename) => {
  // filename will be one of: data, bitfield, tree, signatures, key, secret_key
  // the data file will contain all your data concatenated.

  // just store all files in ram by returning a random-access-memory instance
  return new RAM()
})
```

Per default Hypercore uses [random-access-file](https://github.com/random-access-storage/random-access-file). This is also useful if you want to store specific files in other directories.

Hypercore will produce the following files:

* `oplog` - The internal truncating journal/oplog that tracks mutations, the public key and other metadata.
* `tree` - The Merkle Tree file.
* `bitfield` - The bitfield of which data blocks this core has.
* `data` - The raw data of each block.

Note that `tree`, `data`, and `bitfield` are normally heavily sparse files.

`key` can be set to a Hypercore public key. If you do not set this the public key will be loaded from storage. If no key exists a new key pair will be generated.

`options` include:

``` js
{
  createIfMissing: true, // create a new Hypercore key pair if none was present in storage
  overwrite: false, // overwrite any old Hypercore that might already exist
  sparse: true, // enable sparse mode, counting unavailable blocks towards core.length and core.byteLength
  valueEncoding: 'json' | 'utf-8' | 'binary', // defaults to binary
  encodeBatch: batch => { ... }, // optionally apply an encoding to complete batches
  keyPair: kp, // optionally pass the public key and secret key as a key pair
  encryptionKey: k, // optionally pass an encryption key to enable block encryption
  onwait: () => {} // hook that is called if gets are waiting for download
}
```

You can also set valueEncoding to any [abstract-encoding](https://github.com/mafintosh/abstract-encoding) or [compact-encoding](https://github.com/compact-encoding) instance.

valueEncodings will be applied to individual blocks, even if you append batches. If you want to control encoding at the batch-level, you can use the `encodeBatch` option, which is a function that takes a batch and returns a binary-encoded batch. If you provide a custom valueEncoding, it will not be applied prior to `encodeBatch`.

#### `const { length, byteLength } = await core.append(block)`

Append a block of data (or an array of blocks) to the core.
Returns the new length and byte length of the core.

``` js
// simple call append with a new block of data
await core.append(Buffer.from('I am a block of data'))

// pass an array to append multiple blocks as a batch
await core.append([Buffer.from('batch block 1'), Buffer.from('batch block 2')])
```

#### `const block = await core.get(index, [options])`

Get a block of data.
If the data is not available locally this method will prioritize and wait for the data to be downloaded.

``` js
// get block #42
const block = await core.get(42)

// get block #43, but only wait 5s
const blockIfFast = await core.get(43, { timeout: 5000 })

// get block #44, but only if we have it locally
const blockLocal = await core.get(44, { wait: false })
```

`options` include:

``` js
{
  wait: true, // wait for block to be downloaded
  onwait: () => {}, // hook that is called if the get is waiting for download
  timeout: 0, // wait at max some milliseconds (0 means no timeout)
  valueEncoding: 'json' | 'utf-8' | 'binary', // defaults to the core's valueEncoding
  decrypt: true // automatically decrypts the block if encrypted
}
```

#### `const has = await core.has(start, [end])`

Check if the core has all blocks between `start` and `end`.

#### `const updated = await core.update([options])`

Waits for initial proof of the new core length until all `findingPeers` calls has finished.

``` js
const updated = await core.update()

console.log('core was updated?', updated, 'length is', core.length)
```

`options` include:

``` js
{
  wait: false
}
```

Use `core.findingPeers()` or `{ wait: true }` to make `await core.update()` blocking.

#### `const [index, relativeOffset] = await core.seek(byteOffset, [options])`

Seek to a byte offset.

Returns `[index, relativeOffset]`, where `index` is the data block the `byteOffset` is contained in and `relativeOffset` is
the relative byte offset in the data block.

``` js
await core.append([Buffer.from('abc'), Buffer.from('d'), Buffer.from('efg')])

const first = await core.seek(1) // returns [0, 1]
const second = await core.seek(3) // returns [1, 0]
const third = await core.seek(5) // returns [2, 1]
```

``` js
{
  wait: true, // wait for data to be downloaded
  timeout: 0 // wait at max some milliseconds (0 means no timeout)
}
```

#### `const stream = core.createReadStream([options])`

Make a read stream to read a range of data out at once.

``` js
// read the full core
const fullStream = core.createReadStream()

// read from block 10-15
const partialStream = core.createReadStream({ start: 10, end: 15 })

// pipe the stream somewhere using the .pipe method on Node.js or consume it as
// an async iterator

for await (const data of fullStream) {
  console.log('data:', data)
}
```

`options` include:

``` js
{
  start: 0,
  end: core.length,
  live: false,
  snapshot: true // auto set end to core.length on open or update it on every read
}
```

#### `await core.clear(start, [end])`

Clear stored blocks between `start` and `end`, reclaiming storage when possible.

``` js
await core.clear(4) // clear block 4 from your local cache
await core.clear(0, 10) // clear block 0-10 from your local cache
```

The core will also gossip to peers it is connected to, that is no longer has these blocks.

#### `await core.truncate(newLength, [forkId])`

Truncate the core to a smaller length.

Per default this will update the fork id of the core to `+ 1`, but you can set the fork id you prefer with the option.
Note that the fork id should be monotonely incrementing.

#### `const hash = await core.treeHash([length])`

Get the Merkle Tree hash of the core at a given length, defaulting to the current length of the core.

#### `const range = core.download([range])`

Download a range of data.

You can await when the range has been fully downloaded by doing:

```js
await range.done()
```

A range can have the following properties:

``` js
{
  start: startIndex,
  end: nonInclusiveEndIndex,
  blocks: [index1, index2, ...],
  linear: false // download range linearly and not randomly
}
```

To download the full core continously (often referred to as non sparse mode) do

``` js
// Note that this will never be consider downloaded as the range
// will keep waiting for new blocks to be appended.
core.download({ start: 0, end: -1 })
```

To downloaded a discrete range of blocks pass a list of indices.

```js
core.download({ blocks: [4, 9, 7] })
```

To cancel downloading a range simply destroy the range instance.

``` js
// will stop downloading now
range.destroy()
```

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

#### `await core.close()`

Fully close this core.

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

Can we append to this core?

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

#### `core.encryptionKey`

Buffer containing the optional block encryption key of this core. Will be `null` unless block encryption is enabled.

#### `core.length`

How many blocks of data are available on this core? If `sparse: false`, this will equal `core.contiguousLength`.

Populated after `ready` has been emitted. Will be `0` before the event.

#### `core.contiguousLength`

How many blocks are contiguously available starting from the first block of this core?

Populated after `ready` has been emitted. Will be `0` before the event.

#### `core.fork`

What is the current fork id of this core?

Populated after `ready` has been emitted. Will be `0` before the event.

#### `core.padding`

How much padding is applied to each block of this core? Will be `0` unless block encryption is enabled.

#### `const stream = core.replicate(isInitiatorOrReplicationStream)`

Create a replication stream. You should pipe this to another Hypercore instance.

The `isInitiator` argument is a boolean indicating whether you are the iniatior of the connection (ie the client)
or if you are the passive part (ie the server).

If you are using a P2P swarm like [Hyperswarm](https://github.com/hyperswarm/hyperswarm) you can know this by checking if the swarm connection is a client socket or server socket. In Hyperswarm you can check that using the [client property on the peer details object](https://github.com/hyperswarm/hyperswarm#swarmonconnection-socket-details--)

If you want to multiplex the replication over an existing Hypercore replication stream you can pass
another stream instance instead of the `isInitiator` boolean.

``` js
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
