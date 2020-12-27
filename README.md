# hypercore

Hypercore is a secure, distributed append-only log.

Built for sharing large datasets and streams of real time data as part of the [Hypercore Protocol](https://hypercore-protocol.org).

``` sh
npm install hypercore
```

[![Build Status](https://github.com/hypercore-protocol/hypercore/workflows/Build%20Status/badge.svg)](https://github.com/hypercore-protocol/hypercore/actions?query=workflow%3A%22Build+Status%22)

To learn more about how hypercore works on a technical level read the [Dat paper](https://github.com/datprotocol/whitepaper/blob/master/dat-paper.pdf).

## Features

* **Sparse replication.** Only download the data you are interested in.
* **Realtime.** Get the latest updates to the log fast and securely.
* **Performant.** Uses a simple flat file structure to maximize I/O performance.
* **Secure.** Uses signed merkle trees to verify log integrity in real time.
* **Browser support.** Simply pick a storage provider (like [random-access-memory](https://github.com/random-access-storage/random-access-memory)) that works in the browser

Note that the latest release is Hypercore 8, which is not compatible with Hypercore 7 on the wire format, but storage compatible.

## Usage

``` js
var hypercore = require('hypercore')
var feed = hypercore('./my-first-dataset', {valueEncoding: 'utf-8'})

feed.append('hello')
feed.append('world', function (err) {
  if (err) throw err
  feed.get(0, console.log) // prints hello
  feed.get(1, console.log) // prints world
})
```

To get find other modules that help with building data structures, P2P networks etc on top of Hypercore see the [companion modules](#Companion-modules) list at the bottom of this page.

## Terminology

 - **feed**. This is what hypercores are: a data feed. Feeds are permanent data structures that can be shared on the dat network.
 - **stream**. Streams are a tool in the code for reading or writing data. Streams are temporary and almost always returned by functions.
 - **pipe**. Streams tend to either be readable (giving data) or writable (receiving data). If you connect a readable to a writable, that's called piping.
 - **replication stream**. A stream returned by the `replicate()` function which can be piped to a peer. It is used to sync the peers' hypercore feeds.
 - **swarming**. Swarming describes adding yourself to the network, finding peers, and sharing data with them. Piping a replication feed describes sharing the data with one peer.

## API

#### `var feed = hypercore(storage, [key], [options])`

Create a new hypercore feed.

`storage` should be set to a directory where you want to store the data and feed metadata.

``` js
var feed = hypercore('./directory') // store data in ./directory
```

Alternatively you can pass a function instead that is called with every filename hypercore needs to function and return your own [abstract-random-access](https://github.com/random-access-storage/abstract-random-access) instance that is used to store the data.

``` js
var ram = require('random-access-memory')
var feed = hypercore(function (filename) {
  // filename will be one of: data, bitfield, tree, signatures, key, secret_key
  // the data file will contain all your data concatenated.

  // just store all files in ram by returning a random-access-memory instance
  return ram()
})
```

Per default hypercore uses [random-access-file](https://github.com/random-access-storage/random-access-file). This is also useful if you want to store specific files in other directories. For example you might want to store the secret key elsewhere.

`key` can be set to a hypercore feed public key. If you do not set this the public key will be loaded from storage. If no key exists a new key pair will be generated.

`options` include:

``` js
{
  createIfMissing: true, // create a new hypercore key pair if none was present in storage
  overwrite: false, // overwrite any old hypercore that might already exist
  valueEncoding: 'json' | 'utf-8' | 'binary', // defaults to binary
  sparse: false, // do not mark the entire feed to be downloaded
  eagerUpdate: true, // always fetch the latest update that is advertised. default false in sparse mode.
  secretKey: buffer, // optionally pass the corresponding secret key yourself
  storeSecretKey: true, // if false, will not save the secret key
  storageCacheSize: 65536, // the # of entries to keep in the storage system's LRU cache (false or 0 to disable)
  onwrite: (index, data, peer, cb) // optional hook called before data is written after being verified
                                   // (remember to call cb() at the end of your handler)
  stats: true // collect network-related statistics,
  // Optionally use custom cryptography for signatures
  crypto: {
    sign (data, secretKey, cb(err, signature)),
    verify (signature, data, key, cb(err, valid))
  }
  noiseKeyPair: { publicKey, secretKey } // set a static key pair to use for Noise authentication when replicating
}
```

You can also set valueEncoding to any [abstract-encoding](https://github.com/mafintosh/abstract-encoding) instance.

__Note:__ The `[key]` and `secretKey` are _Node.js_ buffer instances, not browser-based ArrayBuffer instances. When creating hypercores in browser, if you pass an ArrayBuffer instance, you will get an error similar to `key must be at least 16, was given undefined`. Instead, create a Node.js Buffer instance using [Feross‘s](https://github.com/feross) [buffer](https://github.com/feross/buffer) module (`npm install buffer`). e.g.,

```javascript
const storage = someRandomAccessStorage
const myPublicKey = someUint8Array

const Buffer = require('buffer').Buffer
const hypercorePublicKeyBuffer = Buffer.from(myPublicKey.buffer)

const hypercore = hypercore(storage, hypercorePublicKeyBuffer)
```

#### `feed.append(data, [callback])`

Append a block of data to the feed.

Callback is called with `(err, seq)` when all data has been written at the returned `seq` number or error will be not `null`.

#### `const id = feed.get(index, [options], callback)`

Get a block of data.
If the data is not available locally this method will prioritize and wait for the data to be downloaded before calling the callback.

Options include

``` js
{
  wait: true, // wait for index to be downloaded
  onwait: () => {}, // hook that is called if the get is waiting for download
  timeout: 0, // wait at max some milliseconds (0 means no timeout)
  valueEncoding: 'json' | 'utf-8' | 'binary' // defaults to the feed's valueEncoding
}
```

Callback is called with `(err, data)`

#### `feed.getBatch(start, end, [options], callback)`

Get a range of blocks efficiently. End index is non-inclusive. Options include

``` js
{
  wait: sameAsAbove,
  timeout: sameAsAbove,
  valueEncoding: sameAsAbove
}
```

#### `feed.cancel(getId)`

Cancel a pending get.

#### `feed.head([options], callback)`

Get the block of data at the tip of the feed. This will be the most recently
appended block.

Accepts the same `options` as `feed.get()`.

#### `const id = feed.download([range], [callback])`

Download a range of data. Callback is called when all data has been downloaded.
A range can have the following properties:

``` js
{
  start: startIndex,
  end: nonInclusiveEndIndex,
  linear: false // download range linearly and not randomly
}
```

If you do not mark a range the entire feed will be marked for download.

If you have not enabled sparse mode (`sparse: true` in the feed constructor) then the entire
feed will be marked for download when the feed is created.

If you have an array of blocks you want to get downloaded you also also pass that

``` js
{
  blocks: [0, 1, 4, 10] // will download those 4 blocks as fast as possible
}
```

#### `feed.undownload(downloadId)`

Cancel a previous download request.

#### `feed.signature([index], callback)`

Get a signature proving the correctness of the block at index, or the whole stream.

Callback is called with `(err, signature)`.
The signature has the following properties:
``` js
{
  index: lastSignedBlock,
  signature: Buffer
}
```

#### `feed.verify(index, signature, callback)`

Verify a signature is correct for the data up to index, which must be the last signed
block associated with the signature.

Callback is called with `(err, success)` where success is true only if the signature is
correct.

#### `feed.rootHashes(index, callback)`

Retrieve the root *hashes* for given `index`.

Callback is called with `(err, roots)`; `roots` is an *Array* of *Node* objects:
```
Node {
  index: location in the merkle tree of this root,
  size: total bytes in children of this root,
  hash: hash of the children of this root (32-byte buffer)
}
```


#### `var number = feed.downloaded([start], [end])`

Returns total number of downloaded blocks within range.
If `end` is not specified it will default to the total number of blocks.
If `start` is not specified it will default to 0.

#### `var bool = feed.has(index)`

Return true if a data block is available locally.
False otherwise.

#### `var bool = feed.has(start, end)`
Return true if all data blocks within a range are available locally.
False otherwise.

#### `feed.clear(start, [end], [callback])`

Clear a range of data from the local cache.
Will clear the data from the bitfield and make a call to the underlying storage provider to delete the byte range the range occupies.

`end` defaults to `start + 1`.

#### `feed.seek(byteOffset, callback)`

Seek to a byte offset.

Calls the callback with `(err, index, relativeOffset)`, where `index` is the data block the byteOffset is contained in and `relativeOffset` is
the relative byte offset in the data block.

#### `feed.update([minLength], [callback])`

Wait for the feed to contain at least `minLength` elements.
If you do not provide `minLength` it will be set to current length + 1.

Does not download any data from peers except for a proof of the new feed length.

``` js
console.log('length is', feed.length)
feed.update(function () {
  console.log('length has increased', feed.length)
})
```

Per default update will wait until a peer arrives and the update can be performed.
If you only wanna check if any of the current peers you are connected to can
update you (and return an error otherwise if you use the `ifAvailable` option)

``` js
feed.update({ ifAvailable: true, minLength: 10 }, function (err) {
  // returns an error if non of your current peers can update you
})
```

#### `feed.setDownloading(bool)`

Call this with `false` to make the feed stop downloading from other peers.

#### `feed.setUploading(bool)`

Call this with `false` to make the feed stop uploading to other peers.

#### `var stream = feed.createReadStream([options])`

Create a readable stream of data.

Options include:

``` js
{
  start: 0, // read from this index
  end: feed.length, // read until this index
  snapshot: true, // if set to false it will update `end` to `feed.length` on every read
  tail: false, // sets `start` to `feed.length`
  live: false, // set to true to keep reading forever
  timeout: 0, // timeout for each data event (0 means no timeout)
  wait: true, // wait for data to be downloaded
  batch: 1 // amount of messages to read in batch, increasing it (e.g. 100) can improve the performance reading
}
```

#### `var stream = feed.createWriteStream(opts)`

Create a writable stream.
Options include:

```
{
  maxBlockSize: Infinity // set this to auto chunk individual blocks if they are larger than this number
}
```

#### `var stream = feed.replicate(isInitiator, [options])`

Create a replication stream. You should pipe this to another hypercore instance.

The `isInitiator` argument is a boolean indicating whether you are the iniatior of the connection (ie the client)
or if you are the passive part (ie the server).

If you are using a P2P swarm like [Hyperswarm](https://github.com/hyperswarm/hyperswarm) you can know this by checking if the swarm connection is a client socket or server socket. In Hyperswarm you can check that using [client property on the peer details object](https://github.com/hyperswarm/hyperswarm#swarmonconnection-socket-details--)

If you want to multiplex the replication over an existing hypercore replication stream you can pass
another stream instance instead of the `isInitiator` boolean.

``` js
// assuming we have two feeds, localFeed + remoteFeed, sharing the same key
// on a server
var net = require('net')
var server = net.createServer(function (socket) {
  socket.pipe(remoteFeed.replicate(false)).pipe(socket)
})

// on a client
var socket = net.connect(...)
socket.pipe(localFeed.replicate(true)).pipe(socket)
```

Options include:

``` js
{
  live: false, // keep replicating after all remote data has been downloaded?
  ack: false, // set to true to get explicit acknowledgement when a peer has written a block
  download: true, // download data from peers?
  upload: true, // upload data to peers?
  encrypted: true, // encrypt the data sent using the hypercore key pair
  noise: true, // set to false to disable the NOISE handshake completely, and also disable the capability verification. works only together with encrypted = false.
  keyPair: { publicKey, secretKey }, // use this keypair for Noise authentication
  onauthenticate (remotePublicKey, done) // hook that can be used to authenticate the remote peer.
                                         // calling done with an error will disallow the peer from connecting to you.
}
```

When `ack` is `true`, you can listen on the replication `stream` for an `ack`
event:

``` js
var stream = feed.replicate({ ack: true })
stream.on('ack', function (ack) {
  console.log(ack.start, ack.length)
})
```

#### `feed.close([callback])`

Fully close this feed.

Calls the callback with `(err)` when all storage has been closed.

#### `feed.destroyStorage([callback])`

Destroys all stored data and fully closes this feed.

Calls the callback with `(err)` when all storage has been deleted and closed.

#### `feed.audit([callback])`

Audit all data in the feed. Will check that all current data stored
matches the hashes in the merkle tree and clear the bitfield if not.

When done a report is passed to the callback that looks like this:

```js
{
  valid: 10, // how many data blocks matches the hashes
  invalid: 0, // how many did not
}
```

If a block does not match the hash it is cleared from the data bitfield.

#### `feed.writable`

Can we append to this feed?

Populated after `ready` has been emitted. Will be `false` before the event.

#### `feed.readable`

Can we read from this feed? After closing the feed this will be false.

Populated after `ready` has been emitted. Will be `false` before the event.

#### `feed.key`

Buffer containing the public key identifying this feed.

Populated after `ready` has been emitted. Will be `null` before the event.

#### `feed.discoveryKey`

Buffer containing a key derived from the feeds' public key.
In contrast to `feed.key` this key does not allow you to verify the data but can be used to announce or look for peers that are sharing the same feed, without leaking the feed key.

Populated after `ready` has been emitted. Will be `null` before the event.

#### `feed.length`

How many blocks of data are available on this feed?

Populated after `ready` has been emitted. Will be `0` before the event.

#### `feed.byteLength`

How much data is available on this feed in bytes?

Populated after `ready` has been emitted. Will be `0` before the event.

#### `feed.stats`

Return per-peer and total upload/download counts.

The returned object is of the form:
```js
{
  totals: {
    uploadedBytes: 100,
    uploadedBlocks: 1,
    downloadedBytes: 0,
    downloadedBlocks: 0
  },
  peers: [
    {
      uploadedBytes: 100,
      uploadedBlocks: 1,
      downloadedBytes: 0,
      downloadedBlocks: 0
    },
    ...
  ]
}
```

Stats will be collected by default, but this can be disabled by setting `opts.stats` to false.

#### `feed.on('peer-add', peer)`

Emitted when a peer is added.

#### `feed.on('peer-remove', peer)`

Emitted when a peer is removed.

#### `feed.on('peer-open', peer)`

Emitted when a peer channel has been fully opened.

#### `feed.peers`

A list of all peers you are connected with.

#### `ext = feed.registerExtension(name, handlers)`

Register a new replication extension. `name` should be the name of your extension and `handlers` should look like this:

```js
{
  encoding: 'json' | 'binary' | 'utf-8' | anyAbstractEncoding,
  onmessage (message, peer) {
    // called when a message is received from a peer
    // will be decoded using the encoding you provide
  },
  onerror (err) {
    // called in case of an decoding error
  }
}
```

#### `ext.send(message, peer)`

Send an extension message to a specific peer.

#### `ext.broadcast(message)`

Send a message to every peer you are connected to.

#### `peer.publicKey`

Get the public key buffer for this peer. Useful for identifying a peer in the swarm.

#### `feed.on('ready')`

Emitted when the feed is ready and all properties have been populated.

#### `feed.on('error', err)`

Emitted when the feed experiences a critical error.

#### `feed.on('download', index, data)`

Emitted when a data block has been downloaded.

#### `feed.on('upload', index, data)`

Emitted when a data block is going to be uploaded.

#### `feed.on('append')`

Emitted when the feed has been appended to (i.e. has a new length / byteLength).

#### `feed.on('sync')`

Emitted every time ALL data from `0` to `feed.length` has been downloaded.

#### `feed.on('close')`

Emitted when the feed has been fully closed

## Companion modules

Hypercore works really well with a series of other modules. This in a non-exhaustive list of some of those:

* [Hyperswarm](https://github.com/hyperswarm/hyperswarm) - P2P swarming module that can you share Hypercores over a network.
* [Hyperswarm replicator](https://github.com/hyperswarm/replicator) - Wanna share a single Hypercore without any hastle over a network?
* [Hyperdrive](https://github.com/hypercore-protocol/hyperdrive) - Filesystem abstraction built on Hypercores
* [Hypertrie](https://github.com/hypercore-protocol/hypertrie) - Scalable key/value store built on Hypercores

## License

MIT
