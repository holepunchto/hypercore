# hypercore

Hypercore is a protocol and p2p network for distributing and replicating feeds of binary data. It is the low level component that [Hyperdrive](https://github.com/mafintosh/hyperdrive) is built on top of.

```
npm install hypercore
```

[![build status](http://img.shields.io/travis/mafintosh/hypercore/master.svg?style=flat)](http://travis-ci.org/mafintosh/hypercore)

It runs both in the node and in the browser using [browserify](https://github.com/substack/node-browserify).

## Usage

``` js
var hypercore = require('hypercore')
var net = require('net')

var core = hypercore(db) // db is a leveldb instance
var feed = core.createFeed()

feed.append(['hello', 'world'], function () {
  console.log('appended two blocks')
  console.log('key is', feed.key.toString('hex'))
})

feed.on('upload', function (block, data) {
  console.log('uploaded block', block, data)
})

var server = net.createServer(function (socket) {
  socket.pipe(feed.replicate()).pipe(socket)
})

server.listen(10000)
```

In another process

``` js
var core = hypercore(anotherDb)
var feed = core.createFeed(<key-printed-out-above>)
var socket = net.connect(10000)

socket.pipe(feed.replicate()).pipe(socket)

feed.on('download', function (block, data) {
  console.log('downloaded block', block, data)
})
```

## API

#### `var core = hypercore(db)`

Create a new hypercore instance. `db` should be a leveldb instance.

#### `var feed = core.createFeed([key], [options])`

Create a new feed. A feed stores a list of append-only data (buffers). A feed has a `.key` property that you can pass in to `createFeed` if you want to retrieve an old feed. Per default all feeds are appendable (live).

Options include:

``` js
{
  live: true,
  storage: externalStorage,
  sparse: false,
  verifyReplicationReads: false
}
```

Set `sparse` to `true` if you only want to download the pieces of the feed you are requesting / prioritizing. Otherwise the entire feed will be downloaded if nothing else is prioritized.

Set `verifyReplicationReads` to `true` to automatically check the integrity of the feed-data during replication. This is computationally more intensive, but it allows the feed to recognize when local data has been changed or lost and correct its internal tracking.

If you want to create a static feed, one you cannot reappend data to, pass the `{live: false}` option.
The storage option allows you to store data outside of leveldb. This is very useful if you use hypercore to distribute files.

See the [Storage API](#storage-api) section for more info

Set `key` and `secretKey` (which correspond to `publicKey` and `secretKey` of `sodium-signatures` key pairs) to specify the secret key. *Note:* do not use the same key pair for more than one feed, as keys are used to tell apart different feeds.

#### `var stream = core.replicate(opts)`

Create a generic replication stream. Use the `feed.replicate(stream)` API described below to replicate specific feeds of data.
Options include:

``` js
{
  upload: true, // upload data to other peer
  download: true // download data from other peer
}
```

#### `var stream = core.list([options], [callback])`

List all feed keys in the database. Optionally you can pass a callback to buffer them into an array. Options include:

``` js
{
  values: false // set this to get feed attributes, not just feed keys
}
```

## `Feed API`

As mentioned above a feed stores a list of data for you that you can replicate to other peers. It has the following API

#### `feed.key`

The key of this feed. A 32 byte buffer. Other peers need this key to start replicating the feed.

#### `feed.discoveryKey`

A 32 byte buffer containing a discovery key of the feed. The discovery key is sha-256 hmac of the string `hypercore` using the feed key as the password.
You can use the discovery key to find other peers sharing this feed without disclosing your feed key to a third party.

#### `feed.blocks`

The total number of known data blocks in the feed.

#### `feed.bytes`

The total byte size of known data blocks in the feed.

#### `feed.peers`

Peers connected on the feed.

Each peer has the properties:

```js
peer.remoteLength // Number of blocks the peer has
peer.bytesDownloaded // Number of bytes downloaded from this peer
peer.downloaded // Number of blocks downloaded from this peer
```

#### `feed.open(cb)`

Call this method to ensure that a feed is opened. You do not need to call this but the `.blocks` property will not be populated until the feed has been opened.

#### `feed.append(data, callback)`

Append a block of data to the feed. If you want to append more than one block you can pass in an array.

#### `feed.get(index, [options,] callback)`

Retrieve a block of data from the feed. If the block is not locally-available, this call will queue the block for download and defer calling-back until the block is received. Options include:

``` js
{
  verify: false // verify the data against the feed checksum, and fail the get() if !==
  wait: true // queue the download if not found. If false, will just respond with a notFound error
}
```

#### `feed.verifyStorage(callback)`

Do a full read of the feed's data from storage, and verify the data against the feed's checksums. If any blocks that were thought to be saved are found missing, they will be marked as absent.

Use this as a way to correct the internal tracking of available data, if you think the files were modified externally. Be warned: it will require a lot of disk-reads, as every block has to be verified.

Calls back with the following structure:

```js
{
  numMissing: number of blocks discovered missing
}
```

#### `feed.blocksRemaining()`

Get the number of blocks remaining to be downloaded.

#### `feed.prioritize(range, [callback])`

Prioritize a range of blocks to download. Will call the callback when done.
Range should look like this

``` js
{
  start: startBlock,
  end: optionalEndBlock,
  priority: 2 // a priority level spanning [0-5]
  linear: false // download the range linearly
}
```

#### `feed.unprioritize(range)`

Unprioritize a range.

#### `feed.seek(byteOffset, callback)`

Find the block of data containing the byte offset. Calls the callback with `(err, index, offset)` where `index` is the block index and `offset` is the the relative byte offset in the block returned by `.get(index)`.

#### `feed.finalize(callback)`

If you are not using a live feed you need to call this method to finalize the feed once you are ready to share it.
Finalizing will set the `.key` property and allow other peers to get your data.

#### `var stream = feed.createWriteStream([options])`

Create a writable stream that appends to the feed. If the feed is a static feed, it will be finalized when you end the stream.

#### `var stream = feed.createReadStream([options])`

Create a readable stream that reads from the feed. Options include:

``` js
{
  start: startIndex, // read from this index
  end: endIndex, // read until this index
  live: false // set this to keep the read stream open
}
```

#### `var stream = feed.replicate([options])`

Get a replication stream for this feed. Pipe this to another peer to start replicating this feed with another peer.
If you create multiple replication streams to multiple peers you'll upload/download data to all of them (meaning the load will spread out).

Per default the replication stream encrypts all messages sent using the feed key and an incrementing nonce. This helps ensures that the remote peer also the feed key and makes it harder for a man-in-the-middle to sniff the data you are sending.

Set `{private: false}` to disable this.

Hypercore uses a simple multiplexed protocol that allows one replication stream to be used for multiple feeds at once.
If you want to join another replication stream simply pass it as the stream option

``` js
feed.replicate({stream: anotherReplicationStream})
```

As a shorthand you can also do `feed.replicate(stream)`.

#### `feed.unreplicate([stream])`

End a replication stream for the feed. Optionally can pass the `stream` argument to end a specific replication stream.

#### `stream.on('open', discoveryKey)`

Emitted when a remote feed joins the replication stream and you haven't. You can use this as a signal to join the stream yourself if you want to.

#### `feed.on('download', block, data)`

Emitted when a data block has been downloaded

#### `feed.on('download-finished')`

Emitted when all available data has been downloaded.
Will re-fire when a live feed is updated and you download all the new blocks.

#### `feed.on('upload', block, data)`

Emitted when a data block has been uploaded

## Storage API

If you want to use external storage to store the hypercore data (metadata will still be stored in the leveldb) you need to implement the following api and provide that as the `storage` option when creating a feed.

Some node modules that implement this interface are

* [random-access-file](https://github.com/mafintosh/random-access-file) Writes data to a file.
* [random-access-memory](https://github.com/mafintosh/random-access-memory) Writes data to memory.

#### `storage.open(cb)`

This API is *optional*. If you provide this hypercore will call `.open` and wait for the callback to be called before calling any other methods.

#### `storage.read(offset, length, cb)`

This API is *required*. Hypercore calls this when it wants to read data. You should return a buffer with length `length` that way read at the corresponding offset. If you cannot read this buffer call the callback with an error.

#### `storage.write(offset, buffer, cb)`

This API is *required*. Hypercore calls this when it wants to write data. You should write the buffer at the corresponding offset and call the callback afterwards. If there was an error writing you should call the callback with that error.

#### `storage.close(cb)`

This API is *optional*. Hypercore will call this method when the feed is closing.

## License

MIT
