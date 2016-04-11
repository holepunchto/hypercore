# hypercore

Hypercore is a protocol and p2p network for distributing and replicating feeds of binary data. It is the low level component that [Hyperdrive](https://github.com/mafintosh/hyperdrive) is built on top off.

```
npm install hypercore
```

[![build status](http://img.shields.io/travis/mafintosh/hypercore.svg?style=flat)](http://travis-ci.org/mafintosh/hypercore)

It runs both in the node and in the browser using [browserify](https://github.com/substack/browserify).

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
If you want to create a static feed, one you cannot reappend data to, pass the `{live: false}` option.

#### `var stream = core.replicate()`

Create a generic replication stream. Use the `feed.join(stream)` API described below to replicate specific feeds of data.

#### `var stream = core.list([callback])`

List all feed keys in the database. Optionally you can pass a callback to buffer them into an array.

#### `var stream = core.createWriteStream([key], [options])`

Returns a feed as a writable stream.

#### `var stream = core.createReadStream(key, [options])`

Returns a feed as a readable stream.

## `Feed API`

As mentioned above a feed stores a list of data for you that you can replicate to other peers. It has the following API

#### `feed.key`

The key of this feed. A 32 byte buffer. Other peers need this key to start replicating the feed.

#### `feed.publicId`

A 32 byte buffer containing a public id of the feed. The public id is sha-256 hmac of the string `hypercore` using the feed key as the password.
You can use the public id to find other peers sharing this feed without disclosing your feed key to a third party.

#### `feed.blocks`

The total number of known data blocks in the feed.

#### `feed.open(cb)`

Call this method to ensure that a feed is opened. You do not need to call this but the `.blocks` property will not be populated until the feed has been opened.

#### `feed.append(data, callback)`

Append a piece of data to the feed. If you want to append more than once piece you can pass in an array.

#### `feed.flush(callback)`

Flushes all pending appends and calls the callback.

#### `feed.get(index, callback)`

Retrieve a piece of data from the feed.

#### `feed.finalize(callback)`

If you are not using a live feed you need to call this method to finalize the feed once you are ready to share it.
Finalizing will set the `.key` property and allow other peers to get your data.

#### `var stream = feed.replicate([options])`

Get a replication stream for this feed. Pipe this to another peer to start replicating this feed with another peer.
If you create multiple replication streams to multiple peers you'll upload/download data to all of them (meaning the load will spread out).

Per default the replication stream encrypts all messages sent using the feed key and an incrementing nonce. This helps ensures that the remote peer also the feed key and makes it harder for a man-in-the-middle to sniff the data you are sending.

Set `{encrypted: false}` to disable this.

#### `feed.join(stream)`

Join another replication stream. Hypercore uses a simple multiplexed protocol that allows one replication stream to be used for multiple feeds at once.
You do not need to join a replication stream that you created using `feed.replicate()` - you implicitly join that one.

#### `feed.leave(stream)`

Leave a replication stream.

#### `stream.on('feed', publicId)`

Emitted when a remote feed joins the replication stream. You can use this as a signal to join the stream yourself if you want to.

#### `feed.on('download', block, data)`

Emitted when a data block has been downloaded

#### `feed.on('upload', block, data)`

Emitted when a data block has been uploaded

## License

MIT
