# hypercore

Hypercore is a protocol and p2p network for distributing and replicating static feeds of binary data. It is the low level component that [Hyperdrive](https://github.com/mafintosh/hyperdrive) is built on top off. For a detailed technical explanation on how the feed replication works see the [Hyperdrive Specification](https://github.com/mafintosh/hyperdrive/blob/master/SPECIFICATION.md)

```
npm install hypercore
```

[![build status](http://img.shields.io/travis/mafintosh/hypercore.svg?style=flat)](http://travis-ci.org/mafintosh/hypercore)

It runs both in the node and in the browser using [browserify](https://github.com/substack/browserify).

## Usage

First lets add a stream of data to hypercore

``` js
var level = require('level')
var hypercore = require('hypercore')

var db = level('hypercore.db')
var core = hypercore(db) // db can be any levelup instance

var ws = core.createWriteStream() // lets add some data

ws.write('hello')
ws.write('world')
ws.end(function () {
  // will print e1a31bb8716f0a0487377e22dbc7f0491fb47a712ac21519792a4e32cf56fb6f
  console.log('data was stored as', ws.id.toString('hex'))
})
```

Now to access it create a read stream with the same id

``` js
var rs = core.createReadStream('e1a31bb8716f0a0487377e22dbc7f0491fb47a712ac21519792a4e32cf56fb6f')

rs.on('data', function (data) {
  console.log(data.toString()) // prints 'hello' and 'world'
})
```

If we were only interested in the second block of data we can access the low-level feed instead of a read stream

``` js
var feed = core.get('e1a31bb8716f0a0487377e22dbc7f0491fb47a712ac21519792a4e32cf56fb6f')

// feeds give us easy random access
feed.get(1, function (err, block) {
  console.log(block.toString()) // prints 'world'
})
```

To start replicating this feed to other peers we need pipe a peer stream to them.

``` js
// create a peer stream to start replicating feeds to other peers
var stream = core.createPeerStream()
stream.pipe(anotherCore.createPeerStream()).pipe(stream)
```

To find other hypercore peers on the internet sharing feeds we could use a peer discovery module such as [discovery-channel](https://github.com/maxogden/discovery-channel) which uses the BitTorrent dht and multicast-dns to find peers.

``` js
// lets find some hypercore peers on the internet sharing or interested in our feed

var disc = require('discovery-channel')() // npm install discovery-channel
var net = require('net')

var id = new Buffer('e1a31bb8716f0a0487377e22dbc7f0491fb47a712ac21519792a4e32cf56fb6f', 'hex')
var server = net.createServer(onsocket)

server.listen(0, function () {
  announce()
  setInterval(announce, 10000)

  var lookup = disc.lookup(id.slice(0, 20))

  lookup.on('peer', function (ip, port) {
    onsocket(net.connect(port, ip))
  })
})

function onsocket (socket) {
  // connect the peers
  socket.pipe(core.createPeerStream()).pipe(socket)
}

function announce () {
  // discovery-channel currently only works with 20 bytes hashes
  disc.announce(id.slice(0, 20), server.address().port)
}
```

## API

#### `var core = hypercore(db)`

Create a new hypercore instance. db should be a [levelup](https://github.com/level/levelup) instance.

#### `var stream = core.createWriteStream()`

Create a new writable stream that adds a new feed and appends blocks to it.
After the stream has been ended (`finish` has been emitted) you can access `stream.id` and `stream.blocks` to get the feed metadata.

#### `var stream = core.createReadStream(id, [options])`

Create a readable stream that reads from a the feed specified by `id`. Optionally you can specify the following options:

``` js
{
  start: 0, // which block index to start reading from
  limit: Infinity // how many blocks to read
}
```

#### `var stream = core.list()`

Returns a readable stream that will emit the `id` of all feeds stored in the core.

#### `core.on('interested', id)`

When a feed is being used hypercore will emit `interested` with the corresponding feed id. You can use this to query for peers that shares this feed using an external peer discovery mechanism.

#### `core.on('uninterested', id)`

This is emitted when a feed is no longer being used.

#### `var stream = core.createPeerStream()`

Create a new peer replication duplex stream. This stream should be piped together with a remote peer's stream to the start replicating feeds.
When the stream receives a handshake from the remote peer a `handshake` event is emitted.

## Feeds

Everytime you write a stream of data to hypercore it gets added to an underlying binary feed. Feeds give you more low-level access to the data stored through the following api.

#### `var feed = core.add()`

Create a new feed. Call `feed.append` to add blocks and `feed.finalize` when you're done and ready to share this feed.

#### `var feed = core.get(id)`

Access a finalized feed by its id. By getting a feed you'll start replicating this from other peers you are connected too as well.

#### `feed.get(index, callback)`

Get a block from the the feed. If you `.get` a block index that hasn't been downloaded yet this method will wait for that block be downloaded before calling the callback.

#### `feed.append(block, [callback])`

Append a block of data to a new feed. You can only append to a feed that hasn't been finalized. Optionally you can pass in an array of blocks instead of single one to add multiple blocks at the same time.

#### `feed.finalize([callback])`

Finalize a feed. Will set `feed.id` when done. This is the `id` that identifies this feed.

#### `feed.ready([callback])`

Call this method to wait for the feed to have enough metadata to populate its internal state.
After the callback has been called `feed.blocks` is guaranteed to be populated. You *do not* have to call `feed.ready` before trying to `.get` a block. This method is just available for convenience.

#### `var blocks = feed.blocks`

Property containing the number of blocks this feed has. This is only known after at least one block has been fetched.

#### `var bool = feed.has(index)`

Boolean indicating wheather or not a block has been downloaded. Note that since this method is synchronous you have to wait for the feed to open before calling it.

## Extension API

Hypercore supports sending and receiving custom messages using an extension api.
You can use this to implement various additions to the protocol.

#### `core.use(extension)`

Use an extension to the protocol. `extension` should be a string containing the name of your extension.

#### `var bool = stream.remoteSupports(extension)`

Check if the remote stream supports an extension. Should be called after handshake has been emitted

#### `stream.send(extension, buffer)`

Send an extension message

#### `stream.on(extension, onmessage)`

Set a handler for an extension message. `onmessage` should be a function and is called with `(messageAsBuffer)` when a message for the extension is received.

An example extension would be a simple ping / pong messaging scheme

``` js
core.use('ping') // we call the extension ping

var stream = core.createPeerStream()

// connect the stream to someone else

stream.on('handshake', function () {
  if (!stream.remoteSupports('ping')) return
  stream.on('ping', function (message) {
    if (message[0] === 0) return stream.send('ping', new Buffer([1])) // send pong
    if (message[1] === 1) console.log('got pong!')
  })
  stream.send('ping', new Buffer([0])) // send ping
})
```

## License

MIT
