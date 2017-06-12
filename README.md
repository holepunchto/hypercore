# hypercore

Hypercore is a secure, distributed append-only log.

Built for sharing large datasets and streams of real time data as part of the [Dat project](https://datproject.org).

``` sh
npm install hypercore
```

[![Build Status](https://travis-ci.org/mafintosh/hypercore.svg?branch=master)](https://travis-ci.org/mafintosh/hypercore)

To learn more about how hypercore works on a technical level read the [Dat paper](https://github.com/datproject/docs/blob/master/papers/dat-paper.pdf).

## Features

* Sparse replication. Only download the data you are interested in.
* Realtime. Get the latest updates to the log fast and securely.
* Performant. Uses a simple flat file structure to maximize I/O performance.
* Secure. Uses signed merkle trees to verify log integrity in real time.
* Browser support. Simply pick a storage provider (like [random-access-memory](https://github.com/mafintosh/random-access-memory)) that works in the browser

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

## API

#### `var feed = hypercore(storage, [key], [options])`

Create a new hypercore feed.

`storage` should be set to a directory where you want to store the data and feed metadata.

``` js
var feed = hypercore('./directory') // store data in ./directory
```

Alternatively you can pass a function instead that is called with every filename hypercore needs to function and return your own [random-access](https://github.com/juliangruber/abstract-random-access) instance that is used to store the data.

``` js
var ram = require('random-access-memory')
var feed = hypercore(function (filename) {
  // filename will be one of: data, bitfield, tree, signatures, key, secret_key
  // the data file will contain all your data concattenated.

  // just store all files in ram by returning a random-access-memory instance
  return ram()
})
```

Per default hypercore uses [random-access-file](https://github.com/mafintosh/random-access-file). This is also useful if you want to store specific files in other directories. For example you might want to store the secret key elsewhere.

`key` can be set to a hypercore feed public key. If you do not set this the public key will be loaded from storage. If no key exists a new key pair will be generated.

`options` include:

``` js
{
  createIfMissing: true, // create a new hypercore key pair if none was present in storage
  overwrite: false, // overwrite any old hypercore that might already exist
  valueEncoding: 'json' | 'utf-8' | 'binary', // defaults to binary
  sparse: false, // do not mark the entire feed to be downloaded
  secretKey: buffer // optionally pass the corresponding secret key yourself
  storeSecretKey: true // if false, will not save the secret key
}
```

You can also set valueEncoding to any [abstract-encoding](https://github.com/mafintosh/abstract-encoding) instance.

#### `feed.writable`

Can we append to this feed?

Populated after `ready` has been emitted. Will be `false` before the event.

#### `feed.readable`

Can we read from this feed? After closing a feed this will be false.

Populated after `ready` has been emitted. Will be `false` before the event.

#### `feed.key`

Buffer containing the public key identifying this feed.

Populated after `ready` has been emitted. Will be `null` before the event.

#### `feed.discoveryKey`

Buffer containing a key derived from the feed.key.
In contrast to `feed.key` this key does not allow you to verify the data but can be used to announce or look for peers that are sharing the same feed, without leaking the feed key.

Populated after `ready` has been emitted. Will be `null` before the event.

#### `feed.length`

How many blocks of data are available on this feed?

Populated after `ready` has been emitted. Will be `0` before the event.

#### `feed.byteLength`

How much data is available on this feed in bytes?

Populated after `ready` has been emitted. Will be `0` before the event.

#### `feed.get(index, [options], callback)`

Get a block of data.
If the data is not available locally this method will prioritize and wait for the data to be downloaded before calling the callback.

Options include

``` js
{
  wait: true, // wait for index to be downloaded
  timeout: 0, // wait at max some milliseconds (0 means no timeout)
  valueEncoding: 'json' | 'utf-8' | 'binary' // defaults to the feed's valueEncoding
}
```

Callback is called with `(err, data)`

#### `feed.download([range], [callback])`

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
feed will be marked for download for you when the feed is created.

#### `feed.undownload(range)`

Cancel a previous download request.

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

#### `feed.append(data, [callback])`

Append a block of data to the feed.

Callback is called with `(err)` when all data has been written or an error occured.

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
  wait: true // wait for data to be downloaded
}
```

#### `var stream = feed.createWriteStream()`

Create a writable stream.

#### `var stream = feed.replicate([options])`

Create a replication stream. You should pipe this to another hypercore instance.

``` js
// assuming we have two feeds, localFeed + remoteFeed, sharing the same key
// on a server
var net = require('net')
var server = net.createServer(function (socket) {
  socket.pipe(remoteFeed.replicate()).pipe(socket)
})

// on a client
var socket = net.connect(...)
socket.pipe(localFeed.replicate()).pipe(socket)
```

Options include:

``` js
{
  live: false, // keep replicating after all remote data has been downloaded?
  download: true, // download data from peers?
  encrypt: true // encrypt the data sent using the hypercore key pair
}
```

#### `feed.close([callback])`

Fully close this feed.

Calls the callback with `(err)` when all storage has been closed.

#### `feed.on('ready')`

Emitted when the feed is ready and all properties have been populated.

#### `feed.on('error', err)`

Emitted when the feed experiences a critical error.

#### `feed.on('download', index, data)`

Emitted when a data block has been downloaded.

#### `feed.on('upload', index, data)`

Emitted when a data block is uploaded.

#### `feed.on('append')`

Emitted when the feed has been appended to (i.e. has a new length / byteLength)

#### `feed.on('sync')`

Emitted everytime ALL data from `0` to `feed.length` has been downloaded.

#### `feed.on('close')`

Emitted when the feed has been fully closed

## License

MIT
