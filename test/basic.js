var create = require('./helpers/create')
var createTrackingRam = require('./helpers/create-tracking-ram')
var crypto = require('hypercore-crypto')
var tape = require('tape')
var hypercore = require('../')
var ram = require('random-access-memory')
var bufferAlloc = require('buffer-alloc-unsafe')

tape('append', function (t) {
  t.plan(8)

  var feed = create({ valueEncoding: 'json' })

  feed.append({
    hello: 'world'
  })

  feed.append([{
    hello: 'verden'
  }, {
    hello: 'welt'
  }])

  feed.flush(function () {
    t.same(feed.length, 3, '3 blocks')
    t.same(feed.byteLength, 54, '54 bytes')

    feed.get(0, function (err, value) {
      t.error(err, 'no error')
      t.same(value, { hello: 'world' })
    })

    feed.get(1, function (err, value) {
      t.error(err, 'no error')
      t.same(value, { hello: 'verden' })
    })

    feed.get(2, function (err, value) {
      t.error(err, 'no error')
      t.same(value, { hello: 'welt' })
    })
  })
})

tape('flush', function (t) {
  var feed = create()

  feed.append('hello')

  feed.flush(function (err) {
    t.error(err, 'no error')
    t.same(feed.length, 1, '1 block')
    t.end()
  })
})

tape('verify', function (t) {
  t.plan(9)

  var feed = create()
  var evilfeed = create(feed.key, { secretKey: feed.secretKey })

  feed.append('test', function (err) {
    t.error(err, 'no error')

    evilfeed.append('t\0st', function (err) {
      t.error(err, 'no error')

      feed.signature(0, function (err, sig) {
        t.error(err, 'no error')
        t.same(sig.index, 0, '0 signed at 0')

        feed.verify(0, sig.signature, function (err, success) {
          t.error(err, 'no error')
          t.ok(success)
        })

        evilfeed.verify(0, sig.signature, function (err, success) {
          t.ok(!!err)
          t.ok(err instanceof Error)
          t.ok(!success, 'fake verify failed')
        })
      })
    })
  })
})

tape('rootHashes', function (t) {
  t.plan(9)

  var feed = create()
  var evilfeed = create(feed.key, { secretKey: feed.secretKey })

  feed.append('test', function (err) {
    t.error(err, 'no error')

    evilfeed.append('t\0st', function (err) {
      t.error(err, 'no error')

      var result = []

      feed.rootHashes(0, onroots)
      evilfeed.rootHashes(0, onroots)

      function onroots (err, roots) {
        t.error(err, 'no error')
        t.ok(roots instanceof Array)
        result.push(roots)
        if (result.length < 2) return
        t.notEqual(result[0], result[1])
        t.equal(result[0].length, result[1].length)
        t.notEqual(Buffer.compare(result[0][0].hash, result[1][0].hash), 0)
      }
    })
  })
})

tape('pass in secret key', function (t) {
  var keyPair = crypto.keyPair()
  var secretKey = keyPair.secretKey
  var key = keyPair.publicKey

  var feed = create(key, { secretKey: secretKey })

  feed.on('ready', function () {
    t.same(feed.key, key)
    t.same(feed.secretKey, secretKey)
    t.ok(feed.writable)
    t.end()
  })
})

tape('check existing key', function (t) {
  var feed = hypercore(storage)

  feed.append('hi', function () {
    var key = bufferAlloc(32)
    key.fill(0)
    var otherFeed = hypercore(storage, key)
    otherFeed.on('error', function () {
      t.pass('should error')
      t.end()
    })
  })

  function storage (name) {
    if (storage[name]) return storage[name]
    storage[name] = ram()
    return storage[name]
  }
})

tape('create from existing keys', function (t) {
  t.plan(3)

  var storage1 = storage.bind(null, '1')
  var storage2 = storage.bind(null, '2')

  var feed = hypercore(storage1)

  feed.append('hi', function () {
    var otherFeed = hypercore(storage2, feed.key, { secretKey: feed.secretKey })
    var store = otherFeed._storage
    otherFeed.ready(function () {
      store.open({ key: feed.key }, function (err, data) {
        t.error(err)
        t.equals(data.key.toString('hex'), feed.key.toString('hex'))
        t.equals(data.secretKey.toString('hex'), feed.secretKey.toString('hex'))
      })
    })
  })

  function storage (prefix, name) {
    var fullname = prefix + '_' + name
    if (storage[fullname]) return storage[fullname]
    storage[fullname] = ram()
    return storage[fullname]
  }
})

tape('head', function (t) {
  t.plan(8)

  var feed = create({ valueEncoding: 'json' })

  feed.head(function (err, head) {
    t.ok(!!err)
    t.ok(err instanceof Error)
    step2()
  })

  function step2 () {
    feed.append({
      hello: 'world'
    }, function () {
      feed.head(function (err, head) {
        t.error(err)
        t.same(head, { hello: 'world' })
        step3()
      })
    })
  }

  function step3 () {
    feed.append([{
      hello: 'verden'
    }, {
      hello: 'welt'
    }], function () {
      feed.head({}, function (err, head) {
        t.error(err)
        t.same(head, { hello: 'welt' })
        step4()
      })
    })
  }

  function step4 () {
    feed.append('blender', function () {
      feed.head({ valueEncoding: 'utf-8' }, function (err, head) {
        t.error(err)
        t.same(head, '"blender"\n')
      })
    })
  }
})

tape('append, no cache', function (t) {
  t.plan(8)

  var feed = create({ valueEncoding: 'json', storageCacheSize: 0 })

  feed.append({
    hello: 'world'
  })

  feed.append([{
    hello: 'verden'
  }, {
    hello: 'welt'
  }])

  feed.flush(function () {
    t.same(feed.length, 3, '3 blocks')
    t.same(feed.byteLength, 54, '54 bytes')

    feed.get(0, function (err, value) {
      t.error(err, 'no error')
      t.same(value, { hello: 'world' })
    })

    feed.get(1, function (err, value) {
      t.error(err, 'no error')
      t.same(value, { hello: 'verden' })
    })

    feed.get(2, function (err, value) {
      t.error(err, 'no error')
      t.same(value, { hello: 'welt' })
    })
  })
})

tape('onwrite', function (t) {
  var expected = [
    { index: 0, data: 'hello', peer: null },
    { index: 1, data: 'world', peer: null }
  ]

  var feed = create({
    onwrite: function (index, data, peer, cb) {
      t.same({ index: index, data: data.toString(), peer: peer }, expected.shift())
      cb()
    }
  })

  feed.append(['hello', 'world'], function (err) {
    t.error(err, 'no error')
    t.same(expected.length, 0)
    t.end()
  })
})

tape('close, emitter and callback', function (t) {
  t.plan(3)
  var feed = create()

  feed.on('close', function () {
    t.pass('close emitted')
  })

  feed.close(function (err) {
    t.error(err, 'closed without error')
    t.pass('callback invoked')
  })

  feed.close(function () {
    t.end()
  })
})

tape('close calls pending callbacks', function (t) {
  t.plan(5)

  var feed = create()

  feed.createReadStream({ live: true })
    .once('error', function (err) {
      t.ok(err, 'read stream errors')
    })
    .resume()

  feed.get(0, function (err) {
    t.ok(err, 'get errors')
  })

  feed.once('close', function () {
    t.pass('close emitted')
  })

  feed.ready(function () {
    feed.close(function () {
      feed.createReadStream({ live: true })
        .once('error', function (err) {
          t.ok(err, 'read stream still errors')
        })
        .resume()

      feed.get(0, function (err) {
        t.ok(err, 'get still errors')
      })
    })
  })
})

tape('get batch', function (t) {
  t.plan(2 * 3)

  var feed = create({ valueEncoding: 'utf-8' })

  feed.append(['a', 'be', 'cee', 'd'], function () {
    feed.getBatch(0, 4, function (err, batch) {
      t.error(err)
      t.same(batch, ['a', 'be', 'cee', 'd'])
    })
    feed.getBatch(1, 3, function (err, batch) {
      t.error(err)
      t.same(batch, ['be', 'cee'])
    })
    feed.getBatch(2, 4, function (err, batch) {
      t.error(err)
      t.same(batch, ['cee', 'd'])
    })
  })
})

tape('append returns the seq', function (t) {
  var feed = hypercore(storage)

  feed.append('a', function (err, seq) {
    t.error(err)
    t.same(seq, 0)
    feed.append(['b', 'c'], function (err, seq) {
      t.error(err)
      t.same(seq, 1)
      feed.append(['d'], function (err, seq) {
        t.error(err)
        t.same(seq, 3)

        var reloaded = hypercore(storage)
        reloaded.append(['e'], function (err, seq) {
          t.error(err)
          t.same(seq, 4)
          t.same(reloaded.length, 5)
          t.end()
        })
      })
    })
  })

  function storage (name) {
    if (storage[name]) return storage[name]
    storage[name] = ram()
    return storage[name]
  }
})

tape('append and createWriteStreams preserve seq', function (t) {
  var feed = create()

  var ws = feed.createWriteStream()

  ws.write('a')
  ws.write('b')
  ws.write('c')
  ws.end(function () {
    t.same(feed.length, 3)
    feed.append('d', function (err, seq) {
      t.error(err)
      t.same(seq, 3)
      t.same(feed.length, 4)

      var ws1 = feed.createWriteStream()

      ws1.write('e')
      ws1.write('f')
      ws1.end(function () {
        feed.append('g', function (err, seq) {
          t.error(err)
          t.same(seq, 6)
          t.same(feed.length, 7)
          t.end()
        })
      })
    })
  })
})

tape('closing all streams on close', function (t) {
  var memories = {}
  var feed = hypercore(function (filename) {
    var memory = memories[filename]
    if (!memory) {
      memory = ram()
      memories[filename] = memory
    }
    return memory
  })
  var expectedFiles = ['key', 'secret_key', 'tree', 'data', 'bitfield', 'signatures']
  feed.ready(function () {
    t.deepEquals(Object.keys(memories), expectedFiles, 'all files are open')
    feed.close(function () {
      expectedFiles.forEach(function (filename) {
        var memory = memories[filename]
        t.ok(memory.closed, filename + ' is closed')
      })
      t.end()
    })
  })
})

tape('writes are batched', function (t) {
  var trackingRam = createTrackingRam()
  var feed = hypercore(trackingRam)
  var ws = feed.createWriteStream()

  ws.write('ab')
  ws.write('cd')
  setImmediate(function () {
    ws.write('ef')
    ws.write('gh')
    ws.end(function () {
      t.deepEquals(trackingRam.log.data, [
        { write: [0, Buffer.from('abcd')] },
        { write: [4, Buffer.from('efgh')] }
      ])
      feed.close(function () {
        t.end()
      })
    })
  })
})

tape('cancel get', function (t) {
  var feed = create()
  var cancelled = false

  const get = feed.get(42, function (err) {
    t.ok(cancelled, 'was cancelled')
    t.ok(err, 'had error')
    t.end()
  })

  setImmediate(function () {
    cancelled = true
    feed.cancel(get)
  })
})

tape('onwait', function (t) {
  t.plan(2)

  var feed = create()

  feed.append('a', function () {
    feed.get(0, {
      onwait () {
        t.fail('no onwait')
      }
    }, function () {
      t.ok('should call cb')
    })

    feed.get(42, {
      onwait () {
        t.ok('should wait')
      }
    }, function () {
      t.fail('no cb')
    })
  })
})
