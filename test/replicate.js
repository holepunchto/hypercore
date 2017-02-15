var tape = require('tape')
var ram = require('random-access-memory')
var hypercore = require('./helpers/create')

tape('replicate non live', function (t) {
  var core1 = hypercore()
  var core2 = hypercore()

  var feed = core1.createFeed({live: false})

  feed.append('hello')
  feed.append('world')
  feed.finalize(function () {
    var clone = core2.createFeed(feed.key)
    var missing = 2

    replicate(clone, feed)

    clone.on('download', function (block) {
      t.same(clone.blocks, 2, 'should be 2 blocks')
      t.same(clone.bytes, 10, 'should be 10 bytes')
      if (block >= 2) t.fail('unknown block')
      if (!--missing) t.end()
    })
  })
})

tape('replicate non live, bigger', function (t) {
  var core1 = hypercore()
  var core2 = hypercore()

  var feed = core1.createFeed({live: false})
  var expectedBytes = 0

  for (var i = 0; i < 1000; i++) {
    var msg = Buffer('#' + i)
    expectedBytes += msg.length
    feed.append(msg)
  }

  feed.finalize(function () {
    var clone = core2.createFeed(feed.key)
    var missing = 1000

    replicate(clone, feed)

    clone.on('download', function (block) {
      if (missing === 1000) {
        t.same(clone.blocks, 1000, 'should be 1000 blocks')
        t.same(clone.bytes, expectedBytes, 'should be ' + expectedBytes + ' bytes')
      }
      if (block >= 1000) t.fail('unknown block')
      if (!--missing) t.end()
    })
  })
})

tape('replicate non live, bigger with external storage', function (t) {
  var core1 = hypercore()
  var core2 = hypercore()

  var feed = core1.createFeed({live: false, storage: ram()})

  for (var i = 0; i < 1000; i++) {
    feed.append('#' + i)
  }

  feed.finalize(function () {
    var clone = core2.createFeed(feed.key, {storage: ram()})
    var missing = 1000

    replicate(clone, feed)

    clone.on('download', function (block) {
      if (missing === 1000) t.same(clone.blocks, 1000, 'should be 1000 blocks')
      if (block >= 1000) t.fail('unknown block')
      if (!--missing) t.end()
    })
  })
})

tape('replicate live', function (t) {
  var core1 = hypercore()
  var core2 = hypercore()

  var feed = core1.createFeed()

  feed.append('hello')
  feed.append('world')
  feed.finalize(function () {
    var clone = core2.createFeed(feed.key)
    var missing = 2

    replicate(clone, feed)

    clone.on('download', function (block) {
      t.same(clone.blocks, 2, 'should be 2 blocks')
      if (block >= 2) t.fail('unknown block')
      if (!--missing) t.end()
    })
  })
})

tape('replicate live with append', function (t) {
  var core1 = hypercore()
  var core2 = hypercore()

  var feed = core1.createFeed()

  feed.append('hello')
  feed.append('world')
  feed.flush(function () {
    var clone = core2.createFeed(feed.key)
    var missing = 2
    var twice = false

    clone.on('download', function (block) {
      missing--
      t.ok(missing >= 0, 'downloading expected block')
      if (missing) return

      if (twice) return validate(clone)
      twice = true
      missing = 2
      feed.append(['hej', 'verden'])
    })

    replicate(clone, feed)
  })

  function validate (clone) {
    clone.get(0, function (_, data) {
      t.same(data, Buffer('hello'))
      clone.get(1, function (_, data) {
        t.same(data, Buffer('world'))
        clone.get(2, function (_, data) {
          t.same(data, Buffer('hej'))
          clone.get(3, function (_, data) {
            t.same(data, Buffer('verden'))
            t.same(clone.bytes, 19, '19 bytes')
            t.end()
          })
        })
      })
    })
  }
})

tape('replicate live with append + early get', function (t) {
  t.plan(8)

  var core1 = hypercore()
  var core2 = hypercore()

  var feed = core1.createFeed()

  feed.append('hello')
  feed.append('world')
  feed.flush(function () {
    var clone = core2.createFeed(feed.key)
    var missing = 2
    var twice = false

    validate(clone)

    clone.on('download', function (block) {
      missing--
      t.ok(missing >= 0, 'downloading expected block')
      if (missing) return

      if (twice) return
      twice = true
      missing = 2
      feed.append(['hej', 'verden'])
    })

    replicate(clone, feed)
  })

  function validate (clone) {
    clone.get(0, function (_, data) {
      t.same(data, Buffer('hello'))
    })
    clone.get(1, function (_, data) {
      t.same(data, Buffer('world'))
    })
    clone.get(2, function (_, data) {
      t.same(data, Buffer('hej'))
    })
    clone.get(3, function (_, data) {
      t.same(data, Buffer('verden'))
    })
  }
})

tape('replicate + reload live feed', function (t) {
  var feed = hypercore().createFeed()
  var clone = hypercore()
  var clonedFeed = clone.createFeed(feed.key)

  replicate(feed, clonedFeed)

  feed.append('hello', function () {
    clonedFeed.get(0, function () {
      t.same(clonedFeed.blocks, 1)
      t.ok(clonedFeed.live)

      clonedFeed.close(function () {
        var restoredFeed = clone.createFeed(feed.key)

        restoredFeed.open(function () {
          t.same(restoredFeed.blocks, 1)
          t.ok(restoredFeed.live)
          t.end()
        })
      })
    })
  })
})

tape('same peer-id across streams', function (t) {
  var core = hypercore()

  var stream1 = core.replicate()
  var stream2 = core.replicate()
  var feed = core.createFeed()
  var stream3 = feed.replicate()

  t.same(core.id, stream1.id, 'peer-id exposed on the core')
  t.same(stream1.id, stream2.id, 'same peer-id')
  t.same(stream2.id, stream3.id, 'same peer-id')
  t.same(stream1.id, stream3.id, 'same peer-id')
  t.end()
})

tape('same remote peer-id across streams', function (t) {
  t.plan(2)

  var core = hypercore()
  var feed = core.createFeed()
  var stream1 = feed.replicate()
  var stream2 = feed.replicate()

  stream1.on('handshake', function () {
    t.same(stream1.id, stream1.remoteId, 'connected to self')
  })

  stream2.on('handshake', function () {
    t.same(stream2.id, stream2.remoteId, 'connected to self')
  })

  stream1.pipe(stream2).pipe(stream1)
})

tape('replicate live sparse with prioritization', function (t) {
  var core1 = hypercore()
  var core2 = hypercore()

  var feed = core1.createFeed()

  feed.append('hello')
  feed.append('world')
  feed.append('again')
  feed.finalize(function () {
    var clone = core2.createFeed(feed.key, {
      sparse: true
    })

    replicate(clone, feed)

    clone.on('download', function (block) {
      t.same(clone.blocks, 3, 'should be 3 blocks')
      if (block !== 2) return t.fail('unknown block')
      t.pass('correct block')
      t.end()
    })
    clone.prioritize({start: 2, end: 3})
  })
})

tape('replicate live sparse with download', function (t) {
  var core1 = hypercore()
  var core2 = hypercore()

  var feed = core1.createFeed()

  feed.append('hello')
  feed.append('world')
  feed.append('again')
  feed.finalize(function () {
    var clone = core2.createFeed(feed.key, {
      sparse: true
    })

    replicate(clone, feed)

    clone.on('download', function (block) {
      t.same(clone.blocks, 3, 'should be 3 blocks')
      if (block !== 2) return t.fail('unknown block')
      t.pass('correct block')
      t.end()
    })
    clone.get(2, function (_, data) {
      t.same(data.toString(), 'again', 'correct string')
    })
  })
})

tape('replicate live sparse with prioritization', function (t) {
  var core1 = hypercore()
  var core2 = hypercore()

  var feed = core1.createFeed()

  feed.append('hello')
  feed.append('world')
  feed.append('again')
  feed.finalize(function () {
    var clone = core2.createFeed(feed.key, {
      sparse: true
    })

    replicate(clone, feed)

    clone.on('download', function (block) {
      t.same(clone.blocks, 3, 'should be 3 blocks')
      if (block !== 2) return t.fail('unknown block')
      t.pass('correct block')
      t.end()
    })
    clone.prioritize({start: 2, end: 3})
  })
})

function replicate (a, b) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream)
}
