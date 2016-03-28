var tape = require('tape')
var memdb = require('memdb')
var hypercore = require('../')

tape('replicate non live', function (t) {
  var core1 = create()
  var core2 = create()

  var feed = core1.createFeed({live: false})

  feed.append('hello')
  feed.append('world')
  feed.finalize(function () {
    var clone = core2.createFeed(feed.key)
    var missing = 2

    replicate(clone, feed)

    clone.on('download', function (block) {
      t.same(clone.blocks, 2, 'should be 2 blocks')
      if (block > 2) t.fail('unknown block')
      missing--
    })

    clone.on('synchronized', function () {
      t.same(missing, 0, 'no missing blocks')
      t.end()
    })
  })
})

tape('replicate live', function (t) {
  var core1 = create()
  var core2 = create()

  var feed = core1.createFeed()

  feed.append('hello')
  feed.append('world')
  feed.finalize(function () {
    var clone = core2.createFeed(feed.key)
    var missing = 2

    replicate(clone, feed)

    clone.on('download', function (block) {
      t.same(clone.blocks, 2, 'should be 2 blocks')
      if (block > 2) t.fail('unknown block')
      missing--
    })

    clone.on('synchronized', function () {
      t.same(missing, 0, 'no missing blocks')
      t.end()
    })
  })
})

tape('replicate live with append', function (t) {
  var core1 = create()
  var core2 = create()

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
            t.end()
          })
        })
      })
    })
  }
})

tape('replicate live with append + early get', function (t) {
  t.plan(8)

  var core1 = create()
  var core2 = create()

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

tape('same peer-id across streams', function (t) {
  var core = create()

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

  var core = create()
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

function replicate (a, b) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream)
}

function create () {
  return hypercore(memdb())
}
