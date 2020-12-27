var create = require('./helpers/create')
var tape = require('tape')
var replicate = require('./helpers/replicate')

tape('seek to byte offset', function (t) {
  var feed = create()

  feed.append(['hello', 'how', 'are', 'you', 'doing', '?'])

  feed.flush(function () {
    feed.seek(9, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 2)
      t.same(offset, 1)
      t.end()
    })
  })
})

tape('seek twice', function (t) {
  t.plan(6)

  var feed = create()

  feed.append(['hello', 'how', 'are', 'you', 'doing', '?'])

  feed.flush(function () {
    feed.seek(9, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 2)
      t.same(offset, 1)
    })

    feed.seek(16, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 4)
      t.same(offset, 2)
    })
  })
})

tape('seek many times', function (t) {
  t.plan(12)

  var feed = create()

  feed.append(['foo', 'b', 'ar', 'baz'], function () {
    feed.seek(0, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 0)
      t.same(offset, 0)
    })

    feed.seek(2, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 0)
      t.same(offset, 2)
    })

    feed.seek(4, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 2)
      t.same(offset, 0)
    })

    feed.seek(5, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 2)
      t.same(offset, 1)
    })
  })
})

tape('seek waits', function (t) {
  t.plan(6)

  var feed = create()

  feed.seek(9, function (err, index, offset) {
    t.error(err, 'no error')
    t.same(index, 2)
    t.same(offset, 1)
  })

  feed.seek(16, function (err, index, offset) {
    t.error(err, 'no error')
    t.same(index, 4)
    t.same(offset, 2)
  })

  feed.append(['hello'], function () {
    feed.append(['how', 'are', 'you', 'doing', '?'])
  })
})

tape('seek works for sparse trees', function (t) {
  var feed = create()

  feed.append('aa', function () {
    var clone = create(feed.key, { sparse: true })

    replicate(feed, clone, { live: true })

    clone.get(0, function () { // make sure we have a tree rooted at 0
      const chunks = Array(15)
      chunks.fill('aa')
      feed.append(chunks, function () {
        clone.get(15, function () { // get an updated tree that is disconnected with the prev one
          clone.seek(1, function (err, index, offset) { // old seek still works
            t.error(err, 'no error')
            t.same(index, 0)
            t.same(offset, 1)
            clone.seek(8, function (err, index, offset) {
              t.error(err, 'no error')
              t.same(index, 4)
              t.same(offset, 0)
              t.end()
            })
          })
        })
      })
    })
  })
})

tape('seek to sibling', function (t) {
  t.plan(9)

  var feed = create()

  feed.append(['aa', 'aa'], function () {
    feed.seek(2, function (err, index, offset) { // sibling seek
      t.error(err, 'no error')
      t.same(index, 1)
      t.same(offset, 0)
    })
    feed.seek(3, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 1)
      t.same(offset, 1)
    })
    feed.seek(1, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 0)
      t.same(offset, 1)
    })
  })
})

tape('seek to 0 and byteLength', function (t) {
  t.plan(6)

  var feed = create()

  feed.append(['a', 'b', 'c'], function () {
    feed.seek(0, function (err, index, offset) {
      t.same(err, null)
      t.same(index, 0)
      t.same(offset, 0)
    })

    feed.seek(feed.byteLength, function (err, index, offset) {
      t.same(err, null)
      t.same(index, feed.length)
      t.same(offset, 0)
    })
  })
})

tape('seek ifAvailable', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c'], function () {
    var clone = create(feed.key, { sparse: true })

    replicate(feed, clone, { live: true })

    clone.seek(4, { ifAvailable: true }, function (err) {
      t.ok(err, 'should error')
      clone.seek(2, { ifAvailable: true }, function (err, index, offset) {
        t.error(err, 'no error')
        t.same(index, 2)
        t.same(offset, 0)
        t.end()
      })
    })
  })
})

tape('seek ifAvailable multiple peers', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c'], function () {
    var clone1 = create(feed.key, { sparse: true })
    var clone2 = create(feed.key, { sparse: true })

    replicate(feed, clone1, { live: true })
    replicate(clone1, clone2, { live: true })

    clone2.seek(2, { ifAvailable: true }, function (err) {
      t.ok(err, 'should error')
      clone1.get(2, function () {
        clone2.seek(2, { ifAvailable: true }, function (err, index, offset) {
          t.error(err, 'no error')
          t.same(index, 2)
          t.same(offset, 0)
          t.end()
        })
      })
    })
  })
})

tape('seek ifAvailable with many inflight requests', function (t) {
  var feed = create()

  var arr = new Array(100).fill('a')

  feed.append(arr, function () {
    var clone = create(feed.key, { sparse: true })

    replicate(feed, clone, { live: true })

    // Create 100 inflight requests.
    for (let i = 0; i < 100; i++) clone.get(i, () => {})

    clone.seek(2, { ifAvailable: true }, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 2)
      t.same(offset, 0)
      t.end()
    })
  })
})
