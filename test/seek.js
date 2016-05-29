var hypercore = require('./helpers/create')
var tape = require('tape')

tape('can seek to byte offset', function (t) {
  var feed = hypercore().createFeed()

  feed.append(['hello', 'how', 'are', 'you', 'doing', '?'])

  feed.flush(function () {
    feed.seek(9, function (err, block, offset) {
      t.error(err, 'no error')
      t.same(block, 2)
      t.same(offset, 1)
      t.end()
    })
  })
})

tape('can seek twice', function (t) {
  t.plan(6)

  var feed = hypercore().createFeed()

  feed.append(['hello', 'how', 'are', 'you', 'doing', '?'])

  feed.flush(function () {
    feed.seek(9, function (err, block, offset) {
      t.error(err, 'no error')
      t.same(block, 2)
      t.same(offset, 1)
    })

    feed.seek(16, function (err, block, offset) {
      t.error(err, 'no error')
      t.same(block, 4)
      t.same(offset, 2)
    })
  })
})

tape('seek waits', function (t) {
  t.plan(6)

  var feed = hypercore().createFeed()

  feed.seek(9, function (err, block, offset) {
    t.error(err, 'no error')
    t.same(block, 2)
    t.same(offset, 1)
  })

  feed.seek(16, function (err, block, offset) {
    t.error(err, 'no error')
    t.same(block, 4)
    t.same(offset, 2)
  })

  feed.append(['hello'], function () {
    feed.append(['how', 'are', 'you', 'doing', '?'])
  })
})

tape('seek while replicating', function (t) {
  t.plan(6)

  var feed = hypercore().createFeed()
  var clone = hypercore().createFeed(feed.key)

  clone.seek(9, function (err, block, offset) {
    t.error(err, 'no error')
    t.same(block, 2)
    t.same(offset, 1)
  })

  clone.seek(16, function (err, block, offset) {
    t.error(err, 'no error')
    t.same(block, 4)
    t.same(offset, 2)
  })

  feed.append(['hello'], function () {
    feed.append(['how', 'are', 'you', 'doing', '?'], function () {
      var stream = feed.replicate()
      stream.pipe(clone.replicate()).pipe(stream)
    })
  })
})
