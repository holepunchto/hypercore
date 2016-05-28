var hypercore = require('./helpers/create')
var tape = require('tape')

tape('can seek to offset', function (t) {
  var feed = hypercore().createFeed()

  feed.append(['hello', 'how', 'are', 'you', 'doing', '?'])
  feed.flush(function () {
    feed.seek(9, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer('re'), '9 to 11 bytes')
      t.end()
    })
  })
})

tape('can seek twice', function (t) {
  t.plan(4)

  var feed = hypercore().createFeed()

  feed.append(['hello', 'how', 'are', 'you', 'doing', '?'])
  feed.flush(function () {
    feed.seek(9, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer('re'), '9 to 11 bytes')
    })
    feed.seek(16, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer('ing'), '16 to 19 bytes')
    })
  })
})

tape('seek waits', function (t) {
  t.plan(4)

  var feed = hypercore().createFeed()

  feed.seek(9, function (err, data) {
    t.error(err, 'no error')
    t.same(data, Buffer('re'), '9 to 11 bytes')
  })
  feed.seek(16, function (err, data) {
    t.error(err, 'no error')
    t.same(data, Buffer('ing'), '16 to 19 bytes')
  })
  feed.append(['hello'], function () {
    feed.append(['how', 'are', 'you', 'doing', '?'])
  })
})

tape('seek while replicating', function (t) {
  t.plan(4)

  var feed = hypercore().createFeed()
  var clone = hypercore().createFeed(feed.key)

  clone.seek(9, function (err, data) {
    t.error(err, 'no error')
    t.same(data, Buffer('re'), '9 to 11 bytes')
  })
  clone.seek(16, function (err, data) {
    t.error(err, 'no error')
    t.same(data, Buffer('ing'), '16 to 19 bytes')
  })

  feed.append(['hello'], function () {
    feed.append(['how', 'are', 'you', 'doing', '?'], function () {
      var stream = feed.replicate()
      stream.pipe(clone.replicate()).pipe(stream)
    })
  })
})
