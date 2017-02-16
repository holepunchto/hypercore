var create = require('./helpers/create')
var tape = require('tape')

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
