var create = require('./helpers/create')
var tape = require('tape')

tape('cancel', function (t) {
  t.plan(2)

  var feed = create()

  feed.get(0, function (err) {
    t.ok(err, 'expected error')
  })

  feed.get(0, function (err) {
    t.ok(err, 'expected error')
  })

  feed.cancel(0)
})

tape('cancel range', function (t) {
  t.plan(2)

  var feed = create()

  feed.get(0, function (err) {
    t.ok(err, 'expected error')
  })

  feed.get(1, function (err) {
    t.ok(err, 'expected error')
  })

  feed.get(2, function () {
    t.fail('should not error')
  })

  feed.cancel(0, 2)
})

tape('get after cancel', function (t) {
  t.plan(1)

  var feed = create()

  feed.get(0, function (err) {
    t.ok(err, 'expected error')
    feed.get(0, function () {
      t.fail('should not error')
    })
  })

  feed.cancel(0)
})

tape('cancel download', function (t) {
  var feed = create()

  feed.download({ start: 0, end: 10 }, function (err) {
    t.ok(err, 'expected error')
    t.end()
  })

  feed.cancel(0, 10)
})

tape('cancel download and get', function (t) {
  t.plan(3)

  var feed = create()

  feed.download({ start: 1, end: 9 }, function (err) {
    t.ok(err, 'expected error')
  })

  feed.get(5, function (err) {
    t.ok(err, 'expected error')
  })

  feed.get(7, function (err) {
    t.ok(err, 'expected error')
  })

  feed.cancel(0, 10)
})

tape('cancel seek', function (t) {
  var feed = create()

  feed.seek(10, { start: 0, end: 10 }, function (err) {
    t.ok(err, 'expected error')
    t.end()
  })

  feed.cancel(0, 10)
})
