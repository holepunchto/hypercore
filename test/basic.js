var create = require('./helpers/create')
var tape = require('tape')

tape('append', function (t) {
  t.plan(8)

  var feed = create({valueEncoding: 'json'})

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
      t.same(value, {hello: 'world'})
    })

    feed.get(1, function (err, value) {
      t.error(err, 'no error')
      t.same(value, {hello: 'verden'})
    })

    feed.get(2, function (err, value) {
      t.error(err, 'no error')
      t.same(value, {hello: 'welt'})
    })
  })
})

tape('seek', function (t) {
  t.plan(13)

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

    feed.seek(50, function (err, index, offset) {
      t.ok(err, 'out of bounds')
    })
  })
})
