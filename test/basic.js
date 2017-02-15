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

tape('flush', function (t) {
  var feed = create()

  feed.append('hello')

  feed.flush(function (err) {
    t.error(err, 'no error')
    t.same(feed.length, 1, '1 block')
    t.end()
  })
})
