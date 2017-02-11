var create = require('./helpers/create')
var tape = require('tape')

tape('replicate', function (t) {
  t.plan(10)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
    clone.get(3, same(t, 'd'))
    clone.get(4, same(t, 'e'))

    replicate(feed, clone)
  })
})

tape('replicate live', function (t) {
  t.plan(6)

  var feed = create()

  feed.ready(function () {
    var clone = create(feed.key)

    replicate(feed, clone)

    feed.append('a')
    feed.append('b')
    feed.append('c')

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
  })
})

function replicate (a, b) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream)
}

function same (t, val) {
  return function (err, data) {
    t.error(err, 'no error')
    t.same(data.toString(), val)
  }
}
