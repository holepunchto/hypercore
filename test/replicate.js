var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
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

    replicate(feed, clone, {live: true})
  })
})

tape('replicate live', function (t) {
  t.plan(6)

  var feed = create()

  feed.ready(function () {
    var clone = create(feed.key)

    replicate(feed, clone, {live: true})

    feed.append('a')
    feed.append('b')
    feed.append('c')

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
  })
})

tape('download while get', function (t) {
  t.plan(10)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    // add 5 so this never finished
    clone.download({start: 0, end: 6}, function () {
      t.fail('should never happen')
    })

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
    clone.get(3, same(t, 'd'))
    clone.get(4, same(t, 'e'))

    replicate(feed, clone, {live: true})
  })
})

tape('non live', function (t) {
  t.plan(10)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    replicate(clone, feed).on('end', function () {
      clone.get(0, same(t, 'a'))
      clone.get(1, same(t, 'b'))
      clone.get(2, same(t, 'c'))
      clone.get(3, same(t, 'd'))
      clone.get(4, same(t, 'e'))
    })
  })
})

tape('non live, two way', function (t) {
  t.plan(20)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    replicate(clone, feed).on('end', function () {
      clone.get(0, same(t, 'a'))
      clone.get(1, same(t, 'b'))
      clone.get(2, same(t, 'c'))
      clone.get(3, same(t, 'd'))
      clone.get(4, same(t, 'e'))

      var clone2 = create(feed.key)

      replicate(clone, clone2).on('end', function () {
        clone2.get(0, same(t, 'a'))
        clone2.get(1, same(t, 'b'))
        clone2.get(2, same(t, 'c'))
        clone2.get(3, same(t, 'd'))
        clone2.get(4, same(t, 'e'))
      })
    })
  })
})

tape('non-live empty', function (t) {
  var feed = create()

  feed.ready(function () {
    var clone = create(feed.key)

    replicate(feed, clone).on('end', function () {
      t.same(clone.length, 0)
      t.end()
    })
  })
})

tape('basic 3-way replication', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1, {live: true})
    replicate(clone1, clone2, {live: true})

    clone1.get(0, function (err, data) {
      t.error(err, 'no error')
      t.same(data, new Buffer('a'))

      clone2.get(0, function (err) {
        t.error(err, 'no error')
        t.same(data, new Buffer('a'))
        t.end()
      })
    })
  })
})

tape('extra data + factor of two', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], function () {
    var clone1 = create(feed.key)

    replicate(feed, clone1, {live: true})

    clone1.get(1, function (err, data) {
      t.error(err, 'no error')
      t.same(data, new Buffer('b'))
      t.end()
    })
  })
})

tape('3-way another index', function (t) {
  var feed = create()

  feed.append(['a', 'b'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1, {live: true})
    replicate(clone1, clone2, {live: true})

    clone1.get(1, function (err, data) {
      t.error(err, 'no error')
      t.same(data, new Buffer('b'))

      clone2.get(1, function (err) {
        t.error(err, 'no error')
        t.same(data, new Buffer('b'))
        t.end()
      })
    })
  })
})

tape('3-way another index + extra data', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1, {live: true})
    replicate(clone1, clone2, {live: true})

    clone1.get(1, function (err, data) {
      t.error(err, 'no error')
      t.same(data, new Buffer('b'))

      clone2.get(1, function (err) {
        t.error(err, 'no error')
        t.same(data, new Buffer('b'))
        t.end()
      })
    })
  })
})

tape('3-way another index + extra data + factor of two', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1, {live: true})
    replicate(clone1, clone2, {live: true})

    clone1.get(1, function (err, data) {
      t.error(err, 'no error')
      t.same(data, new Buffer('b'))

      clone2.get(1, function (err) {
        t.error(err, 'no error')
        t.same(data, new Buffer('b'))
        t.end()
      })
    })
  })
})

tape('3-way another index + extra data + factor of two + static', function (t) {
  var feed = create({live: false})

  feed.append(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], function () {
    feed.finalize(function () {
      var clone1 = create(feed.key)
      var clone2 = create(feed.key)

      replicate(feed, clone1, {live: true})
      replicate(clone1, clone2, {live: true})

      clone1.get(1, function (err, data) {
        t.error(err, 'no error')
        t.same(data, new Buffer('b'))

        clone2.get(1, function (err) {
          t.error(err, 'no error')
          t.same(data, new Buffer('b'))
          t.end()
        })
      })
    })
  })
})

tape('seek while replicating', function (t) {
  t.plan(6)

  var feed = create()

  feed.ready(function () {
    var clone = create(feed.key)

    clone.seek(9, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 2)
      t.same(offset, 1)
    })

    clone.seek(16, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 4)
      t.same(offset, 2)
    })

    feed.append(['hello'], function () {
      feed.append(['how', 'are', 'you', 'doing', '?'], function () {
        replicate(feed, clone, {live: true})
      })
    })
  })
})

tape('non spare live replication', function (t) {
  var feed = create()

  feed.on('ready', function () {
    feed.append(['a', 'b', 'c'], function () {
      var clone = create(feed.key)

      clone.get(0, function () {
        clone.get(1, function () {
          clone.get(2, function () {
            clone.once('download', function () {
              t.pass('downloaded new block')
              t.end()
            })

            feed.append('a')
          })
        })
      })

      replicate(feed, clone, {live: true})
    })
  })
})

function same (t, val) {
  return function (err, data) {
    t.error(err, 'no error')
    t.same(data.toString(), val)
  }
}
