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

tape('download while get', function (t) {
  t.plan(10)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    // add 5 so this never finished
    clone.download([0, 1, 2, 3, 4, 5], function () {
      t.fail('should never happen')
    })

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
    clone.get(3, same(t, 'd'))
    clone.get(4, same(t, 'e'))

    replicate(feed, clone)
  })
})

tape('basic 3-way replication', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1)
    replicate(clone1, clone2)

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

    replicate(feed, clone1)

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

    replicate(feed, clone1)
    replicate(clone1, clone2)

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

    replicate(feed, clone1)
    replicate(clone1, clone2)

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

    replicate(feed, clone1)
    replicate(clone1, clone2)

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

      replicate(feed, clone1)
      replicate(clone1, clone2)

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

tape.skip('seek while replicating', function (t) {
  t.plan(6)

  var feed = create()

  feed.ready(function () {
    var clone = create(feed.key)

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
        replicate(feed, clone)
      })
    })
  })
})

function same (t, val) {
  return function (err, data) {
    t.error(err, 'no error')
    t.same(data.toString(), val)
  }
}
