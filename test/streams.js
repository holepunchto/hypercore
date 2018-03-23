var tape = require('tape')
var collect = require('stream-collector')
var create = require('./helpers/create')
var bufferFrom = require('buffer-from')

tape('createReadStream to createWriteStream', function (t) {
  var feed1 = create()
  var feed2 = create()

  feed1.append(['hello', 'world'], function () {
    var r = feed1.createReadStream()
    var w = feed2.createWriteStream()

    r.pipe(w).on('finish', function () {
      collect(feed2.createReadStream(), function (err, data) {
        t.error(err, 'no error')
        t.same(data, [bufferFrom('hello'), bufferFrom('world')])
        t.end()
      })
    })
  })
})

tape('createReadStream with start, end', function (t) {
  var feed = create({valueEncoding: 'utf-8'})

  feed.append(['hello', 'multiple', 'worlds'], function () {
    collect(feed.createReadStream({start: 1, end: 2}), function (err, data) {
      t.error(err, 'no error')
      t.same(data, ['multiple'])
      t.end()
    })
  })
})

tape('createReadStream with start, no end', function (t) {
  var feed = create({valueEncoding: 'utf-8'})

  feed.append(['hello', 'multiple', 'worlds'], function () {
    collect(feed.createReadStream({start: 1}), function (err, data) {
      t.error(err, 'no error')
      t.same(data, ['multiple', 'worlds'])
      t.end()
    })
  })
})

tape('createReadStream with no start, end', function (t) {
  var feed = create({valueEncoding: 'utf-8'})

  feed.append(['hello', 'multiple', 'worlds'], function () {
    collect(feed.createReadStream({end: 2}), function (err, data) {
      t.error(err, 'no error')
      t.same(data, ['hello', 'multiple'])
      t.end()
    })
  })
})

tape('createReadStream with live: true', function (t) {
  var feed = create({valueEncoding: 'utf-8'})
  var expected = ['a', 'b', 'c', 'd', 'e']

  t.plan(expected.length)

  var rs = feed.createReadStream({live: true})

  rs.on('data', function (data) {
    t.same(data, expected.shift())
  })

  rs.on('end', function () {
    t.fail('should never end')
  })

  feed.append('a', function () {
    feed.append('b', function () {
      feed.append(['c', 'd', 'e'])
    })
  })
})

tape('createReadStream with live: true after append', function (t) {
  var feed = create({valueEncoding: 'utf-8'})
  var expected = ['a', 'b', 'c', 'd', 'e']

  t.plan(expected.length)

  feed.append(['a', 'b'], function () {
    var rs = feed.createReadStream({live: true})

    rs.on('data', function (data) {
      t.same(data, expected.shift())
    })

    rs.on('end', function () {
      t.fail('should never end')
    })

    feed.append(['c', 'd', 'e'])
  })
})

tape('createReadStream with live: true and tail: true', function (t) {
  var feed = create({valueEncoding: 'utf-8'})
  var expected = ['c', 'd', 'e']

  t.plan(expected.length)

  feed.append(['a', 'b'], function () {
    var rs = feed.createReadStream({live: true, tail: true})

    rs.on('data', function (data) {
      t.same(data, expected.shift())
    })

    rs.on('end', function () {
      t.fail('should never end')
    })

    feed.append(['c', 'd', 'e'])
  })
})
