const tape = require('tape')
const collect = require('stream-collector')
const create = require('./helpers/create')
const bufferFrom = require('buffer-from')

function test (batch = 1) {
  tape('createReadStream to createWriteStream', function (t) {
    const feed1 = create()
    const feed2 = create()

    feed1.append(['hello', 'world'], function () {
      const r = feed1.createReadStream({ batch })
      const w = feed2.createWriteStream()

      r.pipe(w).on('finish', function () {
        collect(feed2.createReadStream({ batch }), function (err, data) {
          t.error(err, 'no error')
          t.same(data, [bufferFrom('hello'), bufferFrom('world')])
          t.end()
        })
      })
    })
  })

  tape('createReadStream with start, end', function (t) {
    const feed = create({ valueEncoding: 'utf-8' })

    feed.append(['hello', 'multiple', 'worlds'], function () {
      collect(feed.createReadStream({ start: 1, end: 2, batch }), function (err, data) {
        t.error(err, 'no error')
        t.same(data, ['multiple'])
        t.end()
      })
    })
  })

  tape('createReadStream with start, no end', function (t) {
    const feed = create({ valueEncoding: 'utf-8' })

    feed.append(['hello', 'multiple', 'worlds'], function () {
      collect(feed.createReadStream({ start: 1, batch }), function (err, data) {
        t.error(err, 'no error')
        t.same(data, ['multiple', 'worlds'])
        t.end()
      })
    })
  })

  tape('createReadStream with no start, end', function (t) {
    const feed = create({ valueEncoding: 'utf-8' })

    feed.append(['hello', 'multiple', 'worlds'], function () {
      collect(feed.createReadStream({ end: 2, batch }), function (err, data) {
        t.error(err, 'no error')
        t.same(data, ['hello', 'multiple'])
        t.end()
      })
    })
  })

  tape('createReadStream with live: true', function (t) {
    const feed = create({ valueEncoding: 'utf-8' })
    const expected = ['a', 'b', 'c', 'd', 'e']

    t.plan(expected.length)

    const rs = feed.createReadStream({ live: true, batch })

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
    const feed = create({ valueEncoding: 'utf-8' })
    const expected = ['a', 'b', 'c', 'd', 'e']

    t.plan(expected.length)

    feed.append(['a', 'b'], function () {
      const rs = feed.createReadStream({ live: true, batch })

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
    const feed = create({ valueEncoding: 'utf-8' })
    const expected = ['c', 'd', 'e']

    t.plan(expected.length)

    feed.append(['a', 'b'], function () {
      const rs = feed.createReadStream({ live: true, tail: true, batch })

      rs.on('data', function (data) {
        t.same(data, expected.shift())
      })

      rs.on('end', function () {
        t.fail('should never end')
      })

      setImmediate(function () {
        feed.append(['c', 'd', 'e'])
      })
    })
  })
}

tape('createWriteStream with maxBlockSize', function (t) {
  t.plan(11 * 2 + 1)

  const feed = create()

  const ws = feed.createWriteStream({ maxBlockSize: 100 * 1024 })

  ws.write(Buffer.alloc(1024 * 1024))
  ws.end(function () {
    t.same(feed.length, 11)

    sameSize(0, 100 * 1024)
    sameSize(1, 100 * 1024)
    sameSize(2, 100 * 1024)
    sameSize(3, 100 * 1024)
    sameSize(4, 100 * 1024)
    sameSize(5, 100 * 1024)
    sameSize(6, 100 * 1024)
    sameSize(7, 100 * 1024)
    sameSize(8, 100 * 1024)
    sameSize(9, 100 * 1024)
    sameSize(10, 1024 * 1024 - 10 * 100 * 1024)

    function sameSize (idx, size) {
      feed.get(idx, function (err, blk) {
        t.error(err, 'no error')
        t.same(blk.length, size)
      })
    }
  })
})

test()
test(10)
