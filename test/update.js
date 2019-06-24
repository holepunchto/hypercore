const tape = require('tape')
const create = require('./helpers/create')

tape('update', function (t) {
  const feed = create()

  feed.ready(function () {
    const clone = create(feed.key, { sparse: true, eagerUpdate: true })

    const s = clone.replicate({ live: true })
    s.pipe(feed.replicate({ live: true })).pipe(s)

    clone.once('append', function () {
      t.same(clone.length, 4, 'did an eager update')
      t.end()
    })

    feed.append([ 'hi', 'ho', 'hi', 'ho' ])
  })
})

tape('disable eager update', function (t) {
  const feed = create()

  feed.ready(function () {
    const clone = create(feed.key, { sparse: true, eagerUpdate: false })

    const s = clone.replicate({ live: true })
    s.pipe(feed.replicate({ live: true })).pipe(s)

    clone.once('append', function () {
      t.fail('should not update')
    })

    feed.append([ 'hi', 'ho', 'hi', 'ho' ], function () {
      setTimeout(() => t.end(), 50)
    })
  })
})

tape('update if available', function (t) {
  const feed = create()

  feed.append([ 'a', 'b', 'c' ], function () {
    const clone = create(feed.key, { sparse: true })

    const s = clone.replicate({ live: true })
    s.pipe(feed.replicate({ live: true })).pipe(s)

    clone.update({ ifAvailable: true }, function (err) {
      t.error(err, 'no error')
      t.same(clone.length, feed.length, 'was updated')
      t.end()
    })
  })
})

tape('update if available (no peers)', function (t) {
  const feed = create()

  feed.append([ 'a', 'b', 'c' ], function () {
    const clone = create(feed.key, { sparse: true })

    clone.update({ ifAvailable: true }, function (err) {
      t.ok(err)
      t.same(clone.length, 0, 'was not updated')
      t.end()
    })
  })
})

tape('update if available (no one has it)', function (t) {
  const feed = create()

  feed.append([ 'a', 'b', 'c' ], function () {
    const clone = create(feed.key, { sparse: true })

    const s = clone.replicate({ live: true })
    s.pipe(feed.replicate({ live: true })).pipe(s)

    clone.update({ ifAvailable: true, minLength: 4 }, function (err) {
      t.ok(err)
      t.same(clone.length, 0, 'was not updated')
      t.end()
    })
  })
})

tape('update with block data', function (t) {
  const feed = create()

  feed.append([ 'a', 'b', 'c', 'd' ], function () {
    const clone = create(feed.key, { sparse: true })

    const s = clone.replicate({ live: true })
    s.pipe(feed.replicate({ live: true })).pipe(s)

    clone.update({ hash: false }, function () {
      t.ok(clone.has(0))
      t.end()
    })
  })
})
