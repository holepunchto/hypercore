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
