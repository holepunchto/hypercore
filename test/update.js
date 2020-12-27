const tape = require('tape')
const create = require('./helpers/create')
const replicate = require('./helpers/replicate')

tape('update', function (t) {
  const feed = create()

  feed.ready(function () {
    const clone = create(feed.key, { sparse: true, eagerUpdate: true })

    replicate(feed, clone, { live: true })

    clone.once('append', function () {
      t.same(clone.length, 4, 'did an eager update')
      t.end()
    })

    feed.append(['hi', 'ho', 'hi', 'ho'])
  })
})

tape('disable eager update', function (t) {
  const feed = create()

  feed.ready(function () {
    const clone = create(feed.key, { sparse: true, eagerUpdate: false })

    replicate(feed, clone, { live: true })

    clone.once('append', function () {
      t.fail('should not update')
    })

    feed.append(['hi', 'ho', 'hi', 'ho'], function () {
      setTimeout(() => t.end(), 50)
    })
  })
})

tape('update if available', function (t) {
  const feed = create()

  feed.append(['a', 'b', 'c'], function () {
    const clone = create(feed.key, { sparse: true })

    replicate(feed, clone, { live: true })

    clone.update({ ifAvailable: true }, function (err) {
      t.error(err, 'no error')
      t.same(clone.length, feed.length, 'was updated')
      t.end()
    })
  })
})

tape('update if available (no peers)', function (t) {
  const feed = create()

  feed.append(['a', 'b', 'c'], function () {
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

  feed.append(['a', 'b', 'c'], function () {
    const clone = create(feed.key, { sparse: true })

    replicate(feed, clone, { live: true })

    clone.update({ ifAvailable: true, minLength: 4 }, function (err) {
      t.ok(err)
      t.same(clone.length, 0, 'was not updated')
      t.end()
    })
  })
})

tape('update if available through top-level option', function (t) {
  const feed = create()

  feed.append(['a', 'b', 'c'], function () {
    const clone = create(feed.key, { sparse: true, ifAvailable: true })

    replicate(feed, clone, { live: true })

    clone.update({ minLength: 4 }, function (err) {
      t.ok(err)
      t.same(clone.length, 0, 'was not updated')
      t.end()
    })
  })
})

tape('update with block data', function (t) {
  const feed = create()

  feed.append(['a', 'b', 'c', 'd'], function () {
    const clone = create(feed.key, { sparse: true })

    replicate(feed, clone, { live: true })

    clone.update({ hash: false }, function () {
      t.ok(clone.has(3))
      t.end()
    })
  })
})

tape('update without hash option should not download block', function (t) {
  const feed = create()

  feed.append(['a', 'b', 'c'], function () {
    const clone = create(feed.key, { sparse: true })

    replicate(feed, clone, { live: true })

    clone.on('download', function (index, data) {
      t.fail('should not trigger a download event')
    })

    clone.update({ ifAvailable: true }, function (err) {
      t.error(err, 'no error')
      t.same(clone.length, 3, 'was updated')
      t.same(clone.downloaded(), 0, 'block was not downloaded')
      t.end()
    })
  })
})

tape('update with block data', function (t) {
  const feed = create()

  feed.append(['a', 'b', 'c', 'd'], function () {
    const clone1 = create(feed.key, { sparse: true })
    const clone2 = create(feed.key, { sparse: true })

    replicate(feed, clone1, { live: true })
    replicate(clone1, clone2, { live: true })

    clone1.get(3, () => {
      clone2.update(function () {
        t.same(clone1.length, clone2.length)
        t.same(clone1.length, feed.length)
        t.end()
      })
    })
  })
})
