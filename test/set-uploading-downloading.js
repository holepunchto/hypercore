const tape = require('tape')
const create = require('./helpers/create')
const replicate = require('./helpers/replicate')

tape('setDownloading', function (t) {
  const feed = create()

  feed.append(['a', 'b'], function () {
    const clone = create(feed.key)

    clone.setDownloading(false)
    let later = false

    clone.get(0, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer.from('a'))
      t.ok(later)

      clone.ifAvailable.ready(function () {
        feed.ifAvailable.ready(function () {
          t.pass('if available drained')
          t.end()
        })
      })
    })

    replicate(feed, clone, { live: true })

    setImmediate(() => {
      later = true
      clone.setDownloading(true)
    })
  })
})

tape('setUploading', function (t) {
  const feed = create()

  feed.append(['a', 'b'], function () {
    const clone = create(feed.key)

    feed.setUploading(false)
    let later = false

    clone.get(0, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer.from('a'))
      t.ok(later)
      clone.ifAvailable.ready(function () {
        feed.ifAvailable.ready(function () {
          t.pass('if available drained')
          t.end()
        })
      })
    })

    replicate(feed, clone)

    setImmediate(() => {
      later = true
      feed.setUploading(true)
    })
  })
})
