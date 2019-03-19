var tape = require('tape')
var create = require('./helpers/create')

tape('basic audit', function (t) {
  var feed = create()

  feed.append('hello')
  feed.append('world', function () {
    feed.audit(function (err, report) {
      t.error(err, 'no error')
      t.same(report, { valid: 2, invalid: 0 })
      t.end()
    })
  })
})

tape('basic audit with bad data', function (t) {
  var feed = create()

  feed.append('hello')
  feed.append('world', function () {
    feed._storage.data.write(0, Buffer.from('H'), function () {
      t.ok(feed.has(0))
      feed.audit(function (err, report) {
        t.error(err, 'no error')
        t.same(report, { valid: 1, invalid: 1 })
        t.notOk(feed.has(0))
        feed.audit(function (err, report) {
          t.error(err, 'no error')
          t.same(report, { valid: 1, invalid: 0 })
          t.end()
        })
      })
    })
  })
})
