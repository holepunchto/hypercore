const tape = require('tape')
const create = require('./helpers/create')

tape('get before timeout', function (t) {
  const feed = create()

  feed.get(0, { timeout: 100 }, function (err) {
    t.error(err, 'no timeout error')
    t.end()
  })

  feed.append('hi')
})

tape('get after timeout', function (t) {
  const feed = create()

  feed.get(42, { timeout: 100 }, function (err) {
    t.ok(err, 'had timeout')
    t.end()
  })
})
