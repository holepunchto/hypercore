var tape = require('tape')
var create = require('./helpers/create')

tape('basic value encoding', function (t) {
  var feed = create({
    valueEncoding: 'json'
  })

  feed.append({ hello: 'world' }, function () {
    feed.get(0, function (err, val) {
      t.error(err, 'no error')
      t.same(val, { hello: 'world' })
      t.end()
    })
  })
})
