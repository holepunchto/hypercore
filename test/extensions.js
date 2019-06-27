var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var tape = require('tape')

var EXAMPLE_TYPE = 'example'
var EXAMPLE_MESSAGE = Buffer.from([4, 20])

tape('sort extension names', function (t) {
  t.plan(1)

  var feed = create(null, {
    extensions: ['b', 'a']
  })

  t.deepEquals(feed.extensions, ['a', 'b'], '')
})

tape('send and receive extension messages', function (t) {
  t.plan(4)

  var feed1 = create(null, {
    extensions: ['example']
  })

  feed1.ready(function () {
    var feed2 = create(feed1.key, {
      extensions: ['example']
    })

    feed2.on('extension', function (type, message, peer) {
      t.equal(type, EXAMPLE_TYPE)
      t.equal(message.toString('hex'), EXAMPLE_MESSAGE.toString('hex'))

      peer.extension(EXAMPLE_TYPE, EXAMPLE_MESSAGE)
    })

    feed1.on('extension', function (type, message, peer) {
      t.equal(type, EXAMPLE_TYPE)
      t.equal(message.toString('hex'), EXAMPLE_MESSAGE.toString('hex'))
    })

    feed1.on('peer-add', function () {
      feed1.extension(EXAMPLE_TYPE, EXAMPLE_MESSAGE)
    })

    replicate(feed1, feed2)
  })
})
