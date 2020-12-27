var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var tape = require('tape')

var EXAMPLE_TYPE = 'example'
var EXAMPLE_MESSAGE = Buffer.from([4, 20])

tape('send and receive extension messages', function (t) {
  t.plan(2)

  var feed1 = create(null)

  const e1 = feed1.registerExtension(EXAMPLE_TYPE, {
    onmessage (message, peer) {
      t.equal(message.toString('hex'), EXAMPLE_MESSAGE.toString('hex'))
    }
  })

  feed1.ready(function () {
    var feed2 = create(feed1.key)

    const e2 = feed2.registerExtension(EXAMPLE_TYPE, {
      onmessage (message, peer) {
        t.equal(message.toString('hex'), EXAMPLE_MESSAGE.toString('hex'))
        e2.send(EXAMPLE_MESSAGE, peer)
      }
    })

    feed1.once('peer-open', function () {
      e1.broadcast(EXAMPLE_MESSAGE)
    })

    replicate(feed1, feed2, { live: true })
  })
})

tape('send and receive extension messages with encoding', function (t) {
  t.plan(2)

  const feed1 = create(null)

  const e1 = feed1.registerExtension(EXAMPLE_TYPE, {
    encoding: 'json',
    onmessage (message, peer) {
      t.same(message, { hi: 'e1' })
    }
  })

  feed1.ready(function () {
    const feed2 = create(feed1.key)

    const e2 = feed2.registerExtension(EXAMPLE_TYPE, {
      encoding: 'json',
      onmessage (message, peer) {
        t.same(message, { hi: 'e2' })
        e2.send({ hi: 'e1' }, peer)
      }
    })

    feed1.once('peer-open', function () {
      e1.broadcast({ hi: 'e2' })
    })

    replicate(feed1, feed2, { live: true })
  })
})

tape('send and receive extension messages with multiple extensions', function (t) {
  t.plan(2)

  var feed1 = create(null)

  const e1 = feed1.registerExtension(EXAMPLE_TYPE, {
    onmessage (message, peer) {
      t.equal(message.toString('hex'), EXAMPLE_MESSAGE.toString('hex'))
    }
  })

  feed1.registerExtension('aa')

  feed1.ready(function () {
    var feed2 = create(feed1.key)

    feed2.registerExtension('bb')

    const e2 = feed2.registerExtension(EXAMPLE_TYPE, {
      onmessage (message, peer) {
        t.equal(message.toString('hex'), EXAMPLE_MESSAGE.toString('hex'))
        e2.send(EXAMPLE_MESSAGE, peer)
      }
    })

    feed1.once('peer-open', function () {
      e1.broadcast(EXAMPLE_MESSAGE)
    })

    replicate(feed1, feed2, { live: true })
  })
})

tape('extension encoding', t => {
  t.plan(3)

  const f1 = create(null)

  const dummyExt = {
    encoding: {
      encode () {
        t.pass('encoder was invoked')
        return Buffer.from('test')
      },
      decode () {
        t.pass('decode was invoked')
        return 'test'
      }
    },
    onerror (err) {
      t.error(err, 'no error')
    },
    onmessage () {
      t.pass('got message')
    }
  }

  const inst1 = f1.registerExtension('dummy', dummyExt)

  f1.ready(function () {
    const f2 = create(f1.key)
    f2.ready(function () {
      f2.registerExtension('dummy', dummyExt)
      replicate(f1, f2, { live: true })
      f1.once('peer-open', function () {
        inst1.broadcast({ hello: 'world' })
      })
    })
  })
})
