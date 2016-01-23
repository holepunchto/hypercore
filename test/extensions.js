var tape = require('tape')
var memdb = require('memdb')
var hypercore = require('../')

tape('use extension', function (t) {
  var core = create()
  var remote = create()

  core.use('ping')
  remote.use('ping')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  coreStream.on('handshake', function () {
    if (!coreStream.remoteSupports('ping')) return
    coreStream.send('ping', new Buffer([0])) // send ping

    remoteStream.on('ping', function () {
      remoteStream.send('ping', new Buffer([1]))
    })

    coreStream.on('ping', function (buf) {
      t.same(buf, new Buffer([1]), 'pong received')
      t.end()
    })
  })
})

tape('no extension support', function (t) {
  var core = create()
  var remote = create()

  core.use('ping')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  coreStream.on('handshake', function () {
    t.ok(!coreStream.remoteSupports('ping'), 'peer does not support extension')
    t.end()
  })
})

tape('multiple extensions', function (t) {
  var core = create()
  var remote = create()

  core.use('snakes')
  core.use('ping')
  remote.use('ping')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  coreStream.on('handshake', function () {
    t.ok(coreStream.remoteSupports('ping'), 'ping supported')
    t.ok(!coreStream.remoteSupports('snakes'), 'snakes not supported')
    t.end()
  })
})

tape('multiple extension message routing', function (t) {
  var core = create()
  var remote = create()

  core.use('snakes')
  core.use('ping')
  remote.use('ping')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  coreStream.on('handshake', function () {
    coreStream.send('ping', new Buffer([0]))

    remoteStream.on('ping', function (buf) {
      t.same(buf, new Buffer([0]), 'multiple extension message routing works')
      t.end()
    })
  })
})

tape('quite a few extensions in different order', function (t) {
  var core = create()
  var remote = create()

  core.use('verify')
  core.use('hello')
  core.use('world')
  remote.use('world')
  remote.use('hello')
  remote.use('verify')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  var message = new Buffer('hello')

  coreStream.on('handshake', function () {
    if (!coreStream.remoteSupports('verify')) return

    coreStream.send('verify', message)
    remoteStream.on('verify', function (buf) {
      t.same(buf, message, 'multiple extensions in different order work')
      t.end()
    })
  })
})

tape('send multiple buffers', function (t) {
  t.plan(2)
  var core = create()
  var remote = create()

  core.use('ping')
  remote.use('ping')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  coreStream.on('handshake', function () {
    if (!coreStream.remoteSupports('ping')) return

    coreStream.send('ping', new Buffer([0]))
    coreStream.send('ping', new Buffer([0]))

    remoteStream.on('ping', function (buf) {
      t.same(buf, new Buffer([0]))
    })
  })
})

function create () {
  return hypercore(memdb())
}
