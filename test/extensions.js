var tape = require('tape')
var memdb = require('memdb')
var hypercore = require('../')

tape('use extension', function (t) {
  var core = create()
  var remote = create()

  core.use('ping', function (id, peer) {
    peer.receive(id, function (buf) {
      t.same(buf.toString(), 'yo wattup dogg', 'extensions work')
      t.end()
    })
  })

  remote.use('ping', function (id, peer) {
    peer.send(id, 'yo wattup dogg')
  })

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)
})

tape('no extension support', function (t) {
  var core = create()
  var remote = create()

  var ping = core.use('ping')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  coreStream.on('handshake', function () {
    t.ok(!coreStream.supports(ping), 'peer does not support extension')
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
    t.same(coreStream.listAvailable(), [ 'ping' ], 'multiple extension works')
    t.end()
  })
})

tape('multiple extension message routing', function (t) {
  var core = create()
  var remote = create()

  core.use('snakes')
  core.use('ping', function (id, peer) {
    peer.send(id, 'hello')
  })
  remote.use('ping', function (id, peer) {
    peer.receive(id, function (buf) {
      t.same(buf.toString(), 'hello', 'message routing works')
      t.end()
    })
  })

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)
})

tape('quite a few extensions in different order', function (t) {
  var core = create()
  var remote = create()

  var message = new Buffer('hello')

  core.use('verify')
  core.use('hello', function (id, peer) {
    peer.send(id, message)
  })
  core.use('world')
  remote.use('world')

  remote.use('hello', function (id, peer) {
    peer.receive(id, function (buf) {
      t.same(buf, message, 'routing works well')
      t.end()
    })
  })

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)
})

tape('send multiple buffers', function (t) {
  t.plan(2)
  var core = create()
  var remote = create()

  core.use('ping', function (id, peer) {
    peer.receive(id, function (buf) {
      t.same(buf.toString(), 'yoyo')
    })
  })

  remote.use('ping', function (id, peer) {
    peer.send(id, 'yoyo')
    peer.send(id, 'yoyo')
  })

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)
})

function create () {
  return hypercore(memdb())
}
