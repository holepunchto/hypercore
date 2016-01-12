var tape = require('tape')
var memdb = require('memdb')
var hypercore = require('../')

tape('use extension', function (t) {
  var core = create()
  var remote = create()

  var ping = core.use('ping')
  var remotePing = remote.use('ping')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  coreStream.on('handshake', function () {
    if (!coreStream.supports(ping)) return
    coreStream.send(ping, new Buffer([0])) // send ping

    remoteStream.receive(remotePing, function (extension, buf) {
      remoteStream.send(remotePing, new Buffer([1]))
    })

    coreStream.receive(ping, function (extension, buf) {
      t.same(buf, new Buffer([1]), 'pong received')
      t.end()
    })
  })
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
  var ping = core.use('ping')
  var remotePing = remote.use('ping')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  coreStream.on('handshake', function () {
    coreStream.send(ping, new Buffer([0]))

    remoteStream.receive(remotePing, function (extension, buf) {
      t.same(buf, new Buffer([0]), 'multiple extension message routing works')
      t.end()
    })
  })
})

tape('quite a few extensions in different order', function (t) {
  var core = create()
  var remote = create()

  var coreVerify = core.use('verify')
  core.use('hello')
  core.use('world')
  remote.use('world')
  remote.use('hello')
  var remoteVerify = remote.use('verify')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  var message = new Buffer('hello')

  coreStream.on('handshake', function () {
    if (!coreStream.supports(coreVerify)) return

    coreStream.send(coreVerify, message)
    remoteStream.receive(remoteVerify, function (extension, buf) {
      t.same(buf, message, 'multiple extensions in different order work')
      t.end()
    })
  })
})

tape('send multiple buffers', function (t) {
  t.plan(2)
  var core = create()
  var remote = create()

  var corePing = core.use('ping')
  var remotePing = remote.use('ping')

  var coreStream = core.createPeerStream()
  var remoteStream = remote.createPeerStream()

  remoteStream.pipe(coreStream).pipe(remoteStream)

  coreStream.on('handshake', function () {
    if (!coreStream.supports(corePing)) return

    coreStream.send(corePing, new Buffer([0]))
    coreStream.send(corePing, new Buffer([0]))

    remoteStream.receive(remotePing, function (extension, buf) {
      t.same(buf, new Buffer([0]))
    })
  })
})

function create () {
  return hypercore(memdb())
}
