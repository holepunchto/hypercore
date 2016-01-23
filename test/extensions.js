var tape = require('tape')
var memdb = require('memdb')
var hechat = require('he-chat')
var hypercore = require('../')

tape('use he-chat', function (t) {
  var core = create()
  var remote = create()

  core.use(hechat())
  remote.use(hechat())

  core.hechat.broadcast('yo wats up')
  remote.hechat.on('message', function (message) {
    t.same(message, 'yo wats up', 'he-chat works!')
    t.end()
  })

  var coreStream = core.createPeerStream()
  coreStream.pipe(remote.createPeerStream()).pipe(coreStream)
})

function create () {
  return hypercore(memdb())
}
