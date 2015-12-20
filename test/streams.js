var tape = require('tape')
var memdb = require('memdb')
var hypercore = require('../')

tape('write and read', function (t) {
  var core = create()
  var ws = core.createWriteStream()

  ws.write('hello')
  ws.write('world')
  ws.end(function () {
    t.same(ws.blocks, 2, 'two blocks')
    t.ok(ws.id, 'has id after finish')
    var rs = core.createReadStream(ws.id)
    var expected = ['hello', 'world']

    rs.on('data', function (data) {
      t.same(data.toString(), expected.shift(), 'data was written')
    })
    rs.on('end', function () {
      t.same(expected.length, 0, 'no more data')
      t.end()
    })
  })
})

tape('read limit', function (t) {
  var core = create()
  insert(core, function (id) {
    var rs = core.createReadStream(id, {limit: 1})
    var expected = ['hello']

    rs.on('data', function (data) {
      t.same(data.toString(), expected.shift(), 'data was written')
    })
    rs.on('end', function () {
      t.same(expected.length, 0, 'no more data')
      t.end()
    })
  })
})

tape('read start', function (t) {
  var core = create()
  insert(core, function (id) {
    var rs = core.createReadStream(id, {start: 1})
    var expected = ['world']

    rs.on('data', function (data) {
      t.same(data.toString(), expected.shift(), 'data was written')
    })
    rs.on('end', function () {
      t.same(expected.length, 0, 'no more data')
      t.end()
    })
  })
})

tape('empty write', function (t) {
  var core = create()
  var ws = core.createWriteStream()

  ws.end(function () {
    t.same(ws.blocks, 0, 'no blocks')
    t.same(ws.id, null, 'no id')
    t.end()
  })
})

function insert (core, cb) {
  var ws = core.createWriteStream()
  ws.write('hello')
  ws.write('world')
  ws.end(function () {
    cb(ws.id)
  })
}

function create () {
  return hypercore(memdb())
}
