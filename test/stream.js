var tape = require('tape')
var memdb = require('memdb')
var concat = require('concat-stream')
var hypercore = require('../')

tape('createWriteStream to createReadStream', function (t) {
  t.plan(1)
  var core1 = create()
  var core2 = create()

  var w = core1.createWriteStream()
  w.end('hello')

  var r = core2.createReadStream(w.key)
  r.pipe(concat(function (body) {
    t.deepEqual(body.toString(), 'hello')
  }))
  replicate(w.feed, r.feed)
})

tape('createReadStream with start, end', function (t) {
  t.plan(1)
  var core1 = create()
  var core2 = create()

  var w = core1.createWriteStream()
  w.write('hello')
  w.write('multiple')
  w.write('worlds')
  w.end(function () {
    var r = core2.createReadStream(w.key, {start: 1, end: 2})
    r.pipe(concat(function (body) {
      t.deepEqual(body.toString(), 'multiple')
    }))

    replicate(w.feed, r.feed)
  })
})

tape('createReadStream with start, no end', function (t) {
  t.plan(1)
  var core1 = create()
  var core2 = create()

  var w = core1.createWriteStream()
  w.write('hello')
  w.write('multiple')
  w.write('worlds')
  w.end(function () {
    var r = core2.createReadStream(w.key, {start: 1})
    r.pipe(concat(function (body) {
      t.deepEqual(body.toString(), 'multipleworlds')
    }))

    replicate(w.feed, r.feed)
  })
})

tape('createReadStream with end, no start', function (t) {
  t.plan(1)
  var core1 = create()
  var core2 = create()

  var w = core1.createWriteStream()
  w.write('hello')
  w.write('multiple')
  w.write('worlds')
  w.end(function () {
    var r = core2.createReadStream(w.key, {end: 2})
    r.pipe(concat(function (body) {
      t.deepEqual(body.toString(), 'hellomultiple')
    }))

    replicate(w.feed, r.feed)
  })
})

tape('createReadStream from feed', function (t) {
  t.plan(2)
  var core1 = create()
  var core2 = create()

  var w = core1.createWriteStream()
  w.write('hello')
  w.write('multiple')
  w.write('worlds')
  w.end(function () {
    var f = core2.createFeed(w.key)
    f.get(0, function () {
      t.deepEqual(f.blocks, 3)
      var r = core2.createReadStream(f)
      r.pipe(concat(function (body) {
        t.deepEqual(body.toString(), 'hellomultipleworlds')
      }))
    })
    replicate(w.feed, f)
  })
})

function replicate (a, b) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream)
}

function create () {
  return hypercore(memdb())
}
