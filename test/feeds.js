var tape = require('tape')
var memdb = require('memdb')
var hypercore = require('../')

tape('add feed', function (t) {
  var core = create()
  var feed = core.add()

  feed.append('hello')
  feed.append('world')
  feed.finalize(function () {
    t.ok(feed.id, 'has id')
    t.same(feed.blocks, 2, 'two blocks')
    t.end()
  })
})

tape('add feed with batch', function (t) {
  var core = create()
  var feed = core.add()

  feed.append(['hello', 'world'])
  feed.finalize(function () {
    t.ok(feed.id, 'has id')
    t.same(feed.blocks, 2, 'two blocks')
    t.end()
  })
})

tape('append after finalize', function (t) {
  var core = create()
  var feed = core.add()

  feed.append(['hello', 'world'])
  feed.finalize(function () {
    t.throws(function () {
      feed.append('lol')
    })
    t.end()
  })
})

tape('feed has', function (t) {
  var core = create()
  var feed = core.add()

  feed.append(['hello', 'world'], function () {
    t.ok(feed.has(0), 'has block 0')
    t.ok(feed.has(1), 'has block 1')
    t.ok(!feed.has(2), 'does not have block 2')
    t.end()
  })
})

tape('get feed after finalize', function (t) {
  var core = create()
  var feed = core.add()

  feed.append(['hello', 'world'])
  feed.finalize(function () {
    var feed2 = core.get(feed.id)
    feed2.ready(function () {
      t.same(feed2.blocks, 2, 'two blocks')
      t.same(feed2.id, feed.id, 'same id')
      t.end()
    })
  })
})

tape('get block after finalize', function (t) {
  var core = create()
  var feed = core.add()

  feed.append(['hello', 'world'])
  feed.finalize(function () {
    var feed2 = core.get(feed.id)
    feed2.get(0, function (err, block) {
      t.ok(!err, 'no error')
      t.same(block, new Buffer('hello'), 'has block 0')
      t.end()
    })
  })
})

function create () {
  return hypercore(memdb())
}
