var tape = require('tape')
var memdb = require('memdb')
var hypercore = require('../')

tape('set feed storage', function (t) {
  var core = create()
  var store = MemStore()
  var feed = core.add({storage: store})
  feed.append('hello')
  feed.append('world')
  feed.finalize(function () {
    t.same(store.blocks[0], Buffer('hello'))
    t.same(store.blocks[1], Buffer('world'))
    t.end()
  })
})

tape('storage is checksummed', function (t) {
  var core = create()
  var store = MemStore()
  var feed = core.add({storage: store})
  feed.append('hello')
  feed.finalize(function () {
    store.blocks[0] = Buffer('yolo')
    feed.get(0, function (err) {
      t.ok(err, 'checksum failed')
      t.ok(err.notFound, 'not found error')
      t.end()
    })
  })
})

tape('set feed storage with old feed', function (t) {
  var core = create()
  var store = MemStore()
  var feed = core.add({storage: store})
  feed.append('hello')
  feed.append('world')
  feed.finalize(function () {
    feed.close(function () {
      var feed1 = core.get(feed.id, {storage: store})
      feed1.get(0, function (err, blk) {
        t.error(err)
        t.same(blk, Buffer('hello'))
        feed1.get(1, function (err, blk) {
          t.error(err)
          t.same(blk, Buffer('world'))
          t.end()
        })
      })
    })
  })
})

tape('set storage in core', function (t) {
  var store
  var core = create({
    storage: function () {
      store = MemStore()
      return store
    }
  })

  var feed = core.add()
  feed.append('hello')
  feed.append('world')
  feed.finalize(function () {
    t.ok(store, 'created storage')
    t.same(store.blocks[0], Buffer('hello'))
    t.same(store.blocks[1], Buffer('world'))
    t.end()
  })
})

tape('set storage in core with old feed', function (t) {
  var store
  var core = create({
    storage: function (feed) {
      if (feed.id) return store
      store = MemStore()
      return store
    }
  })

  var feed = core.add()
  feed.append('hello')
  feed.append('world')
  feed.finalize(function () {
    feed.close(function () {
      var feed1 = core.get(feed.id)
      feed1.get(0, function (err, blk) {
        t.error(err)
        t.same(blk, Buffer('hello'))
        feed1.get(1, function (err, blk) {
          t.error(err)
          t.same(blk, Buffer('world'))
          t.end()
        })
      })
    })
  })
})

function MemStore () { // TODO: should go in a module
  if (!(this instanceof MemStore)) return new MemStore()
  this.blocks = []
}

MemStore.prototype.append = function (blocks, cb) {
  for (var i = 0; i < blocks.length; i++) this.blocks.push(blocks[i])
  if (cb) process.nextTick(cb)
}

MemStore.prototype.get = function (i, cb) {
  var self = this
  process.nextTick(function () {
    if (!self.blocks[i]) return cb(new Error('Block not found'))
    cb(null, self.blocks[i])
  })
}

MemStore.prototype.put = function (i, block, cb) {
  this.blocks[i] = block
  if (cb) process.nextTick(cb)
}

MemStore.prototype.close = function (cb) {
  if (cb) process.nextTick(cb)
}

function create (opts) {
  return hypercore(memdb(), opts)
}
