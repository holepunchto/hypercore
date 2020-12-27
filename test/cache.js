var tape = require('tape')
var create = require('./helpers/create')

tape('default options does not use data cache', function (t) {
  var feed = create()
  feed.append(['hello', 'world'], err => {
    t.error(err, 'no error')
    feed.get(0, function (err, block) {
      t.error(err, 'no error')
      t.same(feed._storage.treeCache.byteSize, 40)
      feed.get(1, function (err, block) {
        t.error(err, 'no error')
        t.same(feed._storage.treeCache.byteSize, 80)
        t.false(feed._storage.dataCache)
        t.end()
      })
    })
  })
})

tape('numeric data cache opt creates data cache', function (t) {
  var feed = create({ cache: { data: 1024 } })
  var firstLength = Buffer.byteLength('hello')
  var secondLength = Buffer.byteLength('world')

  feed.append(['hello', 'world'], err => {
    t.error(err, 'no error')
    feed.get(0, function (err, block) {
      t.error(err, 'no error')
      t.true(feed._storage.dataCache)
      t.same(feed._storage.dataCache.byteSize, firstLength)
      feed.get(1, function (err, block) {
        t.error(err, 'no error')
        t.same(feed._storage.dataCache.byteSize, firstLength + secondLength)
        t.end()
      })
    })
  })
})

tape('can use a custom cache object', function (t) {
  var treeStorage = []
  var dataStorage = []
  var createCache = function (storage) {
    return {
      get: function (key) {
        return storage[key]
      },
      set: function (key, value) {
        storage[key] = value
      }
    }
  }

  var feed = create({
    cache: {
      tree: createCache(treeStorage),
      data: createCache(dataStorage)
    }
  })
  feed.append(['hello', 'world'], err => {
    t.error(err, 'no error')
    feed.get(0, function (err, block) {
      t.error(err, 'no error')
      t.same(dataStorage.length, 1)
      t.same(treeStorage.length, 1)
      feed.get(1, function (err, block) {
        t.error(err, 'no error')
        t.same(dataStorage.length, 2)
        // tree storage should have entries at 0 and 2
        t.same(treeStorage.length, 3)
        t.end()
      })
    })
  })
})

tape('can disable all caching', function (t) {
  var feed = create({ cache: { tree: false } })

  feed.append(['hello', 'world'], err => {
    t.error(err, 'no error')
    feed.get(0, function (err, block) {
      t.error(err, 'no error')
      feed.get(1, function (err, block) {
        t.error(err, 'no error')
        t.false(feed._storage.dataCache)
        t.false(feed._storage.treeCache)
        t.end()
      })
    })
  })
})
