var tape = require('tape')
var hypercore = require('./helpers/create')

tape('list keys', function (t) {
  var hc = hypercore()
  var feeds = [hc.createFeed(), hc.createFeed(), hc.createFeed()]

  finalizeAll(feeds, function () {
    hc.list(function (err, keys) {
      if (err) throw err

      feeds.sort(sortByKey)
      keys.sort(sortByKey)

      t.deepEqual(keys, feeds.map(function (f) { return f.key }))
      t.end()
    })
  })
})

tape('list values', function (t) {
  var hc = hypercore()
  var feeds = [hc.createFeed(), hc.createFeed(), hc.createFeed()]

  finalizeAll(feeds, function () {
    hc.list({ values: true }, function (err, values) {
      if (err) throw err

      feeds.sort(sortByKey)
      values.sort(sortByKey)

      t.deepEqual(values, feeds.map(function (f) {
        return {
          discoveryKey: f.discoveryKey,
          key: f.key,
          live: f.live,
          prefix: f.prefix,
          secretKey: f.secretKey
        }
      }))
      t.end()
    })
  })
})

function finalizeAll (feeds, cb) {
  feeds.forEach(function (f) { f.finalize(done) })
  var numDone = 0
  function done () {
    if (++numDone === feeds.length) {
      cb()
    }
  }
}

function sortByKey (a, b) {
  var aKey = Buffer.isBuffer(a) ? a : a.key
  var bKey = Buffer.isBuffer(b) ? b : b.key
  return aKey.toString('hex').localeCompare(bKey.toString('hex'))
}
