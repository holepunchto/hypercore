var tape = require('tape')
var hypercore = require('./helpers/create')
var memdb = require('memdb')

tape('get block from hash', function (t) {
  t.plan(6)
  var core = hypercore(memdb())

  var w1 = core.createWriteStream()
  w1.end('hello')
  w1.once('finish', function () {
    w1.feed.head(onhead(1, w1.feed, function () {
      var w2 = core.createWriteStream()
      w2.end('HI')
      w2.once('finish', function () {
        w2.feed.head(onhead(2, w2.feed))
      })
    }))
  })

  function onhead (n, feed, cb) {
    return function (err, hash, block) {
      t.error(err)
      feed.getBlockFromHash(hash, function (err, b) {
        t.error(err)
        t.equal(block, b, 'block match #' + n)
        if (cb) cb()
      })
    }
  }
})
