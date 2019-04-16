var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var tape = require('tape')

tape('accurate stat totals', function (t) {
  t.plan(4)

  var feed = create()
  feed.append(['aa', 'bb', 'cc', 'dd', 'ee'], function () {
    var clone = create(feed.key)
    replicate(feed, clone).on('end', function () {
      var feedStats = feed.stats
      var cloneStats = clone.stats

      t.same(feedStats.totals.uploadedBlocks, 5)
      t.same(feedStats.totals.uploadedBytes, 10)
      t.same(cloneStats.totals.downloadedBlocks, 5)
      t.same(cloneStats.totals.downloadedBytes, 10)
    })
  })
})

tape('accurate per-peer stats', function (t) {
  t.plan(13)

  var feed = create()

  feed.append(['aa', 'bb', 'cc', 'dd', 'ee'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1, { live: true })
    replicate(feed, clone2, { live: true })

    setTimeout(function () {
      onreplicate(clone1, clone2)
    }, 50)
  })

  function onreplicate (clone1, clone2) {
    var feedStats = feed.stats
    var clone1Stats = clone1.stats
    var clone2Stats = clone2.stats

    t.same(feedStats.totals.uploadedBlocks, 10)
    t.same(feedStats.totals.uploadedBytes, 20)
    t.same(feedStats.peers.length, 2)
    t.same(feedStats.peers[0].uploadedBlocks, 5)
    t.same(feedStats.peers[0].uploadedBytes, 10)
    t.same(feedStats.peers[1].uploadedBlocks, 5)
    t.same(feedStats.peers[1].uploadedBytes, 10)

    t.same(clone1Stats.peers.length, 1)
    t.same(clone1Stats.peers[0].downloadedBytes, 10)
    t.same(clone1Stats.peers[0].downloadedBlocks, 5)

    t.same(clone2Stats.peers.length, 1)
    t.same(clone2Stats.peers[0].downloadedBytes, 10)
    t.same(clone2Stats.peers[0].downloadedBlocks, 5)
  }
})

tape('should not collect stats when stats option is false', function (t) {
  t.plan(2)

  var feed = create({ stats: false })
  feed.append(['aa', 'bb', 'cc', 'dd', 'ee'], function () {
    var clone = create(feed.key, { stats: false })
    replicate(feed, clone).on('end', function () {
      var feedStats = feed.stats
      var cloneStats = clone.stats

      t.false(feedStats)
      t.false(cloneStats)
    })
  })
})
