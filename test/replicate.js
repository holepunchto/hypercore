var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var tape = require('tape')
var Protocol = require('hypercore-protocol')

tape('replicate', function (t) {
  t.plan(10)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
    clone.get(3, same(t, 'd'))
    clone.get(4, same(t, 'e'))

    replicate(feed, clone, { live: true })
  })
})

tape('replicate twice', function (t) {
  t.plan(20)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
    clone.get(3, same(t, 'd'))
    clone.get(4, same(t, 'e'))

    replicate(feed, clone).on('end', function () {
      feed.append(['f', 'g', 'h', 'i', 'j'], function () {
        replicate(feed, clone).on('end', function () {
          clone.get(5, same(t, 'f'))
          clone.get(6, same(t, 'g'))
          clone.get(7, same(t, 'h'))
          clone.get(8, same(t, 'i'))
          clone.get(9, same(t, 'j'))
        })
      })
    })
  })
})

tape('replicate live', function (t) {
  t.plan(6)

  var feed = create()

  feed.ready(function () {
    var clone = create(feed.key)

    replicate(feed, clone, { live: true })

    feed.append('a')
    feed.append('b')
    feed.append('c')

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
  })
})

tape('download while get', function (t) {
  t.plan(10)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    // add 5 so this never finished
    clone.download({ start: 0, end: 6 }, function () {
      t.fail('should never happen')
    })

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
    clone.get(3, same(t, 'd'))
    clone.get(4, same(t, 'e'))

    replicate(feed, clone, { live: true })
  })
})

tape('non live', function (t) {
  t.plan(10)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    replicate(clone, feed).on('end', function () {
      clone.get(0, same(t, 'a'))
      clone.get(1, same(t, 'b'))
      clone.get(2, same(t, 'c'))
      clone.get(3, same(t, 'd'))
      clone.get(4, same(t, 'e'))
    })
  })
})

tape('non live, two way', function (t) {
  t.plan(20)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    replicate(clone, feed).on('end', function () {
      clone.get(0, same(t, 'a'))
      clone.get(1, same(t, 'b'))
      clone.get(2, same(t, 'c'))
      clone.get(3, same(t, 'd'))
      clone.get(4, same(t, 'e'))

      var clone2 = create(feed.key)

      replicate(clone, clone2).on('end', function () {
        clone2.get(0, same(t, 'a'))
        clone2.get(1, same(t, 'b'))
        clone2.get(2, same(t, 'c'))
        clone2.get(3, same(t, 'd'))
        clone2.get(4, same(t, 'e'))
      })
    })
  })
})

tape('non-live empty', function (t) {
  var feed = create()

  feed.ready(function () {
    var clone = create(feed.key)

    replicate(feed, clone).on('end', function () {
      t.same(clone.length, 0)
      t.end()
    })
  })
})

tape('basic 3-way replication', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1, { live: true })
    replicate(clone1, clone2, { live: true })

    clone1.get(0, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer.from('a'))

      clone2.get(0, function (err) {
        t.error(err, 'no error')
        t.same(data, Buffer.from('a'))
        t.end()
      })
    })
  })
})

tape('basic 3-way replication sparse and not sparse', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone1 = create(feed.key, { sparse: true })
    var clone2 = create(feed.key)

    replicate(feed, clone1, { live: true })

    clone1.get(0, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer.from('a'))

      replicate(clone1, clone2, { live: true })

      clone2.get(0, function (err) {
        t.error(err, 'no error')
        t.same(data, Buffer.from('a'))
        var inflight = clone2.peers[0].inflightRequests
        if (inflight.length === 1 && inflight[0].index === 0) inflight = [] // just has not been cleared yet
        t.same(inflight, [], 'no additional requests')
        t.end()
      })
    })
  })
})

tape('extra data + factor of two', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], function () {
    var clone1 = create(feed.key)

    replicate(feed, clone1, { live: true })

    clone1.get(1, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer.from('b'))
      t.end()
    })
  })
})

tape('3-way another index', function (t) {
  var feed = create()

  feed.append(['a', 'b'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1, { live: true })
    replicate(clone1, clone2, { live: true })

    clone1.get(1, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer.from('b'))

      clone2.get(1, function (err) {
        t.error(err, 'no error')
        t.same(data, Buffer.from('b'))
        t.end()
      })
    })
  })
})

tape('3-way another index + extra data', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1, { live: true })
    replicate(clone1, clone2, { live: true })

    clone1.get(1, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer.from('b'))

      clone2.get(1, function (err) {
        t.error(err, 'no error')
        t.same(data, Buffer.from('b'))
        t.end()
      })
    })
  })
})

tape('3-way another index + extra data + factor of two', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], function () {
    var clone1 = create(feed.key)
    var clone2 = create(feed.key)

    replicate(feed, clone1, { live: true })
    replicate(clone1, clone2, { live: true })

    clone1.get(1, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer.from('b'))

      clone2.get(1, function (err) {
        t.error(err, 'no error')
        t.same(data, Buffer.from('b'))
        t.end()
      })
    })
  })
})

tape('3-way another index + extra data + factor of two + static', function (t) {
  var feed = create({ live: false })

  feed.append(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], function () {
    feed.finalize(function () {
      var clone1 = create(feed.key)
      var clone2 = create(feed.key)

      replicate(feed, clone1, { live: true })
      replicate(clone1, clone2, { live: true })

      clone1.get(1, function (err, data) {
        t.error(err, 'no error')
        t.same(data, Buffer.from('b'))

        clone2.get(1, function (err) {
          t.error(err, 'no error')
          t.same(data, Buffer.from('b'))
          t.end()
        })
      })
    })
  })
})

tape('seek while replicating', function (t) {
  t.plan(6)

  var feed = create()

  feed.ready(function () {
    var clone = create(feed.key)

    clone.seek(9, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 2)
      t.same(offset, 1)
    })

    clone.seek(16, function (err, index, offset) {
      t.error(err, 'no error')
      t.same(index, 4)
      t.same(offset, 2)
    })

    feed.append(['hello'], function () {
      feed.append(['how', 'are', 'you', 'doing', '?'], function () {
        replicate(feed, clone, { live: true })
      })
    })
  })
})

tape('non spare live replication', function (t) {
  var feed = create()

  feed.on('ready', function () {
    feed.append(['a', 'b', 'c'], function () {
      var clone = create(feed.key)

      clone.get(0, function () {
        clone.get(1, function () {
          clone.get(2, function () {
            clone.once('download', function () {
              t.pass('downloaded new block')
              t.end()
            })

            feed.append('a')
          })
        })
      })

      replicate(feed, clone, { live: true })
    })
  })
})

tape('can wait for updates', function (t) {
  var feed = create()

  feed.on('ready', function () {
    var clone = create(feed.key)

    clone.update(function (err) {
      t.error(err, 'no error')
      t.same(clone.length, 3)
      t.end()
    })

    replicate(feed, clone, { live: true }).once('duplex-channel', function () {
      feed.append(['a', 'b', 'c'])
    })
  })
})

tape('replicate while clearing', function (t) {
  var feed = create()

  feed.on('ready', function () {
    var clone = create(feed.key, { sparse: true })

    clone.get(1, function (err) {
      t.error(err, 'no error')
      feed.clear(2, function (err) {
        t.error(err, 'no error')
        clone.get(2, { timeout: 50 }, function (err) {
          t.ok(err, 'had timeout error')
          t.end()
        })
      })
    })

    replicate(feed, clone, { live: true }).once('duplex-channel', function () {
      feed.append(['a', 'b', 'c'])
    })
  })
})

tape('replicate while cancelling', function (t) {
  t.plan(2)

  var feed = create()

  feed.on('ready', function () {
    var clone = create(feed.key, { sparse: true })

    clone.on('download', function () {
      t.fail('should not download')
    })

    feed.on('upload', function () {
      t.pass('should upload')
      clone.cancel(0)
    })

    clone.get(0, function (err) {
      t.ok(err, 'expected error')
    })

    feed.append(['a', 'b', 'c'])

    replicate(feed, clone, { live: true })
  })
})

tape('allow push', function (t) {
  t.plan(3)

  var feed = create()

  feed.on('ready', function () {
    var clone = create(feed.key, { sparse: true, allowPush: true })

    clone.on('download', function () {
      t.pass('push allowed')
    })

    feed.on('upload', function () {
      t.pass('should upload')
      clone.cancel(0)
    })

    clone.get(0, function (err) {
      t.ok(err, 'expected error')
    })

    feed.append(['a', 'b', 'c'])

    replicate(feed, clone, { live: true })
  })
})

tape('shared stream, non live', function (t) {
  var a = create()
  var b = create()

  a.append(['a', 'b'], function () {
    b.append(['c', 'd'], function () {
      var a1 = create(a.key)
      var b1 = create(b.key)

      a1.ready(function () {
        var s = a.replicate(true)
        b1.replicate(s)

        var s1 = a1.replicate(false)
        b.replicate(s1)

        s.pipe(s1).pipe(s)

        s.on('end', function () {
          t.ok(a1.has(0))
          t.ok(a1.has(1))
          t.ok(b1.has(0))
          t.ok(b1.has(1))
          t.end()
        })
      })
    })
  })
})

tape('get total downloaded chunks', function (t) {
  var feed = create()
  feed.append(['a', 'b', 'c', 'e'])
  feed.on('ready', function () {
    var clone = create(feed.key, { sparse: true })
    clone.get(1, function (err) {
      t.error(err, 'no error')
      t.same(clone.downloaded(), 1)
      t.same(clone.downloaded(0), 1)
      t.same(clone.downloaded(2), 0)
      t.same(clone.downloaded(0, 1), 0)
      t.same(clone.downloaded(2, 4), 0)
      clone.get(3, function (err) {
        t.error(err, 'no error')
        t.same(clone.downloaded(), 2)
        t.same(clone.downloaded(0), 2)
        t.same(clone.downloaded(2), 1)
        t.same(clone.downloaded(0, 3), 1)
        t.same(clone.downloaded(2, 4), 1)
        t.end()
      })
    })
    replicate(feed, clone, { live: true })
  })
})

tape('feed has a range of chuncks', function (t) {
  var feed = create()
  feed.append(['a', 'b', 'c', 'e'])
  feed.on('ready', function () {
    var clone = create(feed.key, { sparse: true })
    clone.get(0, function (err) {
      t.error(err, 'no error')
      clone.get(1, function (err) {
        t.error(err, 'no error')
        clone.get(2, function (err) {
          t.error(err, 'no error')
          t.ok(clone.has(1))
          t.notOk(clone.has(3))
          t.ok(clone.has(0, clone.length - 1))
          t.notOk(clone.has(0, clone.length))
          t.ok(clone.has(1, 3))
          t.notOk(clone.has(3, 4))
          t.end()
        })
      })
    })
    replicate(feed, clone, { live: true })
  })
})

tape('feed has a large range', function (t) {
  var feed = create()
  feed.append(['a', 'b', 'c', 'e', 'd', 'e', 'f', 'g'])
  feed.append(['a', 'b', 'c', 'e', 'd', 'e', 'f', 'g'])
  feed.append(['a', 'b', 'c', 'e', 'd', 'e', 'f', 'g'])
  feed.on('ready', function () {
    var clone = create(feed.key, { sparse: true })
    var count = 20
    var gotten = 20
    function got () {
      gotten--
      if (gotten === 0) {
        t.same(clone.downloaded(), 20)
        t.notOk(clone.has(5, 24))
        t.notOk(clone.has(12, 24))
        t.notOk(clone.has(20, 24))
        t.ok(clone.has(0, 20))
        t.ok(clone.has(3, 20))
        t.ok(clone.has(8, 20))
        t.ok(clone.has(19, 20))
        t.ok(clone.has(0, 16))
        t.ok(clone.has(3, 16))
        t.ok(clone.has(8, 16))
        t.end()
      }
    }
    for (var i = 0; i < count; i++) {
      clone.get(i, got)
    }
    replicate(feed, clone, { live: true })
  })
})

tape('replicate no download', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    clone.get(0, function () {
      t.fail('Data was received')
    })

    replicate(feed, clone, { live: true }, { live: true, download: false })

    setTimeout(function () {
      t.pass('No data was received')
      t.end()
    }, 300)
  })
})

tape('replicate no upload', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    clone.get(0, function () {
      t.fail('Data was received')
    })

    replicate(feed, clone, { live: true, upload: false }, { live: true })

    setTimeout(function () {
      t.pass('No data was received')
      t.end()
    }, 300)
  })
})

tape('sparse mode, two downloads', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key, { sparse: true })

    replicate(feed, clone)
    clone.update(function () {
      clone.download({ start: 0, end: 4 }, function (err) {
        t.error(err, 'no error')
        // next tick so selection is cleared
        process.nextTick(function () {
          clone.download(4, function (err) {
            t.error(err, 'no error')
            t.end()
          })
        })
      })
    })
  })
})

tape('peer-add and peer-remove are emitted', function (t) {
  t.plan(5)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    feed.on('peer-add', function (peer) {
      t.notEquals(peer.remoteId, null)
      t.pass('peer-add1')
    })
    clone.on('peer-add', function (peer) {
      t.pass('peer-add2')
    })
    feed.on('peer-remove', function (peer) {
      t.pass('peer-remove1')
    })
    clone.on('peer-remove', function (peer) {
      t.pass('peer-remove2')
    })

    replicate(clone, feed)
  })
})

tape('replicate with onwrite', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var expected = ['a', 'b', 'c', 'd', 'e']

    var clone = create(feed.key, {
      onwrite: function (index, data, peer, cb) {
        t.ok(peer, 'has peer')
        t.same(expected[index], data.toString())
        expected[index] = null
        cb()
      }
    })

    clone.on('sync', function () {
      t.same(expected, [null, null, null, null, null])
      t.end()
    })

    replicate(feed, clone, { live: true })
  })
})

tape('replicate from very sparse', function (t) {
  t.plan(3)

  var feed = create()
  var arr = new Array(1e3)

  arr.fill('a')
  feed.append(arr, function loop (err) {
    if (feed.length < 1e6) return feed.append(arr, loop)

    t.error(err, 'no error')
    t.pass('appended ' + arr.length + ' blocks')

    var clone1 = create(feed.key, { sparse: true })
    var clone2 = create(feed.key)
    var missing = 30
    var then = 0

    replicate(feed, clone1, { live: true })

    clone2.on('download', function () {
      if (--missing <= 0) {
        t.pass('downloaded all in ' + (Date.now() - then) + 'ms')
      }
    })

    clone1.download({ start: feed.length - 30, end: feed.length }, function () {
      then = Date.now()
      replicate(clone2, clone1, { live: true })
    })
  })
})

tape('first get hash, then get block', function (t) {
  var feed = create()
  feed.append(['a', 'b', 'c'], function () {
    var clone = create(feed.key, { sparse: true })
    replicate(feed, clone, { live: true })

    // fetches the hash for block #2
    clone.seek(2, function (err) {
      t.error(err, 'no error')
      clone.get(2, function (err, data) {
        t.error(err, 'no error')
        t.same(data, Buffer.from('c'))
        t.end()
      })
    })
  })
})

tape('destroy replication stream before handshake', function (t) {
  var feed = create()
  feed.append(['a', 'b', 'c'], function () {
    var stream = feed.replicate(true)
    stream.destroy()
    var anotherStream = feed.replicate(true)
    setImmediate(function () {
      anotherStream.destroy()
      feed.ifAvailable.ready(function () {
        t.pass('ifAvailable still triggers')
        t.same(feed.peers.length, 0)
        t.end()
      })
    })
  })
})

tape('request timeouts', function (t) {
  t.plan(4)

  var feed = create()
  var stream = new Protocol(false, {
    timeout: 100
  })

  feed.ready(function () {
    var ch = stream.open(feed.key, {
      onwant (want) {
        t.pass('got want')
        ch.have({ start: 0, length: 1 })
      },
      onrequest (request) {
        t.same(request.index, 0, 'got request for #0')
      }
    })

    t.same(typeof stream.timeout.ms, 'number', 'can read timeout ms from protocol stream')

    var timeout = setTimeout(() => t.fail('request should have timed out'), stream.timeout.ms * 2)
    var feedStream = feed.replicate(true, { download: true, timeout: 100 })
    stream.pipe(feedStream).pipe(stream)

    feedStream.on('error', function (err) {
      clearTimeout(timeout)
      t.ok(err, 'stream had timeout error')
    })

    stream.on('error', () => {})
  })
})

tape('double replicate', function (t) {
  var feed = create()

  feed.append('hi', function () {
    var clone = create(feed.key)

    var a = feed.replicate(true)
    var b = clone.replicate(false)
    var missing = 2

    a.pipe(b).pipe(a)
    feed.replicate(a) // replicate twice

    b.on('end', done)
    a.on('end', done)

    function done () {
      if (!--missing) return
      feed.ifAvailable.ready(function () {
        clone.ifAvailable.ready(function () {
          t.pass('no lingering state')
          t.end()
        })
      })
    }
  })
})

tape('regression: replicate without timeout', function (t) {
  t.plan(10)

  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e'], function () {
    var clone = create(feed.key)

    clone.get(0, same(t, 'a'))
    clone.get(1, same(t, 'b'))
    clone.get(2, same(t, 'c'))
    clone.get(3, same(t, 'd'))
    clone.get(4, same(t, 'e'))

    replicate(feed, clone, { live: true, timeout: false })
  })
})

tape('replicate with NOISE disabled', function (t) {
  var feed = create()
  feed.append(['a', 'b', 'c'], function () {
    var clone = create(feed.key)
    const stream = replicate(feed, clone, { live: false, noise: false, encrypted: false })
    clone.get(2, (err, data) => {
      t.error(err, 'no error')
      t.same(data.toString(), 'c')
      t.same(stream.remoteVerified(feed.key), false, 'remote is not verified')
      t.end()
    })
  })
})

tape('replicate and close through stream', function (t) {
  var feed = create()
  var streams
  var clone

  feed.append(['a', 'b', 'c'], function () {
    clone = create(feed.key)
    streams = [feed.replicate(true, { live: true }), clone.replicate(false, { live: true })]
    streams[0].pipe(streams[1]).pipe(streams[0])
    streams[0].on('error', () => {})
    streams[1].on('error', () => {})
  })

  feed.once('peer-open', function () {
    streams[0].close(feed.discoveryKey)
    streams[0].destroy()
    streams[0].on('close', function () {
      t.same(feed.peers.length, 0)
      t.end()
    })
  })
})

tape('download blocks', function (t) {
  var feed = create()

  feed.append(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'], function () {
    var clone = create(feed.key, { sparse: true })

    clone.download({ start: 0, end: 10, blocks: [0, 3, 4, 9] }, function (err) {
      t.error(err, 'no error')
      t.same(clone.length, 10)
      t.ok(clone.has(0))
      t.notOk(clone.has(1))
      t.notOk(clone.has(2))
      t.ok(clone.has(3))
      t.ok(clone.has(4))
      t.notOk(clone.has(5))
      t.notOk(clone.has(6))
      t.notOk(clone.has(7))
      t.notOk(clone.has(8))
      t.ok(clone.has(9))
      t.end()
    })

    replicate(feed, clone, { live: true })
  })
})

function same (t, val) {
  return function (err, data) {
    t.error(err, 'no error')
    t.same(data.toString(), val)
  }
}
