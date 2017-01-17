var tape = require('tape')
var ram = require('random-access-memory')
var hypercore = require('./helpers/create')
var copy = require('./helpers/copy')

tape('append one', function (t) {
  var feed = hypercore().createFeed()

  feed.append('hello', function () {
    t.same(feed.has(0), true, 'has first')
    feed.get(0, function (err, data) {
      t.error(err, 'no error')
      t.same(data, Buffer('hello'), 'first is hello')
      t.end()
    })
  })
})

tape('append, close, resume', function (t) {
  var core = hypercore()
  var feed = core.createFeed()

  feed.append('hello')
  feed.close(function () {
    var feed1 = core.createFeed(feed.key)
    feed1.append('world')
    feed1.flush(function () {
      t.same(feed1.has(0), true, 'has first')
      t.same(feed1.has(1), true, 'has second')
      feed1.get(0, function (err, data) {
        t.error(err, 'no error')
        t.same(data, Buffer('hello'), 'was hello')
        feed1.get(1, function (err, data) {
          t.error(err, 'no error')
          t.same(data, Buffer('world'), 'was world')
          t.end()
        })
      })
    })
  })
})

tape('non-live append and finalize', function (t) {
  var feed = hypercore().createFeed({live: false})
  feed.append('hello')
  feed.finalize(function () {
    t.same(feed.key, Buffer('bbd2540413d287b1f842b75849a4d15f0512e6e4bb7a4921a246fa216817206d', 'hex'))
    t.end()
  })
})

tape('non-live finalize empty', function (t) {
  var feed = hypercore().createFeed({live: false})
  feed.finalize(function () {
    t.same(feed.key, null)
    t.end()
  })
})

tape('non-live replicate without digest', function (t) {
  var feed = hypercore().createFeed({live: false})
  feed.append(['a', 'b', 'c'])
  feed.finalize(function () {
    var clone = hypercore().createFeed(feed.key)
    var missing = [0, 1, 2]

    loop()

    function loop () {
      if (!missing.length) return t.end()
      var next = missing.shift()
      feed.proof(next, function (err, proof) {
        t.error(err, 'no proof error')
        feed.get(next, function (err, data) {
          t.error(err, 'no get error')
          clone.put(next, data, proof, function (err) {
            t.error(err, 'no put error')
            t.same(clone.has(next), true, 'clone has ' + next)
            loop()
          })
        })
      })
    }
  })
})

tape('non-live replicate with digest', function (t) {
  var feed = hypercore().createFeed({live: false})
  feed.append(['a', 'b', 'c'])
  feed.finalize(function () {
    var clone = hypercore().createFeed(feed.key)
    var missing = [0, 1, 2]

    loop()

    function loop () {
      if (!missing.length) return t.end()
      var next = missing.shift()
      feed.proof(next, {digest: clone.digest(next)}, function (err, proof) {
        t.error(err, 'no proof error')
        if (next) t.same(proof.nodes.length, 0, 'no nodes needed')
        feed.get(next, function (err, data) {
          t.error(err, 'no get error')
          clone.put(next, data, proof, function (err) {
            t.error(err, 'no put error')
            t.same(clone.has(next), true, 'clone has ' + next)
            loop()
          })
        })
      })
    }
  })
})

tape('invalid non-live replicate with digest', function (t) {
  t.plan(3)

  var feed = hypercore().createFeed({live: false})
  feed.append(['a', 'b', 'c'])
  feed.finalize(function () {
    var key = Buffer('00000000000000000000000000000000')
    var clone = hypercore().createFeed(key)

    copy(0, feed, clone, function (err) {
      t.ok(err, 'should error')
    })
    copy(1, feed, clone, function (err) {
      t.ok(err, 'should error')
    })
    copy(2, feed, clone, function (err) {
      t.ok(err, 'should error')
    })
  })
})

tape('replicate without digest', function (t) {
  var feed = hypercore().createFeed()
  var clone = hypercore().createFeed(feed.key)
  feed.append(['a', 'b', 'c'], function () {
    var missing = [0, 1, 2]

    loop()

    function loop () {
      if (!missing.length) return t.end()
      var next = missing.shift()
      feed.proof(next, function (err, proof) {
        t.ok(proof.signature, 'signed')
        t.error(err, 'no proof error')
        feed.get(next, function (err, data) {
          t.error(err, 'no get error')
          clone.put(next, data, proof, function (err) {
            t.error(err, 'no put error')
            t.same(clone.has(next), true, 'clone has ' + next)
            loop()
          })
        })
      })
    }
  })
})

tape('replicate with digest', function (t) {
  var feed = hypercore().createFeed()
  var clone = hypercore().createFeed(feed.key)
  feed.append(['a', 'b', 'c'], function () {
    var missing = [0, 1, 2]

    loop()

    function loop () {
      if (!missing.length) return t.end()
      var next = missing.shift()
      feed.proof(next, {digest: clone.digest(next)}, function (err, proof) {
        t.same(!!proof.signature, next === 0, 'only first one is signed')
        t.error(err, 'no proof error')
        if (next) t.same(proof.nodes.length, 0, 'no nodes needed')
        feed.get(next, function (err, data) {
          t.error(err, 'no get error')
          clone.put(next, data, proof, function (err) {
            t.error(err, 'no put error')
            t.same(clone.has(next), true, 'clone has ' + next)
            loop()
          })
        })
      })
    }
  })
})

tape('invalid replicate with digest', function (t) {
  t.plan(3)

  var feed = hypercore().createFeed()
  feed.append(['a', 'b', 'c'])
  feed.finalize(function () {
    var key = Buffer('00000000000000000000000000000000')
    var clone = hypercore().createFeed(key)

    copy(0, feed, clone, function (err) {
      t.ok(err, 'should error')
    })
    copy(1, feed, clone, function (err) {
      t.ok(err, 'should error')
    })
    copy(2, feed, clone, function (err) {
      t.ok(err, 'should error')
    })
  })
})

tape('replicate, append, replicate', function (t) {
  var feed = hypercore().createFeed()
  var clone1 = hypercore().createFeed(feed.key)
  var clone2 = hypercore().createFeed(feed.key)

  feed.append(['a', 'b', 'c'], function () {
    copy(1, feed, clone1, function (err) {
      t.error(err, 'no copy error')
      t.ok(clone1.has(1), 'has 1')
      feed.append(['d', 'e', 'f', 'g', 'h', 'i', 'j', 'k'], function () {
        copy(9, feed, clone1, function (err) {
          t.error(err, 'no copy error')
          t.ok(clone1.has(9), 'has 9')
          copy(1, clone1, clone2, function (err) {
            t.error(err, 'no copy error')
            t.ok(clone2.has(1), 'has 1')
            copy(9, clone1, clone2, function (err) {
              t.error(err, 'no copy error')
              t.ok(clone2.has(9), 'has 9')
              t.end()
            })
          })
        })
      })
    })
  })
})

tape('fake size', function (t) {
  var feed = hypercore().createFeed()
  var clone = hypercore().createFeed(feed.key)

  feed.append(['hello', 'world', 'verden'], function (err) {
    t.error(err, 'no error')
    feed.proof(0, function (err, proof) {
      t.error(err, 'no error')
      proof.nodes[1].size = 1
      clone.put(0, Buffer('hello'), proof, function (err) {
        t.ok(!!err, 'validation should fail')
        t.end()
      })
    })
  })
})

tape('append and get empty value', function (t) {
  var feed = hypercore().createFeed()

  feed.append(Buffer(0), function (err) {
    t.error(err, 'no append error')
    feed.get(0, function (err, value) {
      t.error(err, 'no get error')
      t.same(value, Buffer(0), 'empty buffer')
      t.end()
    })
  })
})

tape('sets .blocks', function (t) {
  var core = hypercore()
  var feed = core.createFeed()

  feed.append(['a', 'b', 'c'], function () {
    t.same(feed.blocks, 3)
    feed.append(['d', 'e', 'f'], function () {
      t.same(feed.blocks, 6)
      var clone = core.createFeed(feed.key)
      clone.open(function () {
        t.same(clone.blocks, 6)
        t.end()
      })
    })
  })
})

tape('sets .bytes', function (t) {
  var core = hypercore()
  var feed = core.createFeed()

  feed.append(['a', 'bb', 'c'], function () {
    t.same(feed.bytes, 4)
    feed.append(['ddd', 'ee', 'f'], function () {
      t.same(feed.bytes, 10)
      var clone = core.createFeed(feed.key)
      clone.open(function () {
        t.same(clone.bytes, 10)
        t.end()
      })
    })
  })
})

tape('opts.key as string', function (t) {
  var feed = hypercore().createFeed()
  var clone = hypercore().createFeed({key: feed.key.toString('hex')})

  t.same(clone.key, feed.key)
  t.end()
})

tape('key wins over opts.key', function (t) {
  var feed = hypercore().createFeed()
  var feed2 = hypercore().createFeed()
  var clone = hypercore().createFeed(feed.key, {key: feed2.key})

  t.same(clone.key, feed.key)
  t.end()
})

// tests below was generated by a randomizer triggering different replication edge cases

tape('chaos monkey generated #1', function (t) {
  var ops = []
  ops.push({type: 'append', value: '998'})
  ops.push({type: 'copy', block: 0, from: 'feed', to: 'clone1'})
  runOps(t, ops)
})

tape('chaos monkey generated #2', function (t) {
  var ops = []
  ops.push({type: 'append', value: '993'})
  ops.push({type: 'append', value: '992'})
  ops.push({type: 'append', value: '991'})
  ops.push({type: 'copy', block: 0, from: 'feed', to: 'clone1'})
  ops.push({type: 'append', value: '984'})
  ops.push({type: 'copy', block: 1, from: 'feed', to: 'clone1'})
  ops.push({type: 'append', value: '982'})
  ops.push({type: 'copy', block: 0, from: 'clone1', to: 'clone2'})
  ops.push({type: 'copy', block: 3, from: 'feed', to: 'clone1'})
  runOps(t, ops)
})

tape('chaos monkey generated #3', function (t) {
  var ops = []
  ops.push({type: 'append', value: '996'})
  ops.push({type: 'append', value: '994'})
  ops.push({type: 'copy', block: 1, from: 'feed', to: 'clone1'})
  ops.push({type: 'copy', block: 1, from: 'clone1', to: 'clone2'})
  ops.push({type: 'append', value: '991'})
  ops.push({type: 'append', value: '990'})
  ops.push({type: 'append', value: '988'})
  ops.push({type: 'append', value: '985'})
  ops.push({type: 'copy', block: 5, from: 'feed', to: 'clone1'})
  ops.push({type: 'copy', block: 5, from: 'clone1', to: 'clone2'})
  ops.push({type: 'append', value: '981'})
  ops.push({type: 'append', value: '980'})
  ops.push({type: 'append', value: '976'})
  ops.push({type: 'append', value: '974'})
  ops.push({type: 'append', value: '972'})
  ops.push({type: 'append', value: '971'})
  ops.push({type: 'append', value: '969'})
  ops.push({type: 'copy', block: 8, from: 'feed', to: 'clone1'})
  ops.push({type: 'copy', block: 8, from: 'clone1', to: 'clone2'})
  ops.push({type: 'append', value: '965'})
  ops.push({type: 'copy', block: 6, from: 'feed', to: 'clone1'})
  ops.push({type: 'copy', block: 3, from: 'feed', to: 'clone1'})
  ops.push({type: 'append', value: '961'})
  ops.push({type: 'copy', block: 6, from: 'clone1', to: 'clone2'})
  ops.push({type: 'append', value: '957'})
  ops.push({type: 'append', value: '956'})
  ops.push({type: 'copy', block: 3, from: 'clone1', to: 'clone2'})
  ops.push({type: 'append', value: '953'})
  ops.push({type: 'append', value: '952'})
  ops.push({type: 'append', value: '951'})
  ops.push({type: 'append', value: '950'})
  ops.push({type: 'copy', block: 17, from: 'feed', to: 'clone1'})
  ops.push({type: 'append', value: '947'})
  ops.push({type: 'append', value: '946'})
  ops.push({type: 'copy', block: 18, from: 'feed', to: 'clone1'})
  ops.push({type: 'append', value: '944'})
  ops.push({type: 'copy', block: 22, from: 'feed', to: 'clone1'})
  runOps(t, ops)
})

tape('chaos monkey randomized', function (t) {
  var feed = hypercore().createFeed()
  var clone1 = hypercore().createFeed(feed.key)
  var clone2 = hypercore().createFeed(feed.key)
  var names = ['feed', 'clone1', 'clone2']
  var runs = 500

  loop()

  function loop () {
    if (--runs === 0) return t.end()
    if (Math.random() < 0.3) {
      return feed.append('' + runs, loop)
    }
    if (Math.random() < 0.5) return process.nextTick(loop)

    var from = [feed, clone1, clone2][(Math.random() * 3) | 0]
    var to = null
    if (from === clone1) to = clone2
    else if (from === clone2) to = clone1
    else to = [clone1, clone1][(Math.random() * 2) | 0]

    var blk = -1
    var seen = 0

    for (var i = 0; i < from.bitfield.length; i++) {
      if (!from.has(i) || to.has(i)) continue
      if (Math.random() < 1 / ++seen) blk = i
    }

    if (blk === -1) return process.nextTick(loop)

    copy(blk, from, to, function (err) {
      if (err) {
        t.fail('failed to copy ' + blk + ' from ' + name(from) + ' to ' + name(to))
        t.end()
        return
      }

      t.pass('copied ' + blk + ' from ' + name(from) + ' to ' + name(to))
      loop()
    })
  }

  function name (f) {
    return names[[feed, clone1, clone2].indexOf(f)]
  }
})

function runOps (t, ops) {
  var feed = hypercore().createFeed()
  var clone1 = hypercore().createFeed(feed.key)
  var clone2 = hypercore().createFeed(feed.key)

  loop()

  function get (name) {
    if (name === 'feed') return feed
    if (name === 'clone1') return clone1
    return clone2
  }

  function loop () {
    var next = ops.shift()
    if (!next) {
      t.end()
      return
    }
    if (next.type === 'append') {
      feed.append(next.value, loop)
      return
    }
    if (next.type === 'copy') {
      copy(next.block, get(next.from), get(next.to), function (err) {
        if (err) {
          t.fail('failed to copy ' + next.block + ' from ' + next.from + ' to ' + next.to)
          t.end()
          return
        }
        t.pass('copied ' + next.block + ' from ' + next.from + ' to ' + next.to)
        loop()
      })
      return
    }
  }
}

var json = {
  encode: function (obj, buf, offset) {
    offset = offset || 0
    var str = JSON.stringify(obj)
    buf = buf || Buffer(str.length + offset)
    buf.write(str, offset)
    return buf
  },
  decode: function (buf, offset, end) {
    var str = buf.toString(offset, end)
    return JSON.parse(str)
  }
}

tape('append valueEncoding', function (t) {
  var feed = hypercore({ valueEncoding: json }).createFeed()

  feed.append({ foo: 'bar' }, function () {
    t.same(feed.has(0), true, 'has first')
    feed.get(0, function (err, data) {
      t.error(err, 'no error')
      t.same(data, { foo: 'bar' }, 'first is json')
      t.end()
    })
  })
})

tape('put valueEncoding', function (t) {
  var feed = hypercore({ valueEncoding: json }).createFeed()
  var clone = hypercore({ valueEncoding: json }).createFeed(feed.key)

  feed.append({ foo: 'bar' }, function () {
    t.same(feed.has(0), true, 'has first')
    feed.proof(0, function (err, proof) {
      t.error(err, 'no error')
      clone.put(0, { foo: 'bar' }, proof, function (err) {
        t.error(err, 'no error')
        clone.get(0, function (err, data) {
          t.error(err, 'no error')
          t.same(data, { foo: 'bar' }, 'first is json')
          t.end()
        })
      })
    })
  })
})

tape('verify-on-read', function (t) {
  var feed = hypercore().createFeed({ storage: ram() })

  // write to feed
  feed.append('hello', function () {
    // modify in storage
    feed.storage.put(0, Buffer('ohell'))

    // read without verification
    feed.get(0, function (err, data) {
      // ...no error detected
      t.error(err, 'no error')
      t.same(feed.has(0), true, 'has first')
      t.same(data, Buffer('ohell'), 'first is modified')

      // read with verification
      feed.get(0, { verify: true }, function (err, data) {
        // error detected
        t.ok(err, 'error detected')
        t.same(feed.has(0), false, 'does not have first')
        t.end()
      })
    })
  })
})

tape('verify-on-read 2', function (t) {
  var numErrors = 0
  var n
  var feed = hypercore().createFeed({ storage: ram() })

  // write to feed
  n = 5
  feed.append('one', doneAppending)
  feed.append('two', doneAppending)
  feed.append('three', doneAppending)
  feed.append('four', doneAppending)
  feed.append('five', doneAppending)
  function doneAppending (err) {
    t.error(err, 'no error')
    if (--n > 0) return

    // modify in storage
    feed.storage.put(2, Buffer('33333'))

    // read all with verification
    n = 5
    feed.get(0, { verify: true }, doneReading)
    feed.get(1, { verify: true }, doneReading)
    feed.get(2, { verify: true }, doneReading)
    feed.get(3, { verify: true }, doneReading)
    feed.get(4, { verify: true }, doneReading)
  }

  function doneReading (err) {
    if (err) numErrors++
    if (--n > 0) return

    t.equal(numErrors, 1, 'one error')
    t.equal(feed.has(0), true, 'has block 0')
    t.equal(feed.has(1), true, 'has block 1')
    t.equal(feed.has(2), false, 'doesnt have block 2')
    t.equal(feed.has(3), true, 'has block 3')
    t.equal(feed.has(4), true, 'has block 4')
    t.end()
  }
})

tape('verify storage', function (t) {
  var n
  var feed = hypercore().createFeed({ storage: ram() })

  // write to feed
  n = 5
  feed.append('one', doneAppending)
  feed.append('two', doneAppending)
  feed.append('three', doneAppending)
  feed.append('four', doneAppending)
  feed.append('five', doneAppending)
  function doneAppending (err) {
    t.error(err, 'no error')
    if (--n > 0) return

    // modify in storage
    feed.storage.put(0, Buffer('111'))
    feed.storage.put(2, Buffer('33333'))

    // verify storage
    feed.verifyStorage(function (err, results) {
      t.error(err, 'no error')
      t.equal(results.numMissing, 2)
      t.equal(feed.has(0), false, 'doesnt have block 0')
      t.equal(feed.has(1), true, 'has block 1')
      t.equal(feed.has(2), false, 'doesnt have block 2')
      t.equal(feed.has(3), true, 'has block 3')
      t.equal(feed.has(4), true, 'has block 4')

      // verify storage again
      feed.verifyStorage(function (err, results) {
        t.error(err, 'no error')
        t.equal(results.numMissing, 0) // no newly-lost blocks
        t.end()
      })
    })
  }
})
