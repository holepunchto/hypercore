const create = require('./helpers/create')
const tape = require('tape')

tape('replicate with ack', function (t) {
  const feed = create()
  feed.on('ready', function () {
    const clone = create(feed.key)

    const stream = feed.replicate(true, { live: true, ack: true })
    stream.pipe(clone.replicate(false, { live: true })).pipe(stream)

    stream.once('duplex-channel', function () {
      feed.append(['a', 'b', 'c'])
    })
    let seen = 0
    stream.on('ack', function (ack) {
      seen++
      if (seen > 3) t.fail()
      if (seen === 3) t.end()
    })
  })
})

tape('ack only when something is downloaded', function (t) {
  t.plan(1)
  const feed = create()
  feed.on('ready', function () {
    const clone = create(feed.key)
    const stream1 = clone.replicate(true)
    stream1.on('ack', function (ack) {
      t.fail('unexpected ack')
    })
    feed.append(['a', 'b', 'c'], function () {
      // pre-populate with 3 records
      stream1.pipe(feed.replicate(false)).pipe(stream1)
    })
    stream1.on('end', function () {
      feed.append(['d', 'e'])
      // add 2 more records. only these should be ACK'd
      const acks = []
      const stream2 = feed.replicate(true, { ack: true })
      stream2.on('ack', function (ack) {
        acks.push(ack.start)
      })
      stream2.pipe(clone.replicate(false)).pipe(stream2)
      stream2.on('end', function () {
        t.deepEqual(acks.sort(), [3, 4])
        t.end()
      })
    })
  })
})

tape('simultaneous replication with ack and no-ack', function (t) {
  t.plan(1)
  const feed = create()
  feed.on('ready', function () {
    feed.append(['a', 'b', 'c'])

    const clone1 = create(feed.key)
    const clone2 = create(feed.key)
    const stream0 = feed.replicate(true, { ack: true })
    const stream1 = clone1.replicate(false)
    const stream2 = clone2.replicate(false)
    const stream3 = feed.replicate(true)
    stream1.pipe(stream0).pipe(stream1)
    stream2.pipe(stream3).pipe(stream2)

    const acks = []
    stream0.on('ack', function (ack) {
      acks.push(ack.start)
    })
    stream1.on('ack', function (ack) {
      t.fail('unexpected ack')
    })
    stream2.on('ack', function (ack) {
      t.fail('unexpected ack')
    })
    stream3.on('ack', function (ack) {
      t.fail('unexpected ack')
    })
    stream1.on('end', function () {
      t.deepEqual(acks.sort(), [0, 1, 2])
      t.end()
    })
  })
})

tape('simultaneous replication with two acks', function (t) {
  t.plan(1)
  const feed = create()
  feed.on('ready', function () {
    feed.append(['a', 'b', 'c'])

    const clone1 = create(feed.key)
    const clone2 = create(feed.key)
    const stream0 = feed.replicate(true, { ack: true })
    const stream1 = clone1.replicate(false)
    const stream2 = clone2.replicate(false)
    const stream3 = feed.replicate(true, { ack: true })
    stream1.pipe(stream0).pipe(stream1)
    stream2.pipe(stream3).pipe(stream2)

    const acks = [[], []]
    stream0.on('ack', function (ack) {
      acks[0].push(ack.start)
    })
    stream1.on('ack', function (ack) {
      t.fail('unexpected ack')
    })
    stream2.on('ack', function (ack) {
      t.fail('unexpected ack')
    })
    stream3.on('ack', function (ack) {
      acks[1].push(ack.start)
    })
    let pending = 2
    stream1.on('end', function () {
      if (--pending === 0) check()
    })
    stream2.on('end', function () {
      if (--pending === 0) check()
    })
    function check () {
      acks.forEach(function (r) { r.sort() })
      t.deepEqual(acks, [[0, 1, 2], [0, 1, 2]])
      t.end()
    }
  })
})

tape('acks where clones should not ack', function (t) {
  t.plan(1)
  const feed = create()
  feed.on('ready', function () {
    feed.append(['a', 'b', 'c'])

    const clone1 = create(feed.key)
    const clone2 = create(feed.key)
    const stream1 = feed.replicate(true, { ack: true })
    const stream2 = feed.replicate(true, { ack: true })
    const cstream1 = clone1.replicate(false, { ack: true }) // but shouldn't get any acks
    const cstream2 = clone2.replicate(false, { ack: true }) // but shouldn't get any acks
    stream1.pipe(cstream1).pipe(stream1)
    stream2.pipe(cstream2).pipe(stream2)

    cstream1.on('ack', function (ack) {
      t.fail('unexpected ack')
    })
    cstream2.on('ack', function (ack) {
      t.fail('unexpected ack')
    })
    const acks = [[], []]
    stream1.on('ack', function (ack) {
      acks[0].push(ack.start)
    })
    stream2.on('ack', function (ack) {
      acks[1].push(ack.start)
    })
    let pending = 2
    stream1.on('end', function () {
      if (--pending === 0) check()
    })
    stream2.on('end', function () {
      if (--pending === 0) check()
    })
    function check () {
      acks.forEach(function (r) { r.sort() })
      t.deepEqual(acks, [[0, 1, 2], [0, 1, 2]])
      t.end()
    }
  })
})

tape('transitive clone acks', function (t) {
  t.plan(2)
  const feed = create()
  feed.on('ready', function () {
    feed.append(['a', 'b', 'c'], ready)
  })
  function ready (err) {
    t.ifError(err)
    const clone1 = create(feed.key)
    const clone2 = create(feed.key)
    const stream1 = feed.replicate(true, { live: true, ack: true })
    const stream2 = clone1.replicate(false, { live: true, ack: true })
    const stream3 = clone1.replicate(true, { live: true, ack: true })
    const stream4 = clone2.replicate(false, { live: true, ack: true })
    const acks = [[], [], [], []]
    ;[stream1, stream2, stream3, stream4].forEach(function (stream, i) {
      stream.on('ack', function (ack) {
        acks[i].push(ack.start)
      })
    })
    stream1.pipe(stream2).pipe(stream1)
    stream3.pipe(stream4).pipe(stream3)
    let dl = 0
    clone2.on('download', function () {
      // allow an extra tick for ack response to arrive
      if (++dl === 3) setImmediate(check)
    })
    function check () {
      acks.forEach(function (r) { r.sort() })
      t.deepEqual(acks, [[0, 1, 2], [], [0, 1, 2], []])
    }
  }
})

tape('larger gossip network acks', function (t) {
  t.plan(16)
  const feed = create()
  const cores = [feed]
  const acks = {}
  feed.on('ready', function () {
    for (let i = 1; i < 10; i++) {
      cores.push(create(feed.key))
    }
    next(0)
  })
  const ops = [
    ['append', 'A'],
    ['connect', 0, 1], // acks["0,1"].push(0)
    ['append', 'B'],
    ['append', 'C'],
    ['connect', 1, 2], // acks["1,2"].push(0)
    ['connect', 0, 1], // acks["0,1"].push(1,2)
    ['append', 'D'],
    ['append', 'E'],
    ['append', 'F'],
    ['connect', 0, 5], // acks["0,5"].push(0,1,2,3,4,5)
    ['connect', 2, 5], // acks["5,2"].push(1,2,3,4,5)
    ['connect', 5, 6], // acks["5,6"].push(0,1,2,3,4,5)
    ['connect', 1, 6], // acks["6,1"].push(3,4,5)
    ['append', 'G'],
    ['append', 'H'],
    ['connect', 4, 2], // acks["2,4"].push(0,1,2,3,4,5)
    ['connect', 0, 7], // acks["0,7"].push(0,1,2,3,4,5,6,7)
    ['connect', 4, 7], // acks["7,4"].push(6,7)
    ['connect', 4, 5], // acks["4,5"].push(6,7)
    ['connect', 5, 8], // acks["5,8"].push(0,1,2,3,4,5,6,7)
    ['append', 'I'],
    ['append', 'J'],
    ['append', 'K'],
    ['connect', 0, 8], // acks["0,8"].push(8,9,10)
    ['connect', 5, 9], // acks["5,9"].push(0,1,2,3,4,5,6,7)
    ['connect', 8, 4], // acks["8,4"].push(8,9,10)
    ['append', 'L'],
    ['append', 'M'],
    ['append', 'N'],
    ['append', 'O'],
    ['connect', 9, 0], // acks["0,9"].push(8,9,10,11,12,13,14)
    ['connect', 2, 9] // acks["9,2"].push(6,7,8,9,10,11,12,13,14)
  ]
  function next (i) {
    const op = ops[i]
    if (!op) return check()
    if (op[0] === 'append') {
      feed.append(op[1], function (err) {
        t.ifError(err)
        next(i + 1)
      })
    } else if (op[0] === 'connect') {
      const src = cores[op[1]]
      const dst = cores[op[2]]
      const sr = src.replicate(true, { ack: true })
      const dr = dst.replicate(false, { ack: true })
      sr.on('ack', function (ack) {
        const key = op[1] + ',' + op[2]
        if (!acks[key]) acks[key] = []
        acks[key].push(ack.start)
      })
      dr.on('ack', function (ack) {
        const key = op[2] + ',' + op[1]
        if (!acks[key]) acks[key] = []
        acks[key].push(ack.start)
      })
      sr.pipe(dr).pipe(sr)
      let pending = 2
      sr.on('end', function () { if (--pending === 0) next(i + 1) })
      dr.on('end', function () { if (--pending === 0) next(i + 1) })
    }
  }
  function check () {
    Object.keys(acks).forEach(function (key) {
      acks[key].sort(function (a, b) { return a - b })
    })
    t.deepEqual(acks, {
      '0,1': [0, 1, 2],
      '1,2': [0],
      '0,5': [0, 1, 2, 3, 4, 5],
      '5,2': [1, 2, 3, 4, 5],
      '5,6': [0, 1, 2, 3, 4, 5],
      '6,1': [3, 4, 5],
      '2,4': [0, 1, 2, 3, 4, 5],
      '0,7': [0, 1, 2, 3, 4, 5, 6, 7],
      '7,4': [6, 7],
      '4,5': [6, 7],
      '5,8': [0, 1, 2, 3, 4, 5, 6, 7],
      '0,8': [8, 9, 10],
      '5,9': [0, 1, 2, 3, 4, 5, 6, 7],
      '8,4': [8, 9, 10],
      '0,9': [8, 9, 10, 11, 12, 13, 14],
      '9,2': [6, 7, 8, 9, 10, 11, 12, 13, 14]
    })
  }
})
