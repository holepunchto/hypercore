const Hypercore = require('../../')
const RAM = require('random-access-memory')
const DebuggingStream = require('debugging-stream')

exports.create = async function create (...args) {
  const core = new Hypercore(RAM, ...args)
  await core.ready()
  return core
}

exports.createStored = function createStored () {
  const files = new Map()

  return function (...args) {
    return new Hypercore(storage, ...args)
  }

  function storage (name) {
    if (files.has(name)) return files.get(name).clone()
    const st = new RAM()
    files.set(name, st)
    return st
  }
}

exports.replicate = function replicate (a, b, t, opts = {}) {
  const s1 = a.replicate(true, { keepAlive: false, ...opts })
  const s2 = b.replicate(false, { keepAlive: false, ...opts })

  const closed1 = new Promise(resolve => s1.once('close', resolve))
  const closed2 = new Promise(resolve => s2.once('close', resolve))

  s1.on('error', err => t.comment(`replication stream error (initiator): ${err}`))
  s2.on('error', err => t.comment(`replication stream error (responder): ${err}`))

  if (opts.teardown !== false) {
    t.teardown(async function () {
      s1.destroy()
      s2.destroy()
      await closed1
      await closed2
    })
  }

  s1.pipe(s2).pipe(s1)

  return [s1, s2]
}

exports.replicateDebugStream = function replicate (a, b, t, opts = {}) {
  const { latency, speed, jitter } = opts

  const s1 = a.replicate(true, { keepAlive: false, ...opts })
  const s2Base = b.replicate(false, { keepAlive: false, ...opts })
  const s2 = new DebuggingStream(s2Base, { latency, speed, jitter })

  s1.on('error', err => t.comment(`replication stream error (initiator): ${err}`))
  s2.on('error', err => t.comment(`replication stream error (responder): ${err}`))

  if (opts.teardown !== false) {
    t.teardown(async function () {
      let missing = 2
      await new Promise(resolve => {
        s1.on('close', onclose)
        s1.destroy()

        s2.on('close', onclose)
        s2.destroy()

        function onclose () {
          if (--missing === 0) resolve()
        }
      })
    })
  }

  s1.pipe(s2).pipe(s1)

  return [s1, s2]
}

exports.unreplicate = function unreplicate (streams) {
  return Promise.all(streams.map((s) => {
    return new Promise((resolve) => {
      s.on('error', () => {})
      s.on('close', resolve)
      s.destroy()
    })
  }))
}

exports.eventFlush = async function eventFlush () {
  await new Promise(resolve => setImmediate(resolve))
}
