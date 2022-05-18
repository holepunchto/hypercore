const Hypercore = require('../../')
const ram = require('random-access-memory')

exports.create = async function create (...args) {
  const core = new Hypercore(ram, ...args)
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
    const st = ram()
    files.set(name, st)
    return st
  }
}

exports.replicate = function replicate (a, b, t) {
  const s1 = a.replicate(true, { keepAlive: false })
  const s2 = b.replicate(false, { keepAlive: false })
  s1.on('error', err => t.comment(`replication stream error (initiator): ${err}`))
  s2.on('error', err => t.comment(`replication stream error (responder): ${err}`))
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
