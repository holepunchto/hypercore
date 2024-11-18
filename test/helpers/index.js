const Hypercore = require('../../')
const RAM = require('random-access-memory')
const streamx = require('streamx')

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
  const bytesPerSecond = opts.bytesPerSecond || null

  const s1 = a.replicate(true, { keepAlive: false, ...opts })
  const s2Fast = b.replicate(false, { keepAlive: false, ...opts })
  const s2 = bytesPerSecond ? makeSlow(s2Fast, bytesPerSecond) : s2Fast

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

function makeSlow (stream, bytesPerSecond) {
  const d = new streamx.Duplex({
    write (data, cb) {
      const datas = [data]
      let size = 0
      for (const d of datas) size += d.byteLength

      const wait = Math.ceil(1000 * size / bytesPerSecond) || 1

      setTimeout(() => {
        for (const data of datas) stream.write(data)
        cb(null)
      }, wait)
    }
  })

  stream.on('data', (data) => d.push(data))
  return d
}
