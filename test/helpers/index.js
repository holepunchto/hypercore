const Hypercore = require('../../')
const createTempDir = require('test-tmp')
const CoreStorage = require('hypercore-storage')
const safetyCatch = require('safety-catch')
const DebuggingStream = require('debugging-stream')

exports.create = async function (t, ...args) {
  const dir = await createTempDir(t)

  const db = new CoreStorage(dir)

  const core = new Hypercore(db, ...args)
  await core.ready()

  t.teardown(() => core.close().catch(safetyCatch), { order: 1 })

  return core
}

const createStorage = exports.createStorage = async function (t, dir) {
  if (!dir) dir = await createTempDir(t)
  return new CoreStorage(dir)
}

exports.createStored = async function (t) {
  const dir = await createTempDir(t)
  let db = null

  return async function (...args) {
    if (db) await db.close()
    db = await createStorage(t, dir)
    return new Hypercore(db, ...args)
  }
}

exports.replicate = function replicate (a, b, t, opts = {}) {
  const s1 = a.replicate(true, { keepAlive: false, ...opts })
  const s2 = b.replicate(false, { keepAlive: false, ...opts })

  const closed1 = new Promise(resolve => s1.once('close', resolve))
  const closed2 = new Promise(resolve => s2.once('close', resolve))

  s1.on('error', err => {
    safetyCatch(err)
    t.comment(`replication stream error (initiator): ${err}`)
  })
  s2.on('error', err => {
    safetyCatch(err)
    t.comment(`replication stream error (responder): ${err}`)
  })

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

exports.eventFlush = async function eventFlush () {
  await new Promise(resolve => setImmediate(resolve))
}
