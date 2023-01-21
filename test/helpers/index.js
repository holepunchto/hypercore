const fs = require('fs')
const path = require('path')
const os = require('os')
const Hypercore = require('../../')
const RAM = require('random-access-memory')
const createTestnet = require('@hyperswarm/testnet')
const Corestore = require('corestore')

exports.create = async function create (...args) {
  const core = new Hypercore(RAM, ...args)
  await core.ready()
  return core
}

exports.createCore = async function createCore (t, ...args) {
  const tmpdir = exports.createTmpDir(t.teardown)
  await exports.eventFlush() // + temp
  const core = new Hypercore(tmpdir, ...args)
  await core.ready()
  return core
}

exports.createStore = async function createStore (t, ...args) {
  const tmpdir = exports.createTmpDir(t.teardown)
  await exports.eventFlush() // + temp
  const store = new Corestore(tmpdir)
  await store.ready()
  return store
}

exports.useTestnet = async function useTestnet (t) {
  const swarm = await createTestnet(3, { host: '127.0.0.1' })
  t.teardown(() => swarm.destroy())
  return swarm
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

exports.replicate = function replicate (a, b, t, opts) {
  const s1 = a.replicate(true, { keepAlive: false, ...opts })
  const s2 = b.replicate(false, { keepAlive: false, ...opts })
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

exports.createTmpDir = function createTmpDir (teardown) {
  const tmpdir = path.join(os.tmpdir(), 'hypercore-test-')
  const dir = fs.mkdtempSync(tmpdir)
  if (teardown) teardown(() => fs.rmSync(dir, { recursive: true }))
  return dir
}
