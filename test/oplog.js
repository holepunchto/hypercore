const p = require('path')
const fs = require('fs')
const test = require('tape')
const fsctl = require('fsctl')
const raf = require('random-access-file')
const c = require('compact-encoding')
const crypto = require('hypercore-crypto')

const OpLog = require('../lib/oplog')

const STORAGE_FILE_NAME = 'test-storage'
const SHOULD_ERROR = Symbol('hypercore-oplog-should-error')

test('oplog - basic append', async function (t) {
  const storage = testStorage()

  const log = await OpLog.open(storage)
  await log.append(userData('a'))
  await log.append(userData('b'))

  console.log('before entries')

  let entries = []
  for await (const entry of log) {
    entries.push(entry)
  }

  console.log('after entries')

  t.same(entries.length, 2)
  t.same(entries[0].userData, Buffer.from('a'))
  t.same(entries[0].flushes, 0)
  t.same(entries[1].userData, Buffer.from('b'))
  t.same(entries[1].flushes, 0)

  await log.flush()

  entries = []
  for await (const entry of log) {
    entries.push(entry)
  }

  t.same(entries.length, 0)

  await log.append(userData('c'))

  entries = []
  for await (const entry of log) {
    entries.push(entry)
  }

  t.same(entries.length, 1)
  t.same(entries[0].userData, Buffer.from('c'))
  t.same(entries[0].flushes, 1)

  await cleanup(storage)
  t.end()
})

test('oplog - alternating header writes', async function (t) {
  const storage = testStorage()

  const log = await OpLog.open(storage)

  await log.flush()
  await log.flush()

  {
    const meta = await log._loadMetadata()
    t.same(meta.headers.length, 2)
    t.same(meta.headers[0].flushes, 2)
    t.same(meta.headers[1].flushes, 1)
  }

  await log.flush() // Should overwrite first header

  {
    const meta = await log._loadMetadata()
    t.same(meta.headers.length, 2)
    t.same(meta.headers[0].flushes, 2)
    t.same(meta.headers[1].flushes, 3)
  }

  await log.flush() // Should overwrite second header

  {
    const meta = await log._loadMetadata()
    t.same(meta.headers.length, 2)
    t.same(meta.headers[0].flushes, 4)
    t.same(meta.headers[1].flushes, 3)
  }

  await cleanup(storage)
  t.end()
})

test('oplog - one fully-corrupted header', async function (t) {
  const storage = testStorage()

  const log = await OpLog.open(storage)

  await log.flush()
  await log.flush()

  {
    const meta = await log._loadMetadata()
    t.same(meta.headers.length, 2)
    t.same(meta.headers[0].flushes, 2)
    t.same(meta.headers[1].flushes, 1)
  }

  await log.flush() // Should overwrite second header

  // Corrupt the first header -- second header should win now
  await new Promise((resolve, reject) => {
    storage.write(0, Buffer.from('hello world'), err => {
      if (err) return reject(err)
      return resolve()
    })
  })

  const meta = await log._loadMetadata()
  t.same(meta.headers.length, 2)
  t.false(meta.headers[0])
  t.true(meta.headers[1])
  t.same(meta.headers[meta.latestHeader].flushes, 3)

  await cleanup(storage)
  t.end()
})

test('oplog - header invalid checksum', async function (t) {
  const storage = testStorage()
  const badHeaderEncoding = {
    preencode (state, m) {
      c.uint.preencode(state, m.test)
    },
    encode (state, m) {
      c.uint.encode(state, m.test)
    },
    decode (state) {
      return { test: c.uint.decode(state) }
    }
  }

  const log = await OpLog.open(storage)

  await log.flush()
  await log.flush()

  {
    const meta = await log._loadMetadata()
    t.same(meta.headers.length, 2)
    t.same(meta.headers[0].flushes, 2)
    t.same(meta.headers[1].flushes, 1)
  }

  await log.flush() // Should overwrite second header (flushes = 3)

  // Invalidate the second header's checksum -- first header should win now (flushes = 2)
  await new Promise((resolve, reject) => {
    storage.write(4096 + 1 + 4, c.encode(badHeaderEncoding, { test: 1 }), err => {
      if (err) return reject(err)
      return resolve()
    })
  })

  {
    const meta = await log._loadMetadata()
    t.same(meta.headers.length, 2)
    t.same(meta.headers[0].flushes, 2)
    t.false(meta.headers[1]) // second header is corrupted
  }

  // Invalidate the first header's checksum -- the hypercore is now corrupted
  await new Promise((resolve, reject) => {
    storage.write(1 + 4, c.encode(badHeaderEncoding, { test: 1 }), err => {
      if (err) return reject(err)
      return resolve()
    })
  })

  try {
    await log._loadMetadata()
    t.fail('corruption should have been detected')
  } catch (_) {
    t.pass('corruption was correctly detected')
  }

  await cleanup(storage)
  t.end()
})

test('oplog - concurrent appends throw', async function (t) {
  const storage = testStorage()

  const log = await OpLog.open(storage)

  const appends = []
  appends.push(log.append(userData('a')))
  appends.push(log.append(userData('b')))

  const res = await Promise.allSettled(appends)
  t.same(res[0].status, 'fulfilled')
  t.same(res[1].status, 'rejected')

  await log.append(userData('c'))

  const entries = []
  for await (const entry of log) {
    entries.push(entry)
  }

  t.same(entries.length, 2)
  t.same(entries[0].userData, Buffer.from('a'))
  t.same(entries[1].userData, Buffer.from('c'))

  await cleanup(storage)
  t.end()
})

test('oplog - another hypercore is stored here', async function (t) {
  let storage = testStorage()
  const kp1 = crypto.keyPair()
  const kp2 = crypto.keyPair()

  const log = await OpLog.open(storage, { ...kp1 })

  t.same(log.publicKey, kp1.publicKey)
  t.same(log.secretKey, kp1.secretKey)

  await log.close()
  storage = testStorage()

  try {
    await OpLog.open(storage, { ...kp2 })
    t.fail('should have thrown keypair error')
  } catch (err) {
    t.same(err.message, 'Another hypercore is stored here')
  }

  await cleanup(storage)
  t.end()
})

test('oplog - malformed log entry gets overwritten', async function (t) {
  let storage = testStorage()
  let log = await OpLog.open(storage)

  await log.append(userData('a'))
  await log.append(userData('b'))

  await log.close()

  storage = testStorage()
  log = await OpLog.open(storage)

  // Write a bad oplog message at the end (simulates a failed append)
  await new Promise((resolve, reject) => {
    storage.write(log._logLength + 4096 * 2 + 32, Buffer.from('hello world'), err => {
      if (err) return reject(err)
      return resolve()
    })
  })

  {
    const entries = []
    for await (const entry of log) {
      entries.push(entry)
    }

    t.same(entries.length, 2) // The partial entry should not be present
    t.same(entries[0].userData, Buffer.from('a'))
    t.same(entries[1].userData, Buffer.from('b'))
  }

  // Write a valid oplog message now
  await log.append(userData('c'))

  {
    const entries = []
    for await (const entry of log) {
      entries.push(entry)
    }

    t.same(entries.length, 3) // The partial entry should be overwritten
    t.same(entries[0].userData, Buffer.from('a'))
    t.same(entries[1].userData, Buffer.from('b'))
    t.same(entries[2].userData, Buffer.from('c'))
  }

  await cleanup(storage)
  t.end()
})

test('oplog - log not truncated when header write fails', async function (t) {
  const storage = failingOffsetStorage(4096 * 2 + 32)

  const log = await OpLog.open(storage)

  // Make subsequent header writes fail
  storage[SHOULD_ERROR](true)

  await log.append(userData('a'))
  await log.append(userData('b'))

  // The flush should fail because the header can't be updated -- log should still have entries after this
  try {
    await log.flush()
  } catch (err) {
    t.true(err.synthetic)
  }

  {
    const entries = []
    for await (const entry of log) {
      entries.push(entry)
    }
    t.same(entries.length, 2)
    t.same(entries[0].userData, Buffer.from('a'))
    t.same(entries[1].userData, Buffer.from('b'))
  }

  // Re-enable header writes
  storage[SHOULD_ERROR](false)
  await log.flush() // Should correctly truncate the oplog now

  {
    const entries = []
    for await (const entry of log) {
      entries.push(entry)
    }
    t.same(entries.length, 0)
  }

  await cleanup(storage)
  t.end()
})

function userData (s) {
  return { userData: Buffer.from(s) }
}

function testStorage () {
  return raf(STORAGE_FILE_NAME, { directory: __dirname, lock: fsctl.lock })
}

function failingOffsetStorage (offset) {
  let shouldError = false
  const storage = raf(STORAGE_FILE_NAME, { directory: __dirname, lock: fsctl.lock })
  const write = storage.write.bind(storage)

  storage.write = (off, data, cb) => {
    if (off < offset && shouldError) {
      const err = new Error('Synthetic write failure')
      err.synthetic = true
      return cb(err)
    }
    return write(off, data, cb)
  }
  storage[SHOULD_ERROR] = s => {
    shouldError = s
  }

  return storage
}

function cleanup (storage) {
  return fs.promises.unlink(p.join(__dirname, STORAGE_FILE_NAME))
}
