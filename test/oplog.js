const p = require('path')
const fs = require('fs')
const test = require('tape')
const fsctl = require('fsctl')
const raf = require('random-access-file')
const c = require('compact-encoding')

const OpLog = require('../lib/oplog')

const STORAGE_FILE_NAME = 'test-storage'

test('oplog - basic append', async function (t) {
  const storage = testStorage()

  const log = await OpLog.open(storage)
  await log.append({ flags: 1 })
  await log.append({ flags: 2 })

  let entries = []
  for await (const entry of log) {
    entries.push(entry)
  }

  t.same(entries.length, 2)
  t.same(entries[0].flags, 1)
  t.same(entries[0].flushes, 0)
  t.same(entries[1].flags, 2)
  t.same(entries[1].flushes, 0)

  await log.flush()

  entries = []
  for await (const entry of log) {
    entries.push(entry)
  }

  t.same(entries.length, 0)

  await log.append({ flags: 3 })

  entries = []
  for await (const entry of log) {
    entries.push(entry)
  }

  t.same(entries.length, 1)
  t.same(entries[0].flags, 3)
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
    storage.write(4096 + 1 + 4 , c.encode(badHeaderEncoding, { test: 1 }), err => {
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
  appends.push(log.append({ flags: 1 }))
  appends.push(log.append({ flags: 2 }))

  const res = await Promise.allSettled(appends)
  t.same(res[0].status, 'fulfilled')
  t.same(res[1].status, 'rejected')

  await log.append({ flags: 3 })

  const entries = []
  for await (const entry of log) {
    entries.push(entry)
  }

  t.same(entries.length, 2)
  t.same(entries[0].flags, 1)
  t.same(entries[1].flags, 3)

  await cleanup(storage)
  t.end()
})

function testStorage () {
  return raf(STORAGE_FILE_NAME, { directory: __dirname, lock: fsctl.lock })
}

function cleanup (storage) {
  return fs.promises.unlink(p.join(__dirname, STORAGE_FILE_NAME))
}
