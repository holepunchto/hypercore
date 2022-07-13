const p = require('path')
const fs = require('fs')
const test = require('brittle')
const RAF = require('random-access-file')
const c = require('compact-encoding')

const Oplog = require('../lib/oplog')

const STORAGE_FILE_NAME = 'oplog-test-storage'
const STORAGE_FILE_PATH = p.join(__dirname, STORAGE_FILE_NAME)
const SHOULD_ERROR = Symbol('hypercore-oplog-should-error')

test.configure({ serial: true })

test('oplog - reset storage', async function (t) {
  // just to make sure to cleanup storage if it failed half way through before
  if (fs.existsSync(STORAGE_FILE_PATH)) await fs.promises.unlink(STORAGE_FILE_PATH)
  t.pass('data is reset')
  t.end()
})

test('oplog - basic append', async function (t) {
  const storage = testStorage()

  const logWr = new Oplog(storage)

  await logWr.open()
  await logWr.flush(Buffer.from('h'))
  await logWr.append(Buffer.from('a'))
  await logWr.append(Buffer.from('b'))

  const logRd = new Oplog(storage)

  {
    const { header, entries } = await logRd.open()

    t.alike(header, Buffer.from('h'))
    t.is(entries.length, 2)
    t.alike(entries[0], Buffer.from('a'))
    t.alike(entries[1], Buffer.from('b'))
  }

  await logWr.flush(Buffer.from('i'))

  {
    const { header, entries } = await logRd.open()

    t.alike(header, Buffer.from('i'))
    t.is(entries.length, 0)
  }

  await logWr.append(Buffer.from('c'))

  {
    const { header, entries } = await logRd.open()

    t.alike(header, Buffer.from('i'))
    t.is(entries.length, 1)
    t.alike(entries[0], Buffer.from('c'))
  }

  await cleanup(storage)
  t.end()
})

test('oplog - custom encoding', async function (t) {
  const storage = testStorage()

  const log = new Oplog(storage, {
    headerEncoding: c.string,
    entryEncoding: c.uint
  })

  await log.open()
  await log.flush('one header')
  await log.append(42)
  await log.append(43)

  const { header, entries } = await log.open()

  t.is(header, 'one header')
  t.is(entries.length, 2)
  t.is(entries[0], 42)
  t.is(entries[1], 43)

  await cleanup(storage)
  t.end()
})

test('oplog - alternating header writes', async function (t) {
  const storage = testStorage()

  const log = new Oplog(storage)

  await log.open()
  await log.flush(Buffer.from('1'))
  await log.flush(Buffer.from('2'))

  {
    const { header } = await log.open()
    t.alike(header, Buffer.from('2'))
  }

  await log.flush(Buffer.from('1')) // Should overwrite first header

  {
    const { header } = await log.open()
    t.alike(header, Buffer.from('1'))
  }

  await log.flush(Buffer.from('2')) // Should overwrite second header

  {
    const { header } = await log.open()
    t.alike(header, Buffer.from('2'))
  }

  await cleanup(storage)
  t.end()
})

test('oplog - one fully-corrupted header', async function (t) {
  const storage = testStorage()

  const log = new Oplog(storage)

  await log.open()
  await log.flush(Buffer.from('header 1'))

  {
    const { header } = await log.open()
    t.alike(header, Buffer.from('header 1'))
  }

  await log.flush(Buffer.from('header 2'))

  {
    const { header } = await log.open()
    t.alike(header, Buffer.from('header 2'))
  }

  await log.flush(Buffer.from('header 3')) // should overwrite first header

  {
    const { header } = await log.open()
    t.alike(header, Buffer.from('header 3'))
  }

  // Corrupt the first header -- second header should win now
  await new Promise((resolve, reject) => {
    storage.write(0, Buffer.from('hello world'), err => {
      if (err) return reject(err)
      return resolve()
    })
  })

  {
    const { header } = await log.open()
    t.alike(header, Buffer.from('header 2'), 'one is corrupted or partially written')
  }

  await cleanup(storage)
  t.end()
})

test('oplog - header invalid checksum', async function (t) {
  const storage = testStorage()

  const log = new Oplog(storage)

  await log.open()
  await log.flush(Buffer.from('a'))
  await log.flush(Buffer.from('b'))

  {
    const { header } = await log.open()
    t.alike(header, Buffer.from('b'))
  }

  // Invalidate the first header's checksum -- second header should win now
  await new Promise((resolve, reject) => {
    storage.write(4096 + 8, Buffer.from('a'), err => {
      if (err) return reject(err)
      return resolve()
    })
  })

  {
    const { header } = await log.open()
    t.alike(header, Buffer.from('a'))
  }

  // Invalidate the second header's checksum -- the hypercore is now corrupted
  await new Promise((resolve, reject) => {
    storage.write(8, Buffer.from('b'), err => {
      if (err) return reject(err)
      return resolve()
    })
  })

  try {
    await log.open()
    t.fail('corruption should have been detected')
  } catch {
    t.pass('corruption was correctly detected')
  }

  await cleanup(storage)
  t.end()
})

test('oplog - malformed log entry gets overwritten', async function (t) {
  let storage = testStorage()
  let log = new Oplog(storage)

  await log.flush(Buffer.from('header'))
  await log.append(Buffer.from('a'))
  await log.append(Buffer.from('b'))
  await log.close()

  const offset = log.byteLength

  storage = testStorage()
  log = new Oplog(storage)

  // Write a bad oplog message at the end (simulates a failed append)
  await new Promise((resolve, reject) => {
    storage.write(offset + 4096 * 2, Buffer.from([0, 0, 0, 0, 4, 0, 0, 0]), err => {
      if (err) return reject(err)
      return resolve()
    })
  })

  {
    const { entries } = await log.open()

    t.is(entries.length, 2) // The partial entry should not be present
    t.alike(entries[0], Buffer.from('a'))
    t.alike(entries[1], Buffer.from('b'))
  }

  // Write a valid oplog message now
  await log.append(Buffer.from('c'))

  {
    const { entries } = await log.open()

    t.is(entries.length, 3) // The partial entry should not be present
    t.alike(entries[0], Buffer.from('a'))
    t.alike(entries[1], Buffer.from('b'))
    t.alike(entries[2], Buffer.from('c'))
  }

  await cleanup(storage)
  t.end()
})

test('oplog - log not truncated when header write fails', async function (t) {
  const storage = failingOffsetStorage(4096 * 2)

  const log = new Oplog(storage)

  await log.flush(Buffer.from('header'))
  await log.append(Buffer.from('a'))
  await log.append(Buffer.from('b'))

  // Make subsequent header writes fail
  storage[SHOULD_ERROR](true)

  // The flush should fail because the header can't be updated -- log should still have entries after this
  try {
    await log.flush(Buffer.from('header two'))
  } catch (err) {
    t.ok(err.synthetic)
  }

  {
    const { header, entries } = await log.open()

    t.alike(header, Buffer.from('header'))
    t.is(entries.length, 2)
    t.alike(entries[0], Buffer.from('a'))
    t.alike(entries[1], Buffer.from('b'))
  }

  // Re-enable header writes
  storage[SHOULD_ERROR](false)
  await log.flush(Buffer.from('header two')) // Should correctly truncate the oplog now

  {
    const { header, entries } = await log.open()

    t.alike(header, Buffer.from('header two'))
    t.is(entries.length, 0)
  }

  await cleanup(storage)
  t.end()
})

test('oplog - multi append', async function (t) {
  const storage = testStorage()

  const log = new Oplog(storage)

  await log.open()
  await log.flush(Buffer.from('a'))

  await log.append([
    Buffer.from('1'),
    Buffer.from('22'),
    Buffer.from('333'),
    Buffer.from('4')
  ])

  t.is(log.length, 4)
  t.is(log.byteLength, 32 + 1 + 2 + 3 + 1)

  const { header, entries } = await log.open()

  t.alike(header, Buffer.from('a'))
  t.alike(entries, [
    Buffer.from('1'),
    Buffer.from('22'),
    Buffer.from('333'),
    Buffer.from('4')
  ])

  await cleanup(storage)
  t.end()
})

test('oplog - multi append is atomic', async function (t) {
  const storage = testStorage()

  const log = new Oplog(storage)

  await log.open()
  await log.flush(Buffer.from('a'))

  await log.append(Buffer.from('0'))
  await log.append([
    Buffer.from('1'),
    Buffer.from('22'),
    Buffer.from('333'),
    Buffer.from('4')
  ])

  t.is(log.length, 5)
  t.is(log.byteLength, 40 + 1 + 1 + 2 + 3 + 1)

  // Corrupt the last write, should revert the full batch
  await new Promise((resolve, reject) => {
    storage.write(8192 + log.byteLength - 1, Buffer.from('x'), err => {
      if (err) return reject(err)
      return resolve()
    })
  })

  const { entries } = await log.open()

  t.is(log.length, 1)
  t.alike(entries, [
    Buffer.from('0')
  ])

  await cleanup(storage)
  t.end()
})

function testStorage () {
  return new RAF(STORAGE_FILE_NAME, { directory: __dirname, lock: true })
}

function failingOffsetStorage (offset) {
  let shouldError = false
  const storage = new RAF(STORAGE_FILE_NAME, { directory: __dirname, lock: true })
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

async function cleanup (storage) {
  await new Promise((resolve) => storage.close(() => resolve()))
  await fs.promises.unlink(STORAGE_FILE_PATH)
}
