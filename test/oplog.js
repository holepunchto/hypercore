const p = require('path')
const fs = require('fs')
const test = require('tape')
const fsctl = require('fsctl')
const raf = require('random-access-file')

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

function testStorage () {
  return raf(STORAGE_FILE_NAME, { directory: __dirname, lock: fsctl.lock })
}

function cleanup (storage) {
  return fs.promises.unlink(p.join(__dirname, STORAGE_FILE_NAME))
}
