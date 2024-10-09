const test = require('brittle')
const tmp = require('test-tmp')
const CoreStorage = require('hypercore-on-the-rocks')
const b4a = require('b4a')
const MemoryOverlay = require('../lib/memory-overlay')

const KEY = Buffer.alloc(32).fill('dk0')

test('memory overlay - write + read', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  const w = overlay.createWriteBatch()
  w.putBlock(0, b4a.from('hello'))
  w.putBlock(1, b4a.from('world'))
  w.flush()

  const r = overlay.createReadBatch()
  const p0 = r.getBlock(0)
  const p1 = r.getBlock(1)
  r.tryFlush()

  t.alike(await p0, b4a.from('hello'))
  t.alike(await p1, b4a.from('world'))
})

test('memory overlay - multiple writes', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(0, b4a.from('hello'))
    w.putBlock(1, b4a.from('world'))
    w.flush()
  }

  {
    const r = overlay.createReadBatch()
    const p0 = r.getBlock(0)
    const p1 = r.getBlock(1)
    r.tryFlush()

    t.alike(await p0, b4a.from('hello'))
    t.alike(await p1, b4a.from('world'))
  }

  {
    const w = overlay.createWriteBatch()
    w.putBlock(2, b4a.from('goodbye'))
    w.putBlock(3, b4a.from('test'))
    w.flush()
  }

  {
    const r = overlay.createReadBatch()
    const p2 = r.getBlock(2)
    const p3 = r.getBlock(3)
    r.tryFlush()

    t.alike(await p2, b4a.from('goodbye'))
    t.alike(await p3, b4a.from('test'))
  }
})

test('memory overlay - overwrite', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(0, b4a.from('hello'))
    w.putBlock(1, b4a.from('world'))
    w.flush()
  }

  {
    const w = overlay.createWriteBatch()
    w.putBlock(0, b4a.from('goodbye'))
    w.putBlock(1, b4a.from('test'))
    w.flush()
  }

  {
    const r = overlay.createReadBatch()
    const p0 = r.getBlock(0)
    const p1 = r.getBlock(1)
    r.tryFlush()

    t.alike(await p0, b4a.from('goodbye'))
    t.alike(await p1, b4a.from('test'))
  }
})

test('memory overlay - write + read with offset', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  const w = overlay.createWriteBatch()
  w.putBlock(3, b4a.from('hello'))
  w.putBlock(4, b4a.from('world'))
  w.flush()

  const r = overlay.createReadBatch()
  const p0 = r.getBlock(0)
  const p3 = r.getBlock(3)
  const p4 = r.getBlock(4)
  r.tryFlush()

  t.alike(await p0, null)
  t.alike(await p3, b4a.from('hello'))
  t.alike(await p4, b4a.from('world'))
})

test('memory overlay - multiple writes with offset', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(5, b4a.from('hello'))
    w.putBlock(6, b4a.from('world'))
    w.flush()
  }

  {
    const w = overlay.createWriteBatch()
    w.putBlock(7, b4a.from('goodbye'))
    w.putBlock(8, b4a.from('test'))
    w.flush()
  }

  {
    const r = overlay.createReadBatch()

    const p5 = r.getBlock(5)
    const p6 = r.getBlock(6)
    const p7 = r.getBlock(7)
    const p8 = r.getBlock(8)

    r.tryFlush()

    t.alike(await p5, b4a.from('hello'))
    t.alike(await p6, b4a.from('world'))
    t.alike(await p7, b4a.from('goodbye'))
    t.alike(await p8, b4a.from('test'))
  }
})

test('memory overlay - overwrite with offset', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(3, b4a.from('hello'))
    w.putBlock(4, b4a.from('world'))
    w.flush()
  }

  {
    const w = overlay.createWriteBatch()
    w.putBlock(3, b4a.from('goodbye'))
    w.putBlock(4, b4a.from('test'))
    w.flush()
  }

  {
    const r = overlay.createReadBatch()
    const p0 = r.getBlock(0)
    const p3 = r.getBlock(3)
    const p4 = r.getBlock(4)
    r.tryFlush()

    t.alike(await p0, null)
    t.alike(await p3, b4a.from('goodbye'))
    t.alike(await p4, b4a.from('test'))
  }
})

test('memory overlay - deletion', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(0, b4a.from('hello'))
    w.putBlock(1, b4a.from('world'))
    w.putBlock(2, b4a.from('goodbye'))
    w.flush()
  }

  {
    const w = overlay.createWriteBatch()
    w.deleteBlockRange(1, 10)

    t.exception(() => w.putBlock(2, b4a.from('fail')), 'no put after deletion')

    w.flush()
  }

  {
    const r = overlay.createReadBatch()
    const p0 = r.getBlock(0)
    const p1 = r.getBlock(1)
    const p2 = r.getBlock(2)
    r.tryFlush()

    t.alike(await p0, b4a.from('hello'))
    t.alike(await p1, null)
    t.alike(await p2, null)
  }
})

test('memory overlay - deletion on put batch', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(0, b4a.from('hello'))
    w.putBlock(1, b4a.from('world'))
    w.putBlock(2, b4a.from('goodbye'))
    w.deleteBlockRange(2, 10)
    w.flush()
  }

  {
    const r = overlay.createReadBatch()
    const p0 = r.getBlock(0)
    const p1 = r.getBlock(1)
    const p2 = r.getBlock(2)
    r.tryFlush()

    t.alike(await p0, b4a.from('hello'))
    t.alike(await p1, b4a.from('world'))
    t.alike(await p2, null)
  }
})

test('memory overlay - deletion with offset', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(4, b4a.from('hello'))
    w.putBlock(5, b4a.from('world'))
    w.putBlock(6, b4a.from('goodbye'))
    w.flush()
  }

  {
    const w = overlay.createWriteBatch()
    w.deleteBlockRange(5, 10)
    w.flush()
  }

  {
    const r = overlay.createReadBatch()
    const p4 = r.getBlock(4)
    const p5 = r.getBlock(5)
    const p6 = r.getBlock(6)
    r.tryFlush()

    t.alike(await p4, b4a.from('hello'))
    t.alike(await p5, null)
    t.alike(await p6, null)
  }
})

test('memory overlay - overlap deletions in batch', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(0, b4a.from('hello'))
    w.putBlock(1, b4a.from('world'))
    w.putBlock(2, b4a.from('goodbye'))
    w.putBlock(3, b4a.from('test'))
    w.flush()
  }

  {
    const w = overlay.createWriteBatch()
    w.deleteBlockRange(3, 4)
    w.deleteBlockRange(2, 3)
    w.deleteBlockRange(1, 2)
    w.flush()
  }

  {
    const r = overlay.createReadBatch()
    const p0 = r.getBlock(0)
    const p1 = r.getBlock(1)
    const p2 = r.getBlock(2)
    const p3 = r.getBlock(3)
    r.tryFlush()

    t.alike(await p0, b4a.from('hello'))
    t.alike(await p1, null)
    t.alike(await p2, null)
    t.alike(await p3, null)
  }
})

test('memory overlay - overlap deletions in batch with offset', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(5, b4a.from('hello'))
    w.putBlock(6, b4a.from('world'))
    w.putBlock(7, b4a.from('goodbye'))
    w.putBlock(8, b4a.from('test'))
    w.flush()
  }

  {
    const w = overlay.createWriteBatch()
    w.deleteBlockRange(8, 10)
    w.deleteBlockRange(7, 8)
    w.deleteBlockRange(6, 7)
    w.flush()
  }

  {
    const r = overlay.createReadBatch()
    const p5 = r.getBlock(5)
    const p6 = r.getBlock(6)
    const p7 = r.getBlock(7)
    const p8 = r.getBlock(8)
    r.tryFlush()

    t.alike(await p5, b4a.from('hello'))
    t.alike(await p6, null)
    t.alike(await p7, null)
    t.alike(await p8, null)
  }
})

test('memory overlay - invalid deletion', async function (t) {
  const storage = await getCore(t)
  const overlay = new MemoryOverlay(storage)

  {
    const w = overlay.createWriteBatch()
    w.putBlock(4, b4a.from('hello'))
    w.putBlock(5, b4a.from('world'))
    w.putBlock(6, b4a.from('goodbye'))

    await t.exception(() => w.deleteBlockRange(0, 2))
    await t.exception(() => w.deleteBlockRange(8, 10))

    w.flush()
  }

  {
    const w = overlay.createWriteBatch()
    w.deleteBlockRange(0, 2)
    t.exception(() => w.flush())
  }

  {
    const w = overlay.createWriteBatch()
    w.deleteBlockRange(8, 10)
    t.exception(() => w.flush())
  }

  {
    const r = overlay.createReadBatch()
    const p4 = r.getBlock(4)
    const p5 = r.getBlock(5)
    const p6 = r.getBlock(6)
    r.tryFlush()

    t.alike(await p4, b4a.from('hello'))
    t.alike(await p5, b4a.from('world'))
    t.alike(await p6, b4a.from('goodbye'))
  }
})

async function getStorage (t, dir) {
  if (!dir) dir = await tmp(t)
  const s = new CoreStorage(dir)

  t.teardown(() => s.close())

  return s
}

async function getCore (t, s) {
  if (!s) s = await getStorage(t)

  const c = await s.resume(KEY)
  t.is(c, null)

  return await s.create({ key: KEY, discoveryKey: KEY })
}
