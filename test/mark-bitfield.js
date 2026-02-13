const test = require('brittle')
const MarkBitfield = require('../lib/mark-bitfield')
const { once } = require('events')
const { createStorage } = require('./helpers')

test('MarkBitfield - basic', async (t) => {
  const core = await createCore(t)
  const bitfield = new MarkBitfield(core)

  await t.execution(() => bitfield.set(10, true), 'set')
  t.ok(await bitfield.get(10), 'get')
})

test('MarkBitfield - createMarkStream()', async (t) => {
  t.plan(4)

  const core = await createCore(t)
  const bitfield = new MarkBitfield(core)

  const expected = [10, 50000] // 50k for different page

  for (const i of expected) {
    await bitfield.set(i, true)
  }

  const stream = bitfield.createMarkStream()
  let i = 0
  stream.on('data', (index) => {
    t.is(expected[i++], index, 'got data')
  })

  await once(stream, 'end')

  const reverseStream = bitfield.createMarkStream({ reverse: true })
  reverseStream.on('data', (index) => {
    t.is(expected[--i], index, 'got data : reverse')
  })

  await once(reverseStream, 'end')
})

async function createCore(t) {
  const storage = await createStorage(t)
  return await storage.createCore({
    key: Buffer.alloc(32),
    discoveryKey: Buffer.alloc(32)
  })
}
