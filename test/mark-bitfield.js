const test = require('brittle')
const MarkBitfield = require('../lib/mark-bitfield')
const { createStorage } = require('./helpers')

test('MarkBitfield - basic', async (t) => {
  const core = await createCore(t)
  const bitfield = new MarkBitfield(core)

  await t.execution(() => bitfield.set(10, true), 'set')
  t.ok(await bitfield.get(10), 'get')
})

test('MarkBitfield - createMarkStream()', async (t) => {
  const core = await createCore(t)
  const bitfield = new MarkBitfield(core)

  const expected = [0, 32767, 32768, 50_000, 70_000]

  for (const i of expected) {
    await bitfield.set(i, true)
  }

  t.alike(await toArray(bitfield.createMarkStream()), expected)
  const reverseStream = bitfield.createMarkStream({ reverse: true })
  t.alike(await toArray(reverseStream), expected.reverse())
})

test('MarkBitfield - load from storage', async (t) => {
  const s = await createStorage(t)
  const storage = await s.createCore({
    key: Buffer.alloc(32),
    discoveryKey: Buffer.alloc(32)
  })

  {
    const marks = new MarkBitfield(storage)

    const expected = [
      0,
      MarkBitfield.BITS_PER_PAGE - 1,
      MarkBitfield.BITS_PER_PAGE,
      MarkBitfield.BITS_PER_PAGE * 2
    ]
    for (const i of expected) {
      await marks.set(i, true)
    }

    const results = await toArray(marks.createMarkStream())
    t.alike(results, expected, 'got stream of block indexes')
  }

  // Reloaded from storage
  {
    const marks = new MarkBitfield(storage)
    t.ok(await marks.get(0), '1st page loaded from storage')
    t.ok(await marks.get(MarkBitfield.BITS_PER_PAGE), '2nd page loaded from storage')

    await t.execution(marks.set(MarkBitfield.BITS_PER_PAGE * 2 + 1, true))
    t.ok(await marks.get(MarkBitfield.BITS_PER_PAGE * 2 + 1, true), '3rd page set')

    await marks.clear()

    const clearResults = await toArray(marks.createMarkStream())
    t.alike(clearResults, [], 'clear removed all bits')
  }
})

async function createCore(t) {
  const storage = await createStorage(t)
  return await storage.createCore({
    key: Buffer.alloc(32),
    discoveryKey: Buffer.alloc(32)
  })
}

async function toArray(stream) {
  const results = []

  for await (const chunk of stream) {
    results.push(chunk)
  }

  return results
}
