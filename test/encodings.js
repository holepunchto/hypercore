const test = require('brittle')
const { create } = require('./helpers')

test('encodings - supports built ins', async function (t) {
  const a = await create(null, { valueEncoding: 'json' })

  await a.append({ hello: 'world' })
  t.alike(await a.get(0), { hello: 'world' })
  t.alike(await a.get(0, { valueEncoding: 'utf-8' }), '{"hello":"world"}')
})

test('encodings - supports custom encoding', async function (t) {
  const a = await create(null, { valueEncoding: { encode () { return Buffer.from('foo') }, decode () { return 'bar' } } })

  await a.append({ hello: 'world' })
  t.is(await a.get(0), 'bar')
  t.alike(await a.get(0, { valueEncoding: 'utf-8' }), 'foo')
})
