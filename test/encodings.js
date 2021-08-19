const tape = require('tape')
const { create } = require('./helpers')

tape('encodings - supports built ins', async function (t) {
  const a = await create(null, { valueEncoding: 'json' })

  await a.append({ hello: 'world' })
  t.same(await a.get(0), { hello: 'world' })
  t.same(await a.get(0, { valueEncoding: 'utf-8' }), '{"hello":"world"}')
})

tape('encodings - supports custom encoding', async function (t) {
  const a = await create(null, { valueEncoding: { encode () { return Buffer.from('foo') }, decode () { return 'bar' } } })

  await a.append({ hello: 'world' })
  t.same(await a.get(0), 'bar')
  t.same(await a.get(0, { valueEncoding: 'utf-8' }), 'foo')
})
