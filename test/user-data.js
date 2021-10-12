const test = require('brittle')
const Hypercore = require('../')
const tmp = require('tmp-promise')
const { create } = require('./helpers')

test('userdata - can set through setUserData', async function (t) {
  const core = await create()
  await core.setUserData('hello', Buffer.from('world'))

  t.alike(await core.getUserData('hello'), Buffer.from('world'))

  t.end()
})

test('userdata - can set through constructor option', async function (t) {
  const core = await create({
    userData: {
      hello: Buffer.from('world')
    }
  })

  t.alike(await core.getUserData('hello'), Buffer.from('world'))

  t.end()
})

test('userdata - persists across restarts', async function (t) {
  const dir = await tmp.dir()

  let core = new Hypercore(dir.path, {
    userData: {
      hello: Buffer.from('world')
    }
  })
  await core.ready()

  await core.close()
  core = new Hypercore(dir.path, {
    userData: {
      other: Buffer.from('another')
    }
  })

  t.alike(await core.getUserData('hello'), Buffer.from('world'))
  t.alike(await core.getUserData('other'), Buffer.from('another'))
  t.end()
})
