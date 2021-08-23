const tape = require('tape')
const Hypercore = require('../')
const tmp = require('tmp-promise')
const { create } = require('./helpers')

tape('userdata - can set through setUserData', async function (t) {
  const core = await create()
  await core.setUserData('hello', Buffer.from('world'))

  t.same(await core.getUserData('hello'), Buffer.from('world'))

  t.end()
})

tape('userdata - can set through constructor option', async function (t) {
  const core = await create({
    userData: {
      hello: Buffer.from('world')
    }
  })

  t.same(await core.getUserData('hello'), Buffer.from('world'))

  t.end()
})

tape('userdata - persists across restarts', async function (t) {
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

  t.same(await core.getUserData('hello'), Buffer.from('world'))
  t.same(await core.getUserData('other'), Buffer.from('another'))
  t.end()
})
