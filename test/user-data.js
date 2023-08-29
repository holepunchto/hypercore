const test = require('brittle')
const Hypercore = require('../')
const tmp = require('test-tmp')
const { create } = require('./helpers')

test('userdata - can set through setUserData', async function (t) {
  const core = await create()
  await core.setUserData('hello', Buffer.from('world'))

  t.alike(await core.getUserData('hello'), Buffer.from('world'))
})

test('userdata - can set through constructor option', async function (t) {
  const core = await create({
    userData: {
      hello: Buffer.from('world')
    }
  })

  t.alike(await core.getUserData('hello'), Buffer.from('world'))
})

test('userdata - persists across restarts', async function (t) {
  const dir = await tmp(t)

  let core = new Hypercore(dir, {
    userData: {
      hello: Buffer.from('world')
    }
  })
  await core.ready()

  await core.close()
  core = new Hypercore(dir, {
    userData: {
      other: Buffer.from('another')
    }
  })

  t.alike(await core.getUserData('hello'), Buffer.from('world'))
  t.alike(await core.getUserData('other'), Buffer.from('another'))

  await core.close()
})

test('userdata - big userdata gets swapped to external header', async function (t) {
  const core = await create()
  await core.setUserData('hello', Buffer.alloc(20000))
  await core.setUserData('world', Buffer.alloc(20000))
  await core.setUserData('world2', Buffer.alloc(20000))

  t.alike(await core.getUserData('hello'), Buffer.alloc(20000))
  t.alike(await core.getUserData('world'), Buffer.alloc(20000))
  t.alike(await core.getUserData('world2'), Buffer.alloc(20000))
})
