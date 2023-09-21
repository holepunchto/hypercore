const test = require('brittle')
const tmp = require('test-tmp')
const b4a = require('b4a')
const { create } = require('./helpers')
const Hypercore = require('../')

test('userdata - can set through setUserData', async function (t) {
  const core = await create()
  await core.setUserData('hello', b4a.from('world'))

  t.alike(await core.getUserData('hello'), b4a.from('world'))
})

test('userdata - can set through constructor option', async function (t) {
  const core = await create({
    userData: {
      hello: b4a.from('world')
    }
  })

  t.alike(await core.getUserData('hello'), b4a.from('world'))
})

test('userdata - persists across restarts', async function (t) {
  const dir = await tmp(t)

  let core = new Hypercore(dir, {
    userData: {
      hello: b4a.from('world')
    }
  })
  await core.ready()

  await core.close()
  core = new Hypercore(dir, {
    userData: {
      other: b4a.from('another')
    }
  })

  t.alike(await core.getUserData('hello'), b4a.from('world'))
  t.alike(await core.getUserData('other'), b4a.from('another'))

  await core.close()
})

test('userdata - big userdata gets swapped to external header', async function (t) {
  const core = await create()
  await core.setUserData('hello', b4a.alloc(20000))
  await core.setUserData('world', b4a.alloc(20000))
  await core.setUserData('world2', b4a.alloc(20000))

  t.alike(await core.getUserData('hello'), b4a.alloc(20000))
  t.alike(await core.getUserData('world'), b4a.alloc(20000))
  t.alike(await core.getUserData('world2'), b4a.alloc(20000))
})
