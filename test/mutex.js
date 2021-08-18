const tape = require('tape')
const Mutex = require('../lib/mutex')

tape('mutex - lock after destroy', async function (t) {
  const mutex = new Mutex()
  mutex.destroy()
  try {
    await mutex.lock()
    t.fail('should not be able to lock after destroy')
  } catch {
    t.pass('lock threw after destroy')
  }
})

tape('mutex - graceful destroy', async function (t) {
  t.plan(1)

  const mutex = new Mutex()
  const promises = []
  let resolveCount = 0

  for (let i = 0; i < 5; i++) {
    promises.push(mutex.lock().then(() => resolveCount++))
  }

  const destroyed = mutex.destroy()

  for (let i = 0; i < 5; i++) mutex.unlock()

  await destroyed

  t.same(resolveCount, 5)
})

tape('mutex - quick destroy', async function (t) {
  t.plan(2)

  const mutex = new Mutex()
  const promises = []
  let rejectCount = 0
  let resolveCount = 0

  for (let i = 0; i < 5; i++) {
    promises.push(mutex.lock().then(() => resolveCount++, () => rejectCount++))
  }

  const destroyed = mutex.destroy(new Error('Test error'))

  for (let i = 0; i < 5; i++) mutex.unlock()

  await destroyed

  t.same(resolveCount, 1)
  t.same(rejectCount, 4)
})

tape('mutex - graceful then quick destroy', async function (t) {
  t.plan(2)

  const mutex = new Mutex()
  const promises = []
  let rejectCount = 0
  let resolveCount = 0

  for (let i = 0; i < 5; i++) {
    promises.push(mutex.lock().then(() => resolveCount++, () => rejectCount++))
  }

  const destroyed = mutex.destroy()
  mutex.destroy(new Error('Test error'))

  for (let i = 0; i < 5; i++) mutex.unlock()

  await destroyed

  t.same(resolveCount, 1)
  t.same(rejectCount, 4)
})
