const create = require('./helpers/create')
const replicate = require('./helpers/replicate')
const tape = require('tape')

tape('head without update does not update', t => {
  const feed1 = create()
  let feed2 = null

  feed1.append('hello', () => {
    feed2 = create(feed1.key)
    replicate(feed1, feed2, { live: true })
    feed2.head((err, content) => {
      t.true(err)
      t.end()
    })
  })
})

tape('head with update waits for an update', t => {
  const feed1 = create()
  let feed2 = null

  feed1.append('hello', () => {
    feed2 = create(feed1.key)
    feed2.head({ update: true }, (err, content) => {
      t.error(err, 'no error')
      t.same(content, Buffer.from('hello'))
      t.end()
    })
    setTimeout(() => {
      replicate(feed1, feed2)
    }, 50)
  })
})

tape('head with update/ifAvailable will wait only if an update is available', t => {
  const feed1 = create()
  let feed2 = null

  feed1.append('hello', () => {
    feed2 = create(feed1.key)
    feed2.head({ update: true, ifAvailable: true }, (err, content) => {
      t.error(err, 'no error')
      t.same(content, Buffer.from('hello'))
      t.end()
    })
    replicate(feed1, feed2)
  })
})

tape('head with update/ifAvailable will not wait forever', t => {
  const feed1 = create()
  let feed2 = null

  feed1.append('hello', () => {
    feed2 = create(feed1.key)
    feed2.head({ update: true, ifAvailable: true }, (err, content) => {
      t.true(err)
      t.end()
    })
    setTimeout(() => {
      replicate(feed1, feed2)
    }, 50)
  })
})
