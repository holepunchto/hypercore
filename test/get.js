const create = require('./helpers/create')
const replicate = require('./helpers/replicate')
const tape = require('tape')

tape('get with ifAvailable waits correctly', t => {
  const feed1 = create()
  let feed2 = null

  feed1.append('hello', () => {
    feed2 = create(feed1.key)
    feed2.get(0, { ifAvailable: true }, (err, content) => {
      t.error(err, 'no error')
      t.same(content, Buffer.from('hello'))
      t.end()
    })
    replicate(feed1, feed2)
  })
})

tape('get with ifAvailable returns null for unavilable index', t => {
  const feed1 = create()
  let feed2 = null

  feed1.append('hello', () => {
    feed2 = create(feed1.key)
    feed2.get(1, { ifAvailable: true }, (err, content) => {
      t.true(err)
      t.end()
    })
    replicate(feed1, feed2)
  })
})

tape('get with ifAvailable does not wait forever', t => {
  const feed1 = create()
  let feed2 = null

  feed1.append('hello', () => {
    feed2 = create(feed1.key)
    feed2.get(0, { ifAvailable: true }, (err, content) => {
      t.true(err)
      t.end()
    })
    setTimeout(() => {
      replicate(feed1, feed2)
    }, 50)
  })
})

tape('get with top-level ifAvailable option does not wait forever', t => {
  const feed1 = create()
  let feed2 = null

  feed1.append('hello', () => {
    feed2 = create(feed1.key, { ifAvailable: true })
    feed2.get(0, (err, content) => {
      t.true(err)
      t.end()
    })
    setTimeout(() => {
      replicate(feed1, feed2)
    }, 50)
  })
})
