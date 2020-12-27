var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var tape = require('tape')

tape('head without update does not update', t => {
  var feed1 = create()
  var feed2 = null

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
  var feed1 = create()
  var feed2 = null

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
  var feed1 = create()
  var feed2 = null

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
  var feed1 = create()
  var feed2 = null

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
