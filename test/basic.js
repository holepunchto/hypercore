var create = require('./helpers/create')
var sodium = require('sodium-universal')
var tape = require('tape')
var hypercore = require('../')
var ram = require('random-access-memory')

tape('append', function (t) {
  t.plan(8)

  var feed = create({valueEncoding: 'json'})

  feed.append({
    hello: 'world'
  })

  feed.append([{
    hello: 'verden'
  }, {
    hello: 'welt'
  }])

  feed.flush(function () {
    t.same(feed.length, 3, '3 blocks')
    t.same(feed.byteLength, 54, '54 bytes')

    feed.get(0, function (err, value) {
      t.error(err, 'no error')
      t.same(value, {hello: 'world'})
    })

    feed.get(1, function (err, value) {
      t.error(err, 'no error')
      t.same(value, {hello: 'verden'})
    })

    feed.get(2, function (err, value) {
      t.error(err, 'no error')
      t.same(value, {hello: 'welt'})
    })
  })
})

tape('flush', function (t) {
  var feed = create()

  feed.append('hello')

  feed.flush(function (err) {
    t.error(err, 'no error')
    t.same(feed.length, 1, '1 block')
    t.end()
  })
})

tape('pass in secret key', function (t) {
  var secretKey = new Buffer(sodium.crypto_sign_SECRETKEYBYTES)
  var key = new Buffer(sodium.crypto_sign_PUBLICKEYBYTES)
  sodium.crypto_sign_keypair(key, secretKey)

  var feed = create(key, {secretKey: secretKey})

  feed.on('ready', function () {
    t.same(feed.key, key)
    t.same(feed.secretKey, secretKey)
    t.ok(feed.writable)
    t.end()
  })
})

tape('check existing key', function (t) {
  var feed = hypercore(storage)

  feed.append('hi', function () {
    var key = new Buffer(32)
    key.fill(0)
    var otherFeed = hypercore(storage, key)
    otherFeed.on('error', function () {
      t.pass('should error')
      t.end()
    })
  })

  function storage (name) {
    if (storage[name]) return storage[name]
    storage[name] = ram()
    return storage[name]
  }
})
