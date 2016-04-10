var tape = require('tape')
var protocol = require('../lib/protocol')

var key = Buffer('12345678123456781234567812345678')
// var otherKey = Buffer('12345678123456781234567812345679')

tape('open channel', function (t) {
  var p1 = protocol(function () {
    t.pass('channel opened')
    t.end()
    return {}
  })

  p1.open(key)
  p1.pipe(p1)
})

tape('send message', function (t) {
  var p1 = protocol(function () {
    t.pass('channel opened')
    return {
      onmessage: function (type, message) {
        t.same(type, 1, 'a have message')
        t.same(message.start, 1, 'start is 1')
        t.same(message.end, 5, 'end is 5')
        t.end()
      }
    }
  })

  var channel = p1.open(key)
  p1.send(channel, 1, {start: 1, end: 5}) // send have

  p1.pipe(p1)
})

tape('send message and close', function (t) {
  var p2 = protocol()
  var p1 = protocol(function () {
    t.pass('channel opened')
    return {
      onmessage: function (type, message) {
        t.same(type, 1, 'a have message')
        t.same(message.start, 1, 'start is 1')
        t.same(message.end, 5, 'end is 5')
      },
      onclose: function () {
        t.pass('channel closed')
        t.end()
      }
    }
  })

  var channel = p1.open(key)
  p1.send(channel, 1, {start: 1, end: 5}) // send have
  p1.close(channel)

  p1.pipe(p2).pipe(p1)
})

tape('send two messages', function (t) {
  t.plan(7)

  var p1 = protocol(function () {
    t.pass('channel opened')
    return {
      onmessage: function (type, message) {
        if (type === 1) {
          t.same(type, 1, 'a have message')
          t.same(message.start, 1, 'start is 1')
          t.same(message.end, 5, 'end is 5')
        } else {
          t.same(type, 2, 'a want message')
          t.same(message.start, 4, 'start is 4')
          t.same(message.end, 10, 'end is 10')
        }
      }
    }
  })

  var channel = p1.open(key)
  p1.send(channel, 1, {start: 1, end: 5}) // have message
  p1.send(channel, 2, {start: 4, end: 10}) // want message

  p1.pipe(p1)
})

tape('send extension message', function (t) {
  t.plan(5)

  var extensions = ['abe', 'fest']
  var p1 = protocol({extensions: extensions}, function () {
    t.pass('channel opened')
    return {
      onextension: function (type, buffer) {
        if (type === 0) {
          t.same(type, 0)
          t.same(buffer, Buffer('hello'), 'first extension')
        } else {
          t.same(type, 1)
          t.same(buffer, Buffer('world'), 'second extension')
        }
      }
    }
  })

  var channel = p1.open(key)
  p1.sendExtension(channel, 0, Buffer('hello'))
  p1.sendExtension(channel, 1, Buffer('world'))

  p1.pipe(p1)
})
