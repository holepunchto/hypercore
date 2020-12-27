const tape = require('tape')
const create = require('./helpers/create')

tape('basic value encoding', function (t) {
  const feed = create({
    valueEncoding: 'json'
  })

  feed.append({ hello: 'world' }, function () {
    feed.get(0, function (err, val) {
      t.error(err, 'no error')
      t.same(val, { hello: 'world' })
      t.end()
    })
  })
})

tape('value encoding read-stream', function (t) {
  const feed = create({
    valueEncoding: 'json'
  })

  feed.append({ hello: 'world' }, function () {
    feed.createReadStream()
      .on('data', function (data) {
        t.same(data, { hello: 'world' })
      })
      .on('end', function () {
        feed.createReadStream({ valueEncoding: 'utf-8' })
          .on('data', function (data) {
            t.same(data, '{"hello":"world"}\n')
            t.end()
          })
      })
  })
})

tape('value encoding write-stream', function (t) {
  const feed = create({
    valueEncoding: 'json'
  })

  const ws = feed.createWriteStream()
  ws.write([1, 2, 3])
  ws.end(function () {
    feed.get(0, function (err, val) {
      t.error(err, 'no error')
      t.same(val, [1, 2, 3])
      t.end()
    })
  })
})
