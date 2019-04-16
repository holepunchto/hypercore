var path = require('path')
var speedometer = require('speedometer')
var hypercore = require('../../')

module.exports = function (dir, proof) {
  var speed = speedometer()
  var feed = hypercore(path.join(__dirname, '../cores', dir))

  feed.ready(function () {
    for (var i = 0; i < 16; i++) read()

    function read (err, data) {
      if (speed() > 10000000) return setTimeout(read, 250)
      if (err) throw err
      if (data) speed(data.length)

      var next = Math.floor(Math.random() * feed.length)
      feed.get(next, read)
      if (proof) feed.proof(next, noop)
    }
  })

  process.title = 'hypercore-read-10mb'
  console.log('Reading data at ~10mb/s. Pid is %d', process.pid)
}

function noop () {}
