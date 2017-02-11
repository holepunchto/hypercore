// NOTES: rerunning this benchmark is *a lot* faster on the 2nd
// run. Can we gain massive perf by preallocating files?

var hypercore = require('../../')
var path = require('path')
var raf = require('random-access-file')

module.exports = function (dir, blockSize, count, finalize) {
  var feed = hypercore({live: !finalize, reset: true}, function (name) {
    return raf(path.join(__dirname, '../cores', dir, name))
  })

  var then = Date.now()
  var buf = new Buffer(blockSize)
  buf.fill(0)

  var blocks = []
  while (blocks.length < 128) blocks.push(buf)

  feed.append(blocks, function loop (err) {
    if (err) throw err
    if (feed.blocks < count) feed.append(blocks, loop)
    else if (finalize) feed.finalize(done)
    else done()
  })

  function done () {
    console.log(Math.floor(1000 * blockSize * feed.blocks / (Date.now() - then)) + ' bytes/s')
    console.log(Math.floor(1000 * feed.blocks / (Date.now() - then)) + ' blocks/s')
  }
}

