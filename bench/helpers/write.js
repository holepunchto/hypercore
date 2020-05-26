// NOTES: rerunning this benchmark is *a lot* faster on the 2nd
// run. Can we gain massive perf by preallocating files?

var hypercore = require('../../')
var path = require('path')
var bufferAlloc = require('buffer-alloc-unsafe')

module.exports = function (dir, blockSize, count, finalize) {
  var feed = hypercore(path.join(__dirname, '../cores', dir), { live: !finalize, overwrite: true })

  var then = Date.now()
  var buf = bufferAlloc(blockSize)
  buf.fill(0)

  var blocks = []
  while (blocks.length < 128) blocks.push(buf)

  feed.append(blocks, function loop (err) {
    if (err) throw err
    if (feed.length < count) feed.append(blocks, loop)
    else if (finalize) feed.finalize(done)
    else done()
  })

  function done () {
    console.log(Math.floor(1000 * blockSize * feed.length / (Date.now() - then)) + ' bytes/s')
    console.log(Math.floor(1000 * feed.length / (Date.now() - then)) + ' blocks/s')
  }
}
