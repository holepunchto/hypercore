const path = require('path')
const hypercore = require('../../')

module.exports = function (dir, proof) {
  const feed = hypercore(path.join(__dirname, '../cores', dir))

  const then = Date.now()
  let size = 0
  let cnt = 0

  feed.ready(function () {
    let missing = feed.length
    let reading = 0

    for (let i = 0; i < 16; i++) read(null, null)

    function read (err, data) {
      if (err) throw err

      if (data) {
        reading--
        cnt++
        size += data.length
      }

      if (!missing) {
        if (!reading) {
          console.log(Math.floor(1000 * size / (Date.now() - then)) + ' bytes/s')
          console.log(Math.floor(1000 * cnt / (Date.now() - then)) + ' blocks/s')
        }
        return
      }

      missing--
      reading++

      const block = Math.floor(Math.random() * feed.length)

      if (proof) feed.proof(block, onproof)
      else onproof()

      function onproof () {
        feed.get(block, read)
      }
    }
  })
}
