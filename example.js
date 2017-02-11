var hypercore = require('./')
var raf = require('random-access-file')

var append = false

var w = hypercore({valueEncoding: 'json'}, function (name) {
  return raf('tmp/' + name)
})

w.ready(function () {
  console.log('Contains %d blocks and %d bytes (live: %s)\n', w.blocks, w.bytes, w.live)

  w.createReadStream()
    .on('data', console.log)
    .on('end', console.log.bind(console, '\n(end)'))
})

if (append) {
  w.append({
    hello: 'world'
  })

  w.append({
    hej: 'verden'
  })

  w.append({
    hola: 'mundo'
  })

  w.flush(function () {
    console.log('Appended 3 more blocks')
  })
}
