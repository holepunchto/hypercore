var hypercore = require('./')

var feed = hypercore('./tmp', { valueEncoding: 'json' })

feed.append({
  hello: 'world'
})

feed.append({
  hej: 'verden'
})

feed.append({
  hola: 'mundo'
})

feed.flush(function () {
  console.log('Appended 3 more blocks, %d in total (%d bytes)\n', feed.length, feed.byteLength)

  feed.createReadStream()
    .on('data', console.log)
    .on('end', console.log.bind(console, '\n(end)'))
})
