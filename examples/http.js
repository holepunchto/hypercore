const Hypercore = require('../')
const streamx = require('streamx')
const replicator = require('@hyperswarm/replicator')

const core = new Hypercore('/tmp/movie')

if (process.argv[2] === 'bench') bench()
else if (process.argv[2]) importData()
else start()

class ByteStream extends streamx.Readable {
  constructor (core, byteOffset, byteLength) {
    super()

    this.core = core
    this.byteOffset = byteOffset
    this.byteLength = byteLength
    this.index = 0
    this.range = null
  }

  async _read (cb) {
    let data = null

    if (!this.byteLength) {
      this.push(null)
      return cb(null)
    }

    if (this.byteOffset > 0) {
      const [block, byteOffset] = await core.seek(this.byteOffset)
      this.byteOffset = 0
      this.index = block + 1
      this._select(this.index)
      data = (await core.get(block)).slice(byteOffset)
    } else {
      this._select(this.index + 1)
      data = await core.get(this.index++)
    }

    if (data.length >= this.byteLength) {
      data = data.slice(0, this.byteLength)
      this.push(data)
      this.push(null)
    } else {
      this.push(data)
    }

    this.byteLength -= data.length

    cb(null)
  }

  _select (index) {
    if (this.range !== null) this.range.destroy(null)
    this.range = this.core.download({ start: index, end: index + 32, linear: true })
  }

  _destroy (cb) {
    if (this.range) this.range.destroy(null)
    cb(null)
  }
}

async function bench () {
  await core.ready()

  console.time()
  for (let i = 0; i < core.length; i++) {
    await core.get(i)
  }
  console.timeEnd()
}

async function start () {
  const http = require('http')
  const parse = require('range-parser')

  await core.ready()

  core.on('download', (index) => console.log('Downloaded block #' + index))
  core.download({ start: 0, end: 1 })

  // hack until we update the replicator
  core.ready = (cb) => cb(null)

  replicator(core, {
    discoveryKey: require('crypto').createHash('sha256').update('http').digest(),
    announce: true,
    lookup: true
  })

  http.createServer(function (req, res) {
    res.setHeader('Content-Type', 'video/x-matroska')
    res.setHeader('Accept-Ranges', 'bytes')

    let s

    if (req.headers.range) {
      const range = parse(core.byteLength, req.headers.range)[0]
      const byteLength = range.end - range.start + 1
      res.statusCode = 206
      res.setHeader('Content-Range', 'bytes ' + range.start + '-' + range.end + '/' + core.byteLength)
      s = new ByteStream(core, range.start, byteLength)
    } else {
      s = new ByteStream(core, 0, core.byteLength)
    }

    res.setHeader('Content-Length', s.byteLength)
    s.pipe(res, () => {})
  }).listen(10101)
}

async function importData () {
  const fs = require('fs')
  const rs = fs.createReadStream(process.argv[2])

  for await (const data of rs) {
    await core.append(data)
  }

  console.log('done!', core)
}
