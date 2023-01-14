const Hypercore = require('../')
const streamx = require('streamx')
const Hyperswarm = require('hyperswarm')

// Convert video into a core: node http.js import ./joker-scene.mp4
// Later replicate so other peers can also watch it: node http.js
// Other peers: node http.js <core key>

const key = process.argv[2] && process.argv[2] !== 'import' ? Buffer.from(process.argv[2], 'hex') : null
const core = new Hypercore('/tmp/movie' + (key ? '-peer' : ''), key)

if (process.argv[2] === 'import') importData(process.argv[3])
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
      const [block, byteOffset] = await this.core.seek(this.byteOffset)
      this.byteOffset = 0
      this.index = block + 1
      this._select(this.index)
      data = (await this.core.get(block)).subarray(byteOffset)
    } else {
      this._select(this.index + 1)
      data = await this.core.get(this.index++)
    }

    if (data.length >= this.byteLength) {
      data = data.subarray(0, this.byteLength)
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

async function start () {
  const http = require('http')
  const parse = require('range-parser')

  await core.ready()
  if (core.writable) console.log('Share this core key:', core.key.toString('hex'))

  core.on('download', (index) => console.log('Downloaded block #' + index))

  const swarm = new Hyperswarm()
  swarm.on('connection', (socket) => core.replicate(socket))
  swarm.join(core.discoveryKey)

  if (!core.writable) {
    console.log('Finding peers')
    const done = core.findingPeers()
    swarm.flush().then(done, done)
    await core.update()
  }

  http.createServer(function (req, res) {
    res.setHeader('Content-Type', 'video/mp4')
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
  }).listen(function () {
    console.log('HTTP server on http://localhost:' + this.address().port)
  })
}

async function importData (filename) {
  const fs = require('fs')
  const rs = fs.createReadStream(filename)

  for await (const data of rs) {
    await core.append(data)
  }

  console.log('done!', core)
}
