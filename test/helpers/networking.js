const UDX = require('udx-native')
const safetyCatch = require('safety-catch')
const NoiseStream = require('@hyperswarm/secret-stream')
const rrp = require('resolve-reject-promise')

module.exports = {
  makeStreamPair
}

function makeStreamPair(t, opts = {}) {
  const u = new UDX()
  const a = u.createSocket()
  const b = u.createSocket()

  // need to close after their streams
  t.teardown(() => a.close(), { order: 3 })
  t.teardown(() => b.close(), { order: 4 })

  a.bind(0, '127.0.0.1')
  b.bind(0, '127.0.0.1')

  const p = proxy({ from: a, to: b }, async function () {
    const delay =
      opts.latency[0] +
      Math.round(Math.random() * (opts.latency[1] - opts.latency[0]))
    if (delay) {
      const { promise, resolve } = rrp()
      const timeout = setTimeout(resolve, delay)
      t.teardown(() => {
        // If the test ends before our delay triggered
        clearTimeout(timeout)
        resolve()
      })
      await promise
    }
    return false
  })

  // Needs to be torn down early, otherwise proxy calls can trigger while
  // already tearing down, meaning their timeouts won't be cleared
  t.teardown(() => p.close(), { order: 0 })

  const s1 = u.createStream(1)
  const s2 = u.createStream(2)

  s1.connect(a, 2, p.address().port)
  s2.connect(b, 1, p.address().port)

  t.teardown(() => s1.destroy())
  t.teardown(() => s2.destroy())

  const n1 = new NoiseStream(true, s1)
  const n2 = new NoiseStream(false, s2)

  return [n1, n2]
}

function proxy({ from, to, bind } = {}, handler) {
  from = from.address().port
  to = to.address().port

  const u = new UDX()
  const socket = u.createSocket()

  socket.on('message', function (buf, rinfo) {
    const forwarding = handler()
    const port = rinfo.port === to ? from : to

    if (forwarding && forwarding.then) forwarding.then(fwd).catch(safetyCatch)
    else fwd(forwarding)

    function fwd() {
      socket.trySend(buf, port, '127.0.0.1')
    }
  })

  socket.bind(bind || 0, '127.0.0.1')

  return socket
}
