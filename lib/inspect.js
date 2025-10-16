const b4a = require('b4a')

module.exports = function (core, depth, opts) {
  let indent = ''
  if (typeof opts.indentationLvl === 'number') {
    while (indent.length < opts.indentationLvl) indent += ' '
  }

  let peers = ''
  const min = Math.min(core.peers.length, 5)

  for (let i = 0; i < min; i++) {
    const peer = core.peers[i]

    peers += `${indent}    Peer(\n`
    peers += `${indent}      remotePublicKey: ${opts.stylize(toHex(peer.remotePublicKey), 'string')}\n`
    peers += `${indent}      remoteLength: ${opts.stylize(peer.remoteLength, 'number')}\n`
    peers += `${indent}      remoteFork: ${opts.stylize(peer.remoteFork, 'number')}\n`
    peers += `${indent}      remoteCanUpgrade: ${opts.stylize(peer.remoteCanUpgrade, 'boolean')}\n`
    peers += `${indent}    )\n`
  }

  if (core.peers.length > 5) {
    peers += `${indent}  ... and ${core.peers.length - 5} more\n`
  }

  if (peers) peers = `[\n${peers}${indent}  ]`
  else peers = `[ ${opts.stylize(0, 'number')} ]`

  return (
    `${core.constructor.name}(\n` +
    `${indent}  id: ${opts.stylize(core.id, 'string')}\n` +
    `${indent}  key: ${opts.stylize(toHex(core.key), 'string')}\n` +
    `${indent}  discoveryKey: ${opts.stylize(toHex(core.discoveryKey), 'string')}\n` +
    `${indent}  opened: ${opts.stylize(core.opened, 'boolean')}\n` +
    `${indent}  closed: ${opts.stylize(core.closed, 'boolean')}\n` +
    `${indent}  snapshotted: ${opts.stylize(core.snapshotted, 'boolean')}\n` +
    `${indent}  writable: ${opts.stylize(core.writable, 'boolean')}\n` +
    `${indent}  length: ${opts.stylize(core.length, 'number')}\n` +
    `${indent}  fork: ${opts.stylize(core.fork, 'number')}\n` +
    `${indent}  sessions: [ ${opts.stylize(core.sessions.length, 'number')} ]\n` +
    `${indent}  activeRequests: [ ${opts.stylize(core.activeRequests.length, 'number')} ]\n` +
    `${indent}  peers: ${peers}\n` +
    `${indent})`
  )
}

function toHex(buf) {
  return buf && b4a.toString(buf, 'hex')
}
