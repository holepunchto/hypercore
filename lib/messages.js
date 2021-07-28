const c = require('compact-encoding')
const crc32 = require('./crc32')

const node = exports.node = {
  preencode (state, n) {
    c.uint.preencode(state, n.size)
    c.uint.preencode(state, n.index)
    c.fixed32.preencode(state, n.hash)
  },
  encode (state, n) {
    c.uint.encode(state, n.index)
    c.uint.encode(state, n.size)
    c.fixed32.encode(state, n.hash)
  },
  decode (state) {
    return {
      index: c.uint.decode(state),
      size: c.uint.decode(state),
      hash: c.fixed32.decode(state)
    }
  }
}

const nodeArray = c.array(node)

const dataUpgrade = {
  preencode (state, u) {
    c.uint.preencode(state, u.start)
    c.uint.preencode(state, u.length)
    nodeArray.preencode(state, u.nodes)
    nodeArray.preencode(state, u.additionalNodes)
    c.fixed64.preencode(state, u.signature)
  },
  encode (state, u) {
    c.uint.encode(state, u.start)
    c.uint.encode(state, u.length)
    nodeArray.encode(state, u.nodes)
    nodeArray.encode(state, u.additionalNodes)
    c.fixed64.encode(state, u.signature)
  },
  decode (state) {
    return {
      start: c.uint.decode(state),
      length: c.uint.decode(state),
      nodes: nodeArray.decode(state),
      additionalNodes: nodeArray.decode(state),
      signature: c.fixed64.decode(state)
    }
  }
}

const dataSeek = {
  preencode (state, s) {
    c.uint.preencode(state, s.bytes)
    nodeArray.preencode(state, s.nodes)
  },
  encode (state, s) {
    c.uint.encode(state, s.bytes)
    nodeArray.encode(state, s.nodes)
  },
  decode (state) {
    return {
      bytes: c.uint.decode(state),
      nodes: nodeArray.decode(state)
    }
  }
}

const dataBlock = {
  preencode (state, b) {
    c.uint.preencode(state, b.index)
    c.buffer.preencode(state, b.value)
    nodeArray.preencode(state, b.nodes)
  },
  encode (state, b) {
    c.uint.encode(state, b.index)
    c.buffer.encode(state, b.value)
    nodeArray.encode(state, b.nodes)
  },
  decode (state) {
    return {
      index: c.uint.decode(state),
      value: c.buffer.decode(state),
      nodes: nodeArray.decode(state)
    }
  }
}

exports.data = {
  preencode (state, d) {
    c.uint.preencode(state, d.fork)

    state.end++

    if (d.block) dataBlock.preencode(state, d.block)
    if (d.seek) dataSeek.preencode(state, d.seek)
    if (d.upgrade) dataUpgrade.preencode(state, d.upgrade)
  },
  encode (state, d) {
    c.uint.encode(state, d.fork)

    const s = state.start++
    let bits = 0

    if (d.block) {
      bits |= 1
      dataBlock.encode(state, d.block)
    }
    if (d.seek) {
      bits |= 2
      dataSeek.encode(state, d.seek)
    }
    if (d.upgrade) {
      bits |= 4
      dataUpgrade.encode(state, d.upgrade)
    }

    state.buffer[s] = bits
  },
  decode (state) {
    const fork = c.uint.decode(state)
    const bits = c.uint.decode(state)

    return {
      fork,
      block: (bits & 1) === 0 ? null : dataBlock.decode(state),
      seek: (bits & 2) === 0 ? null : dataSeek.decode(state),
      upgrade: (bits & 4) === 0 ? null : dataUpgrade.decode(state)
    }
  }
}

const requestBlock = {
  preencode (state, b) {
    c.uint.preencode(state, b.index)
    c.bool.preencode(state, b.value)
    c.uint.preencode(state, b.nodes)
  },
  encode (state, b) {
    c.uint.encode(state, b.index)
    c.bool.encode(state, b.value)
    c.uint.encode(state, b.nodes)
  },
  decode (state) {
    return {
      index: c.uint.decode(state),
      value: c.bool.decode(state),
      nodes: c.uint.decode(state)
    }
  }
}

const requestSeek = {
  preencode (state, s) {
    c.uint.preencode(state, s.bytes)
  },
  encode (state, s) {
    c.uint.encode(state, s.bytes)
  },
  decode (state) {
    return {
      bytes: c.uint.decode(state)
    }
  }
}

const requestUpgrade = {
  preencode (state, u) {
    c.uint.preencode(state, u.start)
    c.uint.preencode(state, u.length)
  },
  encode (state, u) {
    c.uint.encode(state, u.start)
    c.uint.encode(state, u.length)
  },
  decode (state) {
    return {
      start: c.uint.decode(state),
      length: c.uint.decode(state)
    }
  }
}

exports.request = {
  preencode (state, r) {
    c.uint.preencode(state, r.fork)

    state.end++

    if (r.block) requestBlock.preencode(state, r.block)
    if (r.seek) requestSeek.preencode(state, r.seek)
    if (r.upgrade) requestUpgrade.preencode(state, r.upgrade)
  },
  encode (state, r) {
    c.uint.encode(state, r.fork)

    const s = state.start++
    let bits = 0

    if (r.block) {
      bits |= 1
      requestBlock.encode(state, r.block)
    }
    if (r.seek) {
      bits |= 2
      requestSeek.encode(state, r.seek)
    }
    if (r.upgrade) {
      bits |= 4
      requestUpgrade.encode(state, r.upgrade)
    }

    state.buffer[s] = bits
  },
  decode (state) {
    const fork = c.uint.decode(state)
    const bits = c.uint.decode(state)

    return {
      fork,
      block: (bits & 1) === 0 ? null : requestBlock.decode(state),
      seek: (bits & 2) === 0 ? null : requestSeek.decode(state),
      upgrade: (bits & 4) === 0 ? null : requestUpgrade.decode(state)
    }
  }
}

exports.have = {
  preencode (state, h) {
    c.uint.preencode(state, h.start)
    if (h.length > 1) c.uint.preencode(state, h.length)
  },
  encode (state, h) {
    c.uint.encode(state, h.start)
    if (h.length > 1) c.uint.encode(state, h.length)
  },
  decode (state) {
    return {
      start: c.uint.decode(state),
      length: state.start < state.end ? c.uint.decode(state) : 1
    }
  }
}

exports.bitfield = {
  preencode (state, b) {
    c.uint.preencode(state, b.start)
    c.uint32array.preencode(state, b.bitfield)
  },
  encode (state, b) {
    c.uint.encode(state, b.start)
    c.uint32array.encode(state, b.bitfield)
  },
  decode (state) {
    return {
      start: c.uint.decode(state),
      bitfield: c.uint32array.decode(state)
    }
  }
}

exports.info = {
  preencode (state, i) {
    c.uint.preencode(state, i.length)
    c.uint.preencode(state, i.fork)
  },
  encode (state, i) {
    c.uint.encode(state, i.length)
    c.uint.encode(state, i.fork)
  },
  decode (state) {
    return {
      length: c.uint.decode(state),
      fork: c.uint.decode(state)
    }
  }
}

exports.handshake = {
  preencode (state, h) {
    c.uint.preencode(state, h.protocolVersion)
    c.string.preencode(state, h.userAgent)
  },
  encode (state, h) {
    c.uint.encode(state, h.protocolVersion)
    c.string.encode(state, h.userAgent)
  },
  decode (state) {
    return {
      protocolVersion: c.uint.decode(state),
      userAgent: c.string.decode(state)
    }
  }
}

exports.extension = {
  preencode (state, a) {
    c.uint.preencode(state, a.alias)
    c.string.preencode(state, a.name)
  },
  encode (state, a) {
    c.uint.encode(state, a.alias)
    c.string.encode(state, a.name)
  },
  decode (state) {
    return {
      alias: c.uint.decode(state),
      name: c.string.decode(state)
    }
  }
}

exports.core = {
  preencode (state, m) {
    c.uint.preencode(state, m.alias)
    c.fixed32.preencode(state, m.discoveryKey)
    c.fixed32.preencode(state, m.capability)
  },
  encode (state, m) {
    c.uint.encode(state, m.alias)
    c.fixed32.encode(state, m.discoveryKey)
    c.fixed32.encode(state, m.capability)
  },
  decode (state) {
    return {
      alias: c.uint.decode(state),
      discoveryKey: c.fixed32.decode(state),
      capability: c.fixed32.decode(state)
    }
  }
}

exports.unknownCore = {
  preencode (state, m) {
    c.fixed32.preencode(state, m.discoveryKey)
  },
  encode (state, m) {
    c.fixed32.encode(state, m.discoveryKey)
  },
  decode (state) {
    return { discoveryKey: c.fixed32.decode(state) }
  }
}

// Oplog Messages

/**
 * (uint length)(32-bit checksum)(sub-encoding)
 */
exports.checksumEncoding = function (enc) {
  return {
    preencode (state, obj) {
      state.end += 4 * 2 // checksum + 32-bit uint length
      enc.preencode(state, obj)
    },
    encode (state, obj) {
      const start = state.start
      const lengthOffset = start + 4
      const prefixOffset = lengthOffset + 4

      state.start = prefixOffset // obj should be stored after checksum/length
      enc.encode(state, obj)
      const end = state.start

      state.start = start
      c.uint32.encode(state, state.end - prefixOffset)
      c.uint32.encode(state, computeChecksum(state.buffer.subarray(prefixOffset, state.end)))

      state.start = end
    },
    decode (state) {
      const length = c.uint32.decode(state)
      const checksum = c.uint32.decode(state)

      const buf = state.buffer.subarray(state.start, state.start + length)
      if (checksum !== computeChecksum(buf)) throw new Error('Checksum test failed')

      const start = state.start
      const decoded = enc.decode(state)
      if ((state.start - start) !== length) throw new Error('Invalid length')
      return decoded
    }
  }
}

function zigzagEncode (n) {
  // 0, -1, 1, -2, 2, ...
  return n < 0 ? (2 * -n) - 1 : n === 0 ? 0 : 2 * n
}

function computeChecksum (buf) {
  return zigzagEncode(crc32(buf))
}

exports.oplog = {}
exports.oplog.header = exports.checksumEncoding({
  preencode (state, h) {
    c.uint.preencode(state, h.flags)
    c.uint.preencode(state, h.flushes)
    c.buffer.preencode(state, h.publicKey)
    c.buffer.preencode(state, h.secretKey)
    c.uint.preencode(state, h.fork)
    c.uint.preencode(state, h.length)
    c.buffer.preencode(state, h.signature)
    c.buffer.preencode(state, h.rootHash)
  },
  encode (state, h) {
    c.uint.encode(state, h.flags)
    c.uint.encode(state, h.flushes)
    c.buffer.encode(state, h.publicKey)
    c.buffer.encode(state, h.secretKey)
    c.uint.encode(state, h.fork)
    c.uint.encode(state, h.length)
    c.buffer.encode(state, h.signature)
    c.buffer.encode(state, h.rootHash)
  },
  decode (state) {
    return {
      flags: c.uint.decode(state),
      flushes: c.uint.decode(state),
      publicKey: c.buffer.decode(state),
      secretKey: c.buffer.decode(state),
      fork: c.uint.decode(state),
      length: c.uint.decode(state),
      signature: c.buffer.decode(state),
      rootHash: c.buffer.decode(state)
    }
  }
})

const mutation = {
  preencode (state, m) {
    c.uint.preencode(state, m.type)
    c.uint.preencode(state, m.fork)
    c.uint.preencode(state, m.length)
    nodeArray.preencode(state, m.nodes)
    c.uint.preencode(state, m.startBlock)
    c.buffer.preencode(state, m.signature)
  },
  encode (state, m) {
    c.uint.encode(state, m.type)
    c.uint.encode(state, m.fork)
    c.uint.encode(state, m.length)
    nodeArray.encode(state, m.nodes)
    c.uint.encode(state, m.startBlock)
    c.buffer.encode(state, m.signature)
  },
  decode (state) {
    return {
      type: c.uint.decode(state),
      fork: c.uint.decode(state),
      length: c.uint.decode(state),
      nodes: nodeArray.decode(state),
      startBlock: c.uint.decode(state),
      signature: c.buffer.decode(state)
    }
  }
}

exports.oplog.op = exports.checksumEncoding({
  preencode (state, op) {
    state.end++ // flags
    c.uint.preencode(state, op.flushes)
    if (op.mutation) mutation.preencode(state, op.mutation)
    if (op.userData) c.buffer.preencode(state, op.userData)
  },
  encode (state, op) {
    const start = state.start++
    c.uint.encode(state, op.flushes)

    let flags = 0
    if (op.mutation) {
      flags |= 1
      mutation.encode(state, op.mutation)
    }
    if (op.userData) {
      flags |= 2
      c.buffer.encode(state, op.userData)
    }

    state.buffer[start] = flags
  },
  decode (state) {
    const flags = c.uint.decode(state)
    const flushes = c.uint.decode(state)
    return {
      flushes,
      mutation: (flags & 1) === 0 ? null : mutation.decode(state),
      userData: (flags & 2) === 0 ? null : c.buffer.decode(state)
    }
  }
})
