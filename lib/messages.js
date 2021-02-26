const c = require('compact-encoding')

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
