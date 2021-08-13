const c = require('compact-encoding')

const node = exports.node = {
  preencode (state, n) {
    c.uint.preencode(state, n.index)
    c.uint.preencode(state, n.size)
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
    c.buffer.preencode(state, u.signature)
  },
  encode (state, u) {
    c.uint.encode(state, u.start)
    c.uint.encode(state, u.length)
    nodeArray.encode(state, u.nodes)
    nodeArray.encode(state, u.additionalNodes)
    c.buffer.encode(state, u.signature)
  },
  decode (state) {
    return {
      start: c.uint.decode(state),
      length: c.uint.decode(state),
      nodes: nodeArray.decode(state),
      additionalNodes: nodeArray.decode(state),
      signature: c.buffer.decode(state)
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

const keyValue = {
  preencode (state, p) {
    c.string.preencode(state, p.key)
    c.buffer.preencode(state, p.value)
  },
  encode (state, p) {
    c.string.encode(state, p.key)
    c.buffer.encode(state, p.value)
  },
  decode (state) {
    return {
      key: c.string.decode(state),
      value: c.buffer.decode(state)
    }
  }
}

const treeUpgrade = {
  preencode (state, u) {
    c.uint.preencode(state, u.fork)
    c.uint.preencode(state, u.ancestors)
    c.uint.preencode(state, u.length)
    c.buffer.preencode(state, u.signature)
  },
  encode (state, u) {
    c.uint.encode(state, u.fork)
    c.uint.encode(state, u.ancestors)
    c.uint.encode(state, u.length)
    c.buffer.encode(state, u.signature)
  },
  decode (state) {
    return {
      fork: c.uint.decode(state),
      ancestors: c.uint.decode(state),
      length: c.uint.decode(state),
      signature: c.buffer.decode(state)
    }
  }
}

const bitfieldUpdate = { // TODO: can maybe be folded into a HAVE later on with the most recent spec
  preencode (state, b) {
    state.end++ // flags
    c.uint.preencode(state, b.start)
    c.uint.preencode(state, b.length)
  },
  encode (state, b) {
    state.buffer[state.start++] = b.drop ? 1 : 0
    c.uint.encode(state, b.start)
    c.uint.encode(state, b.length)
  },
  decode (state) {
    const flags = c.uint.decode(state)
    return {
      drop: (flags & 1) !== 0,
      start: c.uint.decode(state),
      length: c.uint.decode(state)
    }
  }
}

exports.oplogEntry = {
  preencode (state, m) {
    state.end++ // flags
    if (m.userData) keyValue.preencode(state, m.userData)
    if (m.treeNodes) nodeArray.preencode(state, m.treeNodes)
    if (m.treeUpgrade) treeUpgrade.preencode(state, m.treeUpgrade)
    if (m.bitfield) bitfieldUpdate.preencode(state, m.bitfield)
  },
  encode (state, m) {
    state.buffer[state.start++] = (m.userData ? 1 : 0) | (m.treeNodes ? 2 : 0) | (m.treeUpgrade ? 4 : 0) | (m.bitfield ? 8 : 0)
    if (m.userData) keyValue.encode(state, m.userData)
    if (m.treeNodes) nodeArray.encode(state, m.treeNodes)
    if (m.treeUpgrade) treeUpgrade.encode(state, m.treeUpgrade)
    if (m.bitfield) bitfieldUpdate.encode(state, m.bitfield)
  },
  decode (state) {
    const flags = c.uint.decode(state)
    return {
      userData: (flags & 1) !== 0 ? keyValue.decode(state) : null,
      treeNodes: (flags & 2) !== 0 ? nodeArray.decode(state) : null,
      treeUpgrade: (flags & 4) !== 0 ? treeUpgrade.decode(state) : null,
      bitfield: (flags & 8) !== 0 ? bitfieldUpdate.decode(state) : null
    }
  }
}

const keyPair = {
  preencode (state, kp) {
    c.string.preencode(state, kp.type)
    c.buffer.preencode(state, kp.publicKey)
    c.buffer.preencode(state, kp.secretKey)
  },
  encode (state, kp) {
    c.string.encode(state, kp.type)
    c.buffer.encode(state, kp.publicKey)
    c.buffer.encode(state, kp.secretKey)
  },
  decode (state) {
    return {
      type: c.string.decode(state),
      publicKey: c.buffer.decode(state),
      secretKey: c.buffer.decode(state)
    }
  }
}

const treeHeader = {
  preencode (state, t) {
    c.string.preencode(state, t.type)
    c.uint.preencode(state, t.fork)
    c.uint.preencode(state, t.length)
    c.buffer.preencode(state, t.signature)
  },
  encode (state, t) {
    c.string.encode(state, t.type)
    c.uint.encode(state, t.fork)
    c.uint.encode(state, t.length)
    c.buffer.encode(state, t.signature)
  },
  decode (state) {
    return {
      type: c.string.decode(state),
      fork: c.uint.decode(state),
      length: c.uint.decode(state),
      signature: c.buffer.decode(state)
    }
  }
}

const bitfieldHeader = {
  preencode (state, b) {
    c.string.preencode(state, b.type)
  },
  encode (state, b) {
    c.string.encode(state, b.type)
  },
  decode (state) {
    return {
      type: c.string.decode(state)
    }
  }
}

const keyValueArray = c.array(keyValue)

exports.oplogHeader = {
  preencode (state, h) {
    state.end += 2 // version + flags
    keyValueArray.preencode(state, h.userData)
    keyPair.preencode(state, h.keyPair)
    treeHeader.preencode(state, h.tree)
    bitfieldHeader.preencode(state, h.bitfield)
  },
  encode (state, h) {
    state.buffer[state.start++] = 0 // version
    state.buffer[state.start++] = 0 // flags (none for now)
    keyValueArray.encode(state, h.userData)
    keyPair.encode(state, h.keyPair)
    treeHeader.encode(state, h.tree)
    bitfieldHeader.encode(state, h.bitfield)
  },
  decode (state) {
    const version = c.uint.decode(state)
    const flags = c.uint.decode(state)

    if (version !== 0) {
      throw new Error('Invalid header version. Expected 0, got ' + version)
    }

    return {
      userData: keyValueArray.decode(state),
      keyPair: keyPair.decode(state),
      tree: treeHeader.decode(state),
      bitfield: bitfieldHeader.decode(state)
    }
  }
}
