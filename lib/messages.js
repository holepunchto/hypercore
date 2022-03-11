const c = require('compact-encoding')
const b4a = require('b4a')

const EMPTY = b4a.alloc(0)

const node = {
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

const wire = exports.wire = {}

wire.handshake = {
  preencode (state, m) {
    c.uint.preencode(state, 0) // flags for the future
    c.fixed32.preencode(state, m.capability)
  },
  encode (state, m) {
    c.uint.encode(state, 0) // flags for the future
    c.fixed32.encode(state, m.capability)
  },
  decode (state) {
    c.uint.decode(state) // flags for the future
    return {
      capability: c.fixed32.decode(state)
    }
  }
}

const requestBlock = {
  preencode (state, b) {
    c.uint.preencode(state, b.index)
    c.uint.preencode(state, b.nodes)
  },
  encode (state, b) {
    c.uint.encode(state, b.index)
    c.uint.encode(state, b.nodes)
  },
  decode (state) {
    return {
      index: c.uint.decode(state),
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

wire.request = {
  preencode (state, m) {
    state.end++ // flags
    c.uint.preencode(state, m.id)
    c.uint.preencode(state, m.fork)

    if (m.block) requestBlock.preencode(state, m.block)
    if (m.hash) requestBlock.preencode(state, m.hash)
    if (m.seek) requestSeek.preencode(state, m.seek)
    if (m.upgrade) requestUpgrade.preencode(state, m.upgrade)
  },
  encode (state, m) {
    const flags = (m.block ? 1 : 0) | (m.hash ? 2 : 0) | (m.seek ? 4 : 0) | (m.upgrade ? 8 : 0)

    c.uint.encode(state, flags)
    c.uint.encode(state, m.id)
    c.uint.encode(state, m.fork)

    if (m.block) requestBlock.encode(state, m.block)
    if (m.hash) requestBlock.encode(state, m.hash)
    if (m.seek) requestSeek.encode(state, m.seek)
    if (m.upgrade) requestUpgrade.encode(state, m.upgrade)
  },
  decode (state) {
    const flags = c.uint.decode(state)

    return {
      id: c.uint.decode(state),
      fork: c.uint.decode(state),
      block: flags & 1 ? requestBlock.decode(state) : null,
      hash: flags & 2 ? requestBlock.decode(state) : null,
      seek: flags & 4 ? requestSeek.decode(state) : null,
      upgrade: flags & 8 ? requestUpgrade.decode(state) : null
    }
  }
}

wire.cancel = {
  preencode (state, m) {
    c.uint.preencode(state, m.request)
  },
  encode (state, m) {
    c.uint.encode(state, m.request)
  },
  decode (state, m) {
    return {
      request: c.uint.decode(state)
    }
  }
}

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
      value: c.buffer.decode(state) || EMPTY,
      nodes: nodeArray.decode(state)
    }
  }
}

const dataHash = {
  preencode (state, b) {
    c.uint.preencode(state, b.index)
    nodeArray.preencode(state, b.nodes)
  },
  encode (state, b) {
    c.uint.encode(state, b.index)
    nodeArray.encode(state, b.nodes)
  },
  decode (state) {
    return {
      index: c.uint.decode(state),
      nodes: nodeArray.decode(state)
    }
  }
}

wire.data = {
  preencode (state, m) {
    state.end++ // flags
    c.uint.preencode(state, m.request)
    c.uint.preencode(state, m.fork)

    if (m.block) dataBlock.preencode(state, m.block)
    if (m.hash) dataHash.preencode(state, m.hash)
    if (m.seek) dataSeek.preencode(state, m.seek)
    if (m.upgrade) dataUpgrade.preencode(state, m.upgrade)
  },
  encode (state, m) {
    const flags = (m.block ? 1 : 0) | (m.hash ? 2 : 0) | (m.seek ? 4 : 0) | (m.upgrade ? 8 : 0)

    c.uint.encode(state, flags)
    c.uint.encode(state, m.request)
    c.uint.encode(state, m.fork)

    if (m.block) dataBlock.encode(state, m.block)
    if (m.hash) dataHash.encode(state, m.hash)
    if (m.seek) dataSeek.encode(state, m.seek)
    if (m.upgrade) dataUpgrade.encode(state, m.upgrade)
  },
  decode (state) {
    const flags = c.uint.decode(state)

    return {
      request: c.uint.decode(state),
      fork: c.uint.decode(state),
      block: flags & 1 ? dataBlock.decode(state) : null,
      hash: flags & 2 ? dataHash.decode(state) : null,
      seek: flags & 4 ? dataSeek.decode(state) : null,
      upgrade: flags & 8 ? dataUpgrade.decode(state) : null
    }
  }
}

wire.noData = {
  preencode (state, m) {
    c.uint.preencode(state, m.request)
  },
  encode (state, m) {
    c.uint.encode(state, m.request)
  },
  decode (state, m) {
    return {
      request: c.uint.decode(state)
    }
  }
}

wire.want = {
  preencode (state, m) {
    c.uint.preencode(state, m.start)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.uint.encode(state, m.start)
    c.uint.encode(state, m.length)
  },
  decode (state) {
    return {
      start: c.uint.decode(state),
      length: c.uint.decode(state)
    }
  }
}

wire.unwant = {
  preencode (state, m) {
    c.uint.preencode(state, m.start)
    c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.uint.encode(state, m.start)
    c.uint.encode(state, m.length)
  },
  decode (state, m) {
    return {
      start: c.uint.decode(state),
      length: c.uint.decode(state)
    }
  }
}

wire.range = {
  preencode (state, m) {
    state.end++ // flags
    c.uint.preencode(state, m.start)
    if (m.length !== 1) c.uint.preencode(state, m.length)
  },
  encode (state, m) {
    c.uint.encode(state, (m.drop ? 1 : 0) | (m.length === 1 ? 2 : 0))
    c.uint.encode(state, m.start)
    if (m.length !== 1) c.uint.encode(state, m.length)
  },
  decode (state) {
    const flags = c.uint.decode(state)

    return {
      drop: (flags & 1) !== 0,
      start: c.uint.decode(state),
      length: (flags & 2) !== 0 ? 1 : c.uint.decode(state)
    }
  }
}

wire.bitfield = {
  preencode (state, m) {
    c.uint.preencode(state, m.start)
    c.uint32array.preencode(state, m.bitfield)
  },
  encode (state, m) {
    c.uint.encode(state, m.start)
    c.uint32array.encode(state, m.bitfield)
  },
  decode (state, m) {
    return {
      start: c.uint.decode(state),
      bitfield: c.uint32array.decode(state)
    }
  }
}

wire.sync = {
  preencode (state, m) {
    state.end++ // flags
    c.uint.preencode(state, m.fork)
    c.uint.preencode(state, m.length)
    c.uint.preencode(state, m.remoteLength)
  },
  encode (state, m) {
    c.uint.encode(state, (m.canUpgrade ? 1 : 0) | (m.uploading ? 2 : 0) | (m.downloading ? 4 : 0))
    c.uint.encode(state, m.fork)
    c.uint.encode(state, m.length)
    c.uint.encode(state, m.remoteLength)
  },
  decode (state) {
    const flags = c.uint.decode(state)

    return {
      fork: c.uint.decode(state),
      length: c.uint.decode(state),
      remoteLength: c.uint.decode(state),
      canUpgrade: (flags & 1) !== 0,
      uploading: (flags & 2) !== 0,
      downloading: (flags & 4) !== 0
    }
  }
}

wire.reorgHint = {
  preencode (state, m) {
    c.uint.preencode(state, m.from)
    c.uint.preencode(state, m.to)
    c.uint.preencode(state, m.ancestors)
  },
  encode (state, m) {
    c.uint.encode(state, m.from)
    c.uint.encode(state, m.to)
    c.uint.encode(state, m.ancestors)
  },
  decode (state) {
    return {
      from: c.uint.encode(state),
      to: c.uint.encode(state),
      ancestors: c.uint.encode(state)
    }
  }
}

wire.extension = {
  preencode (state, m) {
    c.string.preencode(state, m.name)
    c.raw.preencode(state, m.message)
  },
  encode (state, m) {
    c.string.encode(state, m.name)
    c.raw.encode(state, m.message)
  },
  decode (state) {
    return {
      name: c.string.decode(state),
      message: c.raw.decode(state)
    }
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

const oplog = exports.oplog = {}

oplog.entry = {
  preencode (state, m) {
    state.end++ // flags
    if (m.userData) keyValue.preencode(state, m.userData)
    if (m.treeNodes) nodeArray.preencode(state, m.treeNodes)
    if (m.treeUpgrade) treeUpgrade.preencode(state, m.treeUpgrade)
    if (m.bitfield) bitfieldUpdate.preencode(state, m.bitfield)
  },
  encode (state, m) {
    const s = state.start++
    let flags = 0

    if (m.userData) {
      flags |= 1
      keyValue.encode(state, m.userData)
    }
    if (m.treeNodes) {
      flags |= 2
      nodeArray.encode(state, m.treeNodes)
    }
    if (m.treeUpgrade) {
      flags |= 4
      treeUpgrade.encode(state, m.treeUpgrade)
    }
    if (m.bitfield) {
      flags |= 8
      bitfieldUpdate.encode(state, m.bitfield)
    }

    state.buffer[s] = flags
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
    c.buffer.preencode(state, kp.publicKey)
    c.buffer.preencode(state, kp.secretKey)
  },
  encode (state, kp) {
    c.buffer.encode(state, kp.publicKey)
    c.buffer.encode(state, kp.secretKey)
  },
  decode (state) {
    return {
      publicKey: c.buffer.decode(state),
      secretKey: c.buffer.decode(state)
    }
  }
}

const reorgHint = {
  preencode (state, r) {
    c.uint.preencode(state, r.from)
    c.uint.preencode(state, r.to)
    c.uint.preencode(state, r.ancestors)
  },
  encode (state, r) {
    c.uint.encode(state, r.from)
    c.uint.encode(state, r.to)
    c.uint.encode(state, r.ancestors)
  },
  decode (state) {
    return {
      from: c.uint.decode(state),
      to: c.uint.decode(state),
      ancestors: c.uint.decode(state)
    }
  }
}

const reorgHintArray = c.array(reorgHint)

const hints = {
  preencode (state, h) {
    reorgHintArray.preencode(state, h.reorgs)
  },
  encode (state, h) {
    reorgHintArray.encode(state, h.reorgs)
  },
  decode (state) {
    return {
      reorgs: reorgHintArray.decode(state)
    }
  }
}

const treeHeader = {
  preencode (state, t) {
    c.uint.preencode(state, t.fork)
    c.uint.preencode(state, t.length)
    c.buffer.preencode(state, t.rootHash)
    c.buffer.preencode(state, t.signature)
  },
  encode (state, t) {
    c.uint.encode(state, t.fork)
    c.uint.encode(state, t.length)
    c.buffer.encode(state, t.rootHash)
    c.buffer.encode(state, t.signature)
  },
  decode (state) {
    return {
      fork: c.uint.decode(state),
      length: c.uint.decode(state),
      rootHash: c.buffer.decode(state),
      signature: c.buffer.decode(state)
    }
  }
}

const types = {
  preencode (state, t) {
    c.string.preencode(state, t.tree)
    c.string.preencode(state, t.bitfield)
    c.string.preencode(state, t.signer)
  },
  encode (state, t) {
    c.string.encode(state, t.tree)
    c.string.encode(state, t.bitfield)
    c.string.encode(state, t.signer)
  },
  decode (state) {
    return {
      tree: c.string.decode(state),
      bitfield: c.string.decode(state),
      signer: c.string.decode(state)
    }
  }
}

const keyValueArray = c.array(keyValue)

oplog.header = {
  preencode (state, h) {
    state.end += 1 // version
    types.preencode(state, h.types)
    keyValueArray.preencode(state, h.userData)
    treeHeader.preencode(state, h.tree)
    keyPair.preencode(state, h.signer)
    hints.preencode(state, h.hints)
  },
  encode (state, h) {
    state.buffer[state.start++] = 0 // version
    types.encode(state, h.types)
    keyValueArray.encode(state, h.userData)
    treeHeader.encode(state, h.tree)
    keyPair.encode(state, h.signer)
    hints.encode(state, h.hints)
  },
  decode (state) {
    const version = c.uint.decode(state)

    if (version !== 0) {
      throw new Error('Invalid header version. Expected 0, got ' + version)
    }

    return {
      types: types.decode(state),
      userData: keyValueArray.decode(state),
      tree: treeHeader.decode(state),
      signer: keyPair.decode(state),
      hints: hints.decode(state)
    }
  }
}
