const c = require('compact-encoding')
const b4a = require('b4a')
const { DEFAULT_NAMESPACE } = require('./caps')
const { INVALID_OPLOG_VERSION } = require('hypercore-errors')
const unslab = require('unslab')

const EMPTY = b4a.alloc(0)

const MANIFEST_PATCH = 0b00000001
const MANIFEST_PROLOGUE = 0b00000010
const MANIFEST_LINKED = 0b00000100
const MANIFEST_USER_DATA = 0b00001000

const hashes = {
  preencode (state, m) {
    state.end++ // small uint
  },
  encode (state, m) {
    if (m === 'blake2b') {
      c.uint.encode(state, 0)
      return
    }

    throw new Error('Unknown hash: ' + m)
  },
  decode (state) {
    const n = c.uint.decode(state)
    if (n === 0) return 'blake2b'
    throw new Error('Unknown hash id: ' + n)
  }
}

const signatures = {
  preencode (state, m) {
    state.end++ // small uint
  },
  encode (state, m) {
    if (m === 'ed25519') {
      c.uint.encode(state, 0)
      return
    }

    throw new Error('Unknown signature: ' + m)
  },
  decode (state) {
    const n = c.uint.decode(state)
    if (n === 0) return 'ed25519'
    throw new Error('Unknown signature id: ' + n)
  }
}

const signer = {
  preencode (state, m) {
    signatures.preencode(state, m.signature)
    c.fixed32.preencode(state, m.namespace)
    c.fixed32.preencode(state, m.publicKey)
  },
  encode (state, m) {
    signatures.encode(state, m.signature)
    c.fixed32.encode(state, m.namespace)
    c.fixed32.encode(state, m.publicKey)
  },
  decode (state) {
    return {
      signature: signatures.decode(state),
      namespace: c.fixed32.decode(state),
      publicKey: c.fixed32.decode(state)
    }
  }
}

const signerArray = c.array(signer)

const prologue = {
  preencode (state, p) {
    c.fixed32.preencode(state, p.hash)
    c.uint.preencode(state, p.length)
  },
  encode (state, p) {
    c.fixed32.encode(state, p.hash)
    c.uint.encode(state, p.length)
  },
  decode (state) {
    return {
      hash: c.fixed32.decode(state),
      length: c.uint.decode(state)
    }
  }
}

const manifestv0 = {
  preencode (state, m) {
    hashes.preencode(state, m.hash)
    state.end++ // type

    if (m.prologue && m.signers.length === 0) {
      c.fixed32.preencode(state, m.prologue.hash)
      return
    }

    if (m.quorum === 1 && m.signers.length === 1 && !m.allowPatch) {
      signer.preencode(state, m.signers[0])
    } else {
      state.end++ // flags
      c.uint.preencode(state, m.quorum)
      signerArray.preencode(state, m.signers)
    }
  },
  encode (state, m) {
    hashes.encode(state, m.hash)

    if (m.prologue && m.signers.length === 0) {
      c.uint.encode(state, 0)
      c.fixed32.encode(state, m.prologue.hash)
      return
    }

    if (m.quorum === 1 && m.signers.length === 1 && !m.allowPatch) {
      c.uint.encode(state, 1)
      signer.encode(state, m.signers[0])
    } else {
      c.uint.encode(state, 2)
      c.uint.encode(state, m.allowPatch ? 1 : 0)
      c.uint.encode(state, m.quorum)
      signerArray.encode(state, m.signers)
    }
  },
  decode (state) {
    const hash = hashes.decode(state)
    const type = c.uint.decode(state)

    if (type > 2) throw new Error('Unknown type: ' + type)

    if (type === 0) {
      return {
        version: 0,
        hash,
        allowPatch: false,
        quorum: 0,
        signers: [],
        prologue: {
          hash: c.fixed32.decode(state),
          length: 0
        },
        linked: null,
        userData: null
      }
    }

    if (type === 1) {
      return {
        version: 0,
        hash,
        allowPatch: false,
        quorum: 1,
        signers: [signer.decode(state)],
        prologue: null,
        linked: null,
        userData: null
      }
    }

    const flags = c.uint.decode(state)

    return {
      version: 0,
      hash,
      allowPatch: (flags & 1) !== 0,
      quorum: c.uint.decode(state),
      signers: signerArray.decode(state),
      prologue: null,
      linked: null,
      userData: null
    }
  }
}

const fixed32Array = c.array(c.fixed32)

const manifest = exports.manifest = {
  preencode (state, m) {
    state.end++ // version

    if (m.version === 0) return manifestv0.preencode(state, m)

    state.end++ // flags
    hashes.preencode(state, m.hash)

    c.uint.preencode(state, m.quorum)
    signerArray.preencode(state, m.signers)

    if (m.prologue) prologue.preencode(state, m.prologue)
    if (m.linked) fixed32Array.preencode(state, m.linked)
    if (m.userData) c.buffer.preencode(state, m.userData)
  },
  encode (state, m) {
    c.uint.encode(state, m.version)

    if (m.version === 0) return manifestv0.encode(state, m)

    let flags = 0
    if (m.allowPatch) flags |= MANIFEST_PATCH
    if (m.prologue) flags |= MANIFEST_PROLOGUE
    if (m.linked) flags |= MANIFEST_LINKED
    if (m.userData) flags |= MANIFEST_USER_DATA

    c.uint.encode(state, flags)
    hashes.encode(state, m.hash)

    c.uint.encode(state, m.quorum)
    signerArray.encode(state, m.signers)

    if (m.prologue) prologue.encode(state, m.prologue)
    if (m.linked) fixed32Array.encode(state, m.linked)
    if (m.userData) c.buffer.encode(state, m.userData)
  },
  decode (state) {
    const version = c.uint.decode(state)

    if (version === 0) return manifestv0.decode(state)
    if (version > 2) throw new Error('Unknown version: ' + version)

    const flags = c.uint.decode(state)
    const hash = hashes.decode(state)
    const quorum = c.uint.decode(state)
    const signers = signerArray.decode(state)

    const hasPatch = (flags & MANIFEST_PATCH) !== 0
    const hasPrologue = (flags & MANIFEST_PROLOGUE) !== 0
    const hasLinked = (flags & MANIFEST_LINKED) !== 0
    const hasUserData = (flags & MANIFEST_USER_DATA) !== 0

    return {
      version,
      hash,
      allowPatch: hasPatch,
      quorum,
      signers,
      prologue: hasPrologue ? prologue.decode(state) : null,
      linked: hasLinked ? fixed32Array.decode(state) : null,
      userData: hasUserData ? c.buffer.decode(state) : null
    }
  }
}

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
    c.uint.preencode(state, 1)
    c.fixed32.preencode(state, m.capability)
  },
  encode (state, m) {
    c.uint.encode(state, m.seeks ? 1 : 0)
    c.fixed32.encode(state, m.capability)
  },
  decode (state) {
    const flags = c.uint.decode(state)
    return {
      seeks: (flags & 1) !== 0,
      capability: unslab(c.fixed32.decode(state))
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
    c.uint.preencode(state, s.padding)
  },
  encode (state, s) {
    c.uint.encode(state, s.bytes)
    c.uint.encode(state, s.padding)
  },
  decode (state) {
    return {
      bytes: c.uint.decode(state),
      padding: c.uint.decode(state)
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
    if (m.priority) c.uint.preencode(state, m.priority)
  },
  encode (state, m) {
    const flags = (m.block ? 1 : 0) | (m.hash ? 2 : 0) | (m.seek ? 4 : 0) | (m.upgrade ? 8 : 0) | (m.manifest ? 16 : 0) | (m.priority ? 32 : 0)

    c.uint.encode(state, flags)
    c.uint.encode(state, m.id)
    c.uint.encode(state, m.fork)

    if (m.block) requestBlock.encode(state, m.block)
    if (m.hash) requestBlock.encode(state, m.hash)
    if (m.seek) requestSeek.encode(state, m.seek)
    if (m.upgrade) requestUpgrade.encode(state, m.upgrade)
    if (m.priority) c.uint.encode(state, m.priority)
  },
  decode (state) {
    const flags = c.uint.decode(state)

    return {
      id: c.uint.decode(state),
      fork: c.uint.decode(state),
      block: flags & 1 ? requestBlock.decode(state) : null,
      hash: flags & 2 ? requestBlock.decode(state) : null,
      seek: flags & 4 ? requestSeek.decode(state) : null,
      upgrade: flags & 8 ? requestUpgrade.decode(state) : null,
      manifest: (flags & 16) !== 0,
      priority: flags & 32 ? c.uint.decode(state) : 0
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
    if (m.manifest) manifest.preencode(state, m.manifest)
  },
  encode (state, m) {
    const flags = (m.block ? 1 : 0) | (m.hash ? 2 : 0) | (m.seek ? 4 : 0) | (m.upgrade ? 8 : 0) | (m.manifest ? 16 : 0)

    c.uint.encode(state, flags)
    c.uint.encode(state, m.request)
    c.uint.encode(state, m.fork)

    if (m.block) dataBlock.encode(state, m.block)
    if (m.hash) dataHash.encode(state, m.hash)
    if (m.seek) dataSeek.encode(state, m.seek)
    if (m.upgrade) dataUpgrade.encode(state, m.upgrade)
    if (m.manifest) manifest.encode(state, m.manifest)
  },
  decode (state) {
    const flags = c.uint.decode(state)

    return {
      request: c.uint.decode(state),
      fork: c.uint.decode(state),
      block: flags & 1 ? dataBlock.decode(state) : null,
      hash: flags & 2 ? dataHash.decode(state) : null,
      seek: flags & 4 ? dataSeek.decode(state) : null,
      upgrade: flags & 8 ? dataUpgrade.decode(state) : null,
      manifest: flags & 16 ? manifest.decode(state) : null
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
    c.uint.encode(state, (m.canUpgrade ? 1 : 0) | (m.uploading ? 2 : 0) | (m.downloading ? 4 : 0) | (m.hasManifest ? 8 : 0))
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
      downloading: (flags & 4) !== 0,
      hasManifest: (flags & 8) !== 0
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
    c.uint.preencode(state, h.contiguousLength)
  },
  encode (state, h) {
    reorgHintArray.encode(state, h.reorgs)
    c.uint.encode(state, h.contiguousLength)
  },
  decode (state) {
    return {
      reorgs: reorgHintArray.decode(state),
      contiguousLength: state.start < state.end ? c.uint.decode(state) : 0
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

const externalHeader = {
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

const keyValueArray = c.array(keyValue)

oplog.header = {
  preencode (state, h) {
    state.end += 2 // version + flags
    if (h.external) {
      externalHeader.preencode(state, h.external)
      return
    }
    c.fixed32.preencode(state, h.key)
    if (h.manifest) manifest.preencode(state, h.manifest)
    if (h.keyPair) keyPair.preencode(state, h.keyPair)
    keyValueArray.preencode(state, h.userData)
    treeHeader.preencode(state, h.tree)
    hints.preencode(state, h.hints)
  },
  encode (state, h) {
    c.uint.encode(state, 1)
    if (h.external) {
      c.uint.encode(state, 1) // ONLY set the first big for clarity
      externalHeader.encode(state, h.external)
      return
    }
    c.uint.encode(state, (h.manifest ? 2 : 0) | (h.keyPair ? 4 : 0))
    c.fixed32.encode(state, h.key)
    if (h.manifest) manifest.encode(state, h.manifest)
    if (h.keyPair) keyPair.encode(state, h.keyPair)
    keyValueArray.encode(state, h.userData)
    treeHeader.encode(state, h.tree)
    hints.encode(state, h.hints)
  },
  decode (state) {
    const version = c.uint.decode(state)

    if (version > 1) {
      throw INVALID_OPLOG_VERSION('Invalid header version. Expected <= 1, got ' + version)
    }

    if (version === 0) {
      const old = {
        types: types.decode(state),
        userData: keyValueArray.decode(state),
        tree: treeHeader.decode(state),
        signer: keyPair.decode(state),
        hints: hints.decode(state)
      }

      return {
        external: null,
        key: old.signer.publicKey,
        manifest: {
          version: 0,
          hash: old.types.tree,
          allowPatch: false,
          quorum: 1,
          signers: [{
            signature: old.types.signer,
            namespace: DEFAULT_NAMESPACE,
            publicKey: old.signer.publicKey
          }],
          prologue: null,
          linked: null,
          userData: null
        },
        keyPair: old.signer.secretKey ? old.signer : null,
        userData: old.userData,
        tree: old.tree,
        hints: old.hints
      }
    }

    const flags = c.uint.decode(state)

    if (flags & 1) {
      return {
        external: externalHeader.decode(state),
        key: null,
        manifest: null,
        keyPair: null,
        userData: null,
        tree: null,
        hints: null
      }
    }

    return {
      external: null,
      key: c.fixed32.decode(state),
      manifest: (flags & 2) !== 0 ? manifest.decode(state) : null,
      keyPair: (flags & 4) !== 0 ? keyPair.decode(state) : null,
      userData: keyValueArray.decode(state),
      tree: treeHeader.decode(state),
      hints: hints.decode(state)
    }
  }
}

const uintArray = c.array(c.uint)

const multisigInput = {
  preencode (state, inp) {
    c.uint.preencode(state, inp.signer)
    c.fixed64.preencode(state, inp.signature)
    c.uint.preencode(state, inp.patch)
  },
  encode (state, inp) {
    c.uint.encode(state, inp.signer)
    c.fixed64.encode(state, inp.signature)
    c.uint.encode(state, inp.patch)
  },
  decode (state) {
    return {
      signer: c.uint.decode(state),
      signature: c.fixed64.decode(state),
      patch: c.uint.decode(state)
    }
  }
}

const patchEncodingv0 = {
  preencode (state, n) {
    c.uint.preencode(state, n.start)
    c.uint.preencode(state, n.length)
    uintArray.preencode(state, n.nodes)
  },
  encode (state, n) {
    c.uint.encode(state, n.start)
    c.uint.encode(state, n.length)
    uintArray.encode(state, n.nodes)
  },
  decode (state) {
    return {
      start: c.uint.decode(state),
      length: c.uint.decode(state),
      nodes: uintArray.decode(state)
    }
  }
}

const multisigInputv0 = {
  preencode (state, n) {
    state.end++
    c.uint.preencode(state, n.signer)
    c.fixed64.preencode(state, n.signature)
    if (n.patch) patchEncodingv0.preencode(state, n.patch)
  },
  encode (state, n) {
    c.uint.encode(state, n.patch ? 1 : 0)
    c.uint.encode(state, n.signer)
    c.fixed64.encode(state, n.signature)
    if (n.patch) patchEncodingv0.encode(state, n.patch)
  },
  decode (state) {
    const flags = c.uint.decode(state)
    return {
      signer: c.uint.decode(state),
      signature: c.fixed64.decode(state),
      patch: (flags & 1) ? patchEncodingv0.decode(state) : null
    }
  }
}

const multisigInputArrayv0 = c.array(multisigInputv0)
const multisigInputArray = c.array(multisigInput)

const compactNode = {
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

const compactNodeArray = c.array(compactNode)

exports.multiSignaturev0 = {
  preencode (state, s) {
    multisigInputArrayv0.preencode(state, s.proofs)
    compactNodeArray.preencode(state, s.patch)
  },
  encode (state, s) {
    multisigInputArrayv0.encode(state, s.proofs)
    compactNodeArray.encode(state, s.patch)
  },
  decode (state) {
    return {
      proofs: multisigInputArrayv0.decode(state),
      patch: compactNodeArray.decode(state)
    }
  }
}

exports.multiSignature = {
  preencode (state, s) {
    multisigInputArray.preencode(state, s.proofs)
    compactNodeArray.preencode(state, s.patch)
  },
  encode (state, s) {
    multisigInputArray.encode(state, s.proofs)
    compactNodeArray.encode(state, s.patch)
  },
  decode (state) {
    return {
      proofs: multisigInputArray.decode(state),
      patch: compactNodeArray.decode(state)
    }
  }
}
