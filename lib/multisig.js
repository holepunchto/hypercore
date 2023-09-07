const c = require('compact-encoding')
const b4a = require('b4a')

const encoding = require('./messages').multisignature

module.exports = {
  encode,
  decode,
  assemble,
  partialSignature
}

function encode (signature) {
  return c.encode(encoding, signature)
}

function decode (data) {
  const signature = c.decode(encoding, data)
  for (const proof of signature.proofs) {
    if (proof.patch) {
      proof.patch = inflateUpgrade(proof.patch, signature.nodes)
    }
  }

  return signature
}

function assemble (partials) {
  return encode(aggregate(partials, partials.length))
}

async function partialSignature (core, length) {
  if (length >= core.core.tree.length) length = null

  return {
    length: core.core.tree.length,
    signature: b4a.from(core.core.tree.signature),
    patch: await upgrade(core, length)
  }
}

async function upgrade (core, from) {
  if (!from && from !== 0) return null

  const tree = core.core.tree
  const p = await tree.proof({ upgrade: { start: from, length: tree.length - from } })
  return p.upgrade
}

function aggregate (inputs, thres) {
  let min = -1
  const selected = []

  for (let i = 0; i < inputs.length; i++) {
    const length = inputs[i].length
    const lowest = min < 0 ? null : selected[min]

    if (selected.length < thres) {
      const j = selected.push(inputs[i]) - 1
      if (!lowest || length < lowest.length) min = j
      continue
    }

    if (length <= lowest.length) continue
    selected[min] = inputs[i]
  }

  const length = selected[min].length

  const proofs = []
  const nodes = []

  for (const u of selected) {
    proofs.push(compressProof(u, nodes))
  }

  return {
    length,
    proofs,
    nodes
  }
}

function compareNode (a, b) {
  if (a.index !== b.index) return false
  if (a.size !== b.size) return false
  return b4a.equals(a.hash, b.hash)
}

function compressProof (proof, nodes) {
  const c = {}

  c.length = proof.length
  c.signature = proof.signature
  if (proof.patch) c.patch = compressUpgrade(proof.patch, nodes)

  return c
}

function compressUpgrade (p, nodes) {
  const u = {}

  u.length = p.length
  u.start = p.start
  u.signature = p.signature
  u.publicKey = null
  u.nodes = []

  for (const node of p.nodes) {
    let present = false
    for (let i = 0; i < nodes.length; i++) {
      if (!compareNode(nodes[i], node)) continue

      u.nodes.push(i)
      present = true
      break
    }

    if (present) continue
    u.nodes.push(nodes.push(node) - 1)
  }

  return u
}

function inflateUpgrade (s, nodes) {
  const upgrade = {
    start: s.start,
    length: s.length,
    signature: s.signature,
    nodes: [],
    additionalNodes: []
  }

  for (const i of s.nodes) {
    upgrade.nodes.push(nodes[i])
  }

  return {
    fork: 0,
    block: null,
    seek: null,
    hash: null,
    upgrade
  }
}
