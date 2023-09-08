const c = require('compact-encoding')
const b4a = require('b4a')
const encoding = require('./messages').multiSignature

module.exports = {
  assemble,
  inflate,
  partialSignature,
  signableLength
}

function inflate (data) {
  const compressedInputs = c.decode(encoding, data)
  const inputs = []

  for (const proof of compressedInputs.proofs) {
    inputs.push({
      signer: proof.signer,
      signature: proof.signature,
      patch: inflateUpgrade(proof.patch, compressedInputs.nodes)
    })
  }

  return inputs
}

async function partialSignature (tree, signer, from, to = tree.length, signature = tree.signature) {
  if (from > tree.length) return null
  const patch = to <= from ? null : await upgrade(tree, from, to)

  return {
    signer,
    signature,
    patch
  }
}

async function upgrade (tree, from, to) {
  const p = await tree.proof({ upgrade: { start: from, length: to - from } })
  p.upgrade.additionalNodes = []
  p.upgrade.signature = null
  return p.upgrade
}

function signableLength (lengths, quorum) {
  if (quorum <= 0) quorum = 1
  if (quorum > lengths.length) return 0

  return lengths.sort(cmp)[quorum - 1]
}

function cmp (a, b) {
  return b - a
}

function assemble (inputs) {
  const proofs = []
  const nodes = []

  for (const u of inputs) {
    proofs.push(compressProof(u, nodes))
  }

  return c.encode(encoding, { proofs, nodes })
}

function compareNode (a, b) {
  if (a.index !== b.index) return false
  if (a.size !== b.size) return false
  return b4a.equals(a.hash, b.hash)
}

function compressProof (proof, nodes) {
  return {
    signer: proof.signer,
    signature: proof.signature,
    patch: compressUpgrade(proof.patch, nodes)
  }
}

function compressUpgrade (p, nodes) {
  if (!p) return null

  const u = {
    start: p.start,
    length: p.length,
    nodes: []
  }

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
  if (!s) return null

  const upgrade = {
    start: s.start,
    length: s.length,
    nodes: [],
    additionalNodes: [],
    signature: null
  }

  for (const i of s.nodes) {
    upgrade.nodes.push(nodes[i])
  }

  return upgrade
}
