const c = require('compact-encoding')
const b4a = require('b4a')
const flat = require('flat-tree')
const { MerkleTree } = require('./merkle-tree')
const { multiSignature, multiSignaturev0 } = require('./messages')

module.exports = {
  assemblev0,
  assemble,
  inflatev0,
  inflate,
  partialSignature,
  signableLength
}

function inflatev0 (data) {
  return c.decode(multiSignaturev0, data)
}

function inflate (data) {
  return c.decode(multiSignature, data)
}

async function partialSignature (core, signer, from, to = core.state.length, signature = core.state.signature) {
  if (from > core.state.length) return null
  const nodes = to <= from ? null : await upgradeNodes(core, from, to)

  if (signature.byteLength !== 64) signature = c.decode(multiSignature, signature).proofs[0].signature

  return {
    signer,
    signature,
    patch: nodes ? to - from : 0,
    nodes
  }
}

async function upgradeNodes (core, from, to) {
  const rx = core.state.storage.read()
  const p = await MerkleTree.proof(core.state, rx, { upgrade: { start: from, length: to - from } })
  rx.tryFlush()
  return (await p.settle()).upgrade.nodes
}

function signableLength (lengths, quorum) {
  if (quorum <= 0) quorum = 1
  if (quorum > lengths.length) return 0

  return lengths.sort(cmp)[quorum - 1]
}

function cmp (a, b) {
  return b - a
}

function assemblev0 (inputs) {
  const proofs = []
  const patch = []

  for (const u of inputs) {
    proofs.push(compressProof(u, patch))
  }

  return c.encode(multiSignaturev0, { proofs, patch })
}

function assemble (inputs) {
  const proofs = []
  const patch = []
  const seen = new Set()

  for (const u of inputs) {
    if (u.nodes) {
      for (const node of u.nodes) {
        if (seen.has(node.index)) continue
        seen.add(node.index)
        patch.push(node)
      }
    }

    proofs.push({
      signer: u.signer,
      signature: u.signature,
      patch: u.patch
    })
  }

  return c.encode(multiSignature, { proofs, patch })
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
    patch: proof.patch ? compressUpgrade(proof, nodes) : null
  }
}

function compressUpgrade (p, nodes) {
  const u = {
    start: flat.rightSpan(p.nodes[p.nodes.length - 1].index) / 2 + 1,
    length: p.patch,
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
