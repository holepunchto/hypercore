#!/usr/bin/env node
'use strict'
// Deterministic TEST-ONLY key material. Never use this seed for real data.
const fs = require('node:fs/promises')
const path = require('node:path')
const os = require('node:os')
const assert = require('node:assert/strict')
const { execFileSync } = require('node:child_process')
const Hypercore = require('../..')
const crypto = require('hypercore-crypto')
const caps = require('../../lib/caps')
const { MerkleTree } = require('../../lib/merkle-tree')
const Verifier = require('../../lib/verifier')

const hex = (bytes) => Buffer.from(bytes).toString('hex')
const node = (entry) => ({ index: entry.index, hash: hex(entry.hash), length: entry.size })
function proof(value) {
  assert(value.upgrade, 'fixture proof must contain a signed head')
  assert.equal(value.fork, 0)
  return {
    fork: value.fork,
    block: value.block ? { index: value.block.index, value: hex(value.block.value), nodes: value.block.nodes.map(node) } : null,
    upgrade: {
      start: value.upgrade.start, length: value.upgrade.length,
      nodes: value.upgrade.nodes.map(node), additional_nodes: value.upgrade.additionalNodes.map(node),
      signature: hex(value.upgrade.signature)
    }
  }
}
async function main() {
  const args = process.argv.slice(2)
  if (args.length && (args.length !== 2 || args[0] !== '--output')) {
    throw new Error('Usage: node scripts/rust/generate-js-fixtures.cjs [--output file.json]')
  }
  const output = path.resolve(args[1] || path.join(__dirname, '../../fixtures/rust/js11-compat.json'))
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'shadw-js11-fixture-'))
  const seed = Buffer.alloc(32, 7)
  const pair = crypto.keyPair(seed)
  const blocks = [Buffer.alloc(0), Buffer.from('hello'), Buffer.from('University: π / 学生'), Buffer.from([0, 1, 127, 128, 255]), Buffer.from('fifth block: non-power-of-two forest')]
  let writer, reader, modern
  try {
    writer = new Hypercore(path.join(temp, 'writer'), { compat: true, keyPair: pair })
    await writer.ready()
    assert.equal(writer.core.compat, true)
    assert.equal(hex(writer.key), hex(pair.publicKey))
    const heads = []
    for (let length = 0; length <= blocks.length; length++) {
      if (length > 0) await writer.append(blocks[length - 1])
      if (![0, 1, 2, 3, 5].includes(length)) continue
      const roots = await MerkleTree.getRoots(writer.state, length)
      const treeHash = await writer.treeHash(length)
      assert.equal(hex(treeHash), hex(crypto.tree(roots)))
      const signable = caps.treeSignableCompat(treeHash, length, writer.fork)
      assert.equal(signable.length, 80)
      const signature = length ? writer.state.signature : null
      if (signature) assert(crypto.verify(signable, signature, pair.publicKey), 'actual compat signature must verify')
      heads.push({ length, byte_length: roots.reduce((total, root) => total + root.size, 0), fork: writer.fork,
        roots: roots.map(node), tree_hash: hex(treeHash), signable: hex(signable), signature: signature ? hex(signature) : null })
    }
    const head = await writer.proof({ upgrade: { start: 0, length: writer.length } })
    const blockProofs = []
    for (let index = 0; index < blocks.length; index++) {
      blockProofs.push(await writer.proof({ block: { index, nodes: 0 }, upgrade: { start: 0, length: writer.length } }))
    }
    reader = new Hypercore(path.join(temp, 'reader'), pair.publicKey, { compat: true })
    await reader.ready()
    assert.equal(reader.writable, false)
    await reader.verifyFullyRemote(head)
    assert(await reader.applyProof(head), 'JS must accept its independently keyed signed-head proof')
    for (const p of blockProofs) {
      await reader.verifyFullyRemote(p)
      assert(await reader.applyProof({ ...p, upgrade: null }), 'JS must accept the sparse block proof')
      assert.deepEqual(await reader.get(p.block.index, { wait: false }), blocks[p.block.index])
    }
    const leaves = blocks.map((bytes, index) => ({ index: index * 2, size: bytes.length, hash: crypto.data(bytes) }))
    const parent = { index: 1, size: leaves[0].size + leaves[1].size, hash: crypto.parent(leaves[0], leaves[1]) }
    assert.equal(hex(parent.hash), hex(crypto.parent(leaves[1], leaves[0])))
    // Keep a non-compat manifest vector separate: it is not part of the supported Rust mode.
    modern = new Hypercore(path.join(temp, 'manifest-v1'), { compat: false, keyPair: pair })
    await modern.ready()
    await modern.append(blocks[1])
    const modernHash = await modern.treeHash()
    const manifestPreimage = caps.treeSignable(modern.key, modernHash, modern.length, modern.fork)
    assert.equal(manifestPreimage.length, 112)
    const modernProof = await modern.proof({ upgrade: { start: 0, length: modern.length } })
    await modern.verifyFullyRemote(modernProof)
    const revision = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: path.join(__dirname, '../..'), encoding: 'utf8' }).trim()
    const fixture = {
      format: 'shadw-js11-compat-fixture-v1',
      provenance: {
        source: 'https://github.com/SolutionsAsService/shadw-core', source_revision: revision,
        hypercore: require('../../package.json').version,
        hypercore_crypto: require('hypercore-crypto/package.json').version,
        hypercore_storage: require('hypercore-storage/package.json').version,
        sodium_universal: require('sodium-universal/package.json').version,
        node: process.version,
        scope: 'Actual JS11 native runtime; explicit compat single signer; fork zero; plaintext bytes. Not JS11 wire or storage parity.'
      },
      test_only_seed: hex(seed), warning: 'PUBLIC DETERMINISTIC TEST-ONLY SEED. NEVER USE FOR REAL DATA.',
      public_key: hex(pair.publicKey), discovery_key: hex(crypto.discoveryKey(writer.key)),
      namespaces: { tree: hex(crypto.namespace('hypercore', 6)[0]), manifest: hex(caps.MANIFEST), default_namespace: hex(caps.DEFAULT_NAMESPACE) },
      blocks: blocks.map(hex), leaves: leaves.map(node), parent: node(parent), heads,
      bundle: { format: 'shadw-proof-bundle-v1', public_key: hex(writer.key), length: writer.length,
        head: proof(head), blocks: blockProofs.map(proof) },
      unsupported_manifest_v1: { key: hex(modern.key), encoded_manifest: hex(Verifier.encodeManifest(modern.manifest)),
        tree_hash: hex(modernHash), signable: hex(manifestPreimage), signature_envelope: hex(modern.state.signature),
        scope: 'Reference only: not evidence that the Rust compatibility subset supports non-compat manifests.' }
    }
    await fs.mkdir(path.dirname(output), { recursive: true })
    await fs.writeFile(output, JSON.stringify(fixture, null, 2) + '\n', { mode: 0o644 })
    console.log(JSON.stringify({ output, verified: true, hypercore: fixture.provenance.hypercore,
      hypercore_crypto: fixture.provenance.hypercore_crypto, heads: heads.length, block_proofs: blockProofs.length, public_key: fixture.public_key }))
  } finally {
    await Promise.allSettled([writer, reader, modern].filter(Boolean).map(core => core.close()))
    // This directory was created by this invocation and contains only test-key stores.
    await fs.rm(temp, { recursive: true, force: true })
  }
}
main().catch(error => { console.error(error.stack); process.exitCode = 1 })
