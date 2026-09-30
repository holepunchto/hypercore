#!/usr/bin/env node
'use strict'
// Interoperability verifier, not a production network import service.
const fs = require('node:fs/promises')
const path = require('node:path')
const os = require('node:os')
const assert = require('node:assert/strict')
const Hypercore = require('../..')

function bytes(value, length) {
  assert.equal(typeof value, 'string')
  assert(/^(?:[0-9a-f]{2})*$/.test(value), 'expected canonical hexadecimal bytes')
  const result = Buffer.from(value, 'hex')
  if (length !== undefined) assert.equal(result.length, length)
  return result
}
function uint(value) { assert(Number.isSafeInteger(value) && value >= 0); return value }
function node(value) { return { index: uint(value.index), size: uint(value.length), hash: bytes(value.hash, 32) } }
function proof(value) {
  assert.equal(value.fork, 0)
  assert(value.upgrade)
  return { fork: 0, hash: null, seek: null, manifest: null,
    block: value.block ? { index: uint(value.block.index), value: bytes(value.block.value), nodes: value.block.nodes.map(node) } : null,
    upgrade: { start: uint(value.upgrade.start), length: uint(value.upgrade.length), nodes: value.upgrade.nodes.map(node),
      additionalNodes: value.upgrade.additional_nodes.map(node), signature: bytes(value.upgrade.signature, 64) } }
}
async function main() {
  const args = process.argv.slice(2)
  assert(args.length === 4 && args[0] === '--bundle' && args[2] === '--key',
    'Usage: node scripts/rust/verify-rust-bundle.cjs --bundle FILE --key INDEPENDENT_PUBLIC_KEY_HEX')
  const pinnedKey = bytes(args[3], 32)
  const info = await fs.stat(args[1])
  assert(info.size <= 40 * 1024 * 1024, 'bundle too large')
  const bundle = JSON.parse(await fs.readFile(args[1], 'utf8'))
  assert.equal(bundle.format, 'shadw-proof-bundle-v1')
  assert.equal(bundle.public_key, args[3], 'bundle does not match independently pinned key')
  assert(uint(bundle.length) <= 1_000_000)
  assert(Array.isArray(bundle.blocks) && bundle.blocks.length <= 4096)
  assert(bundle.length > 0 && bundle.head, 'interop fixture must contain a signed head')
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'shadw-rust-proof-check-'))
  let reader
  try {
    reader = new Hypercore(path.join(temp, 'reader'), pinnedKey, { compat: true })
    await reader.ready()
    assert.equal(reader.writable, false)
    const head = proof(bundle.head)
    assert.equal(head.block, null)
    await reader.verifyFullyRemote(head)
    assert(await reader.applyProof(head), 'JS rejected the Rust signed head')
    assert.equal(reader.length, bundle.length)
    const seen = new Set()
    for (const item of bundle.blocks) {
      const p = proof(item)
      assert(p.block && p.block.index < bundle.length && !seen.has(p.block.index))
      seen.add(p.block.index)
      const verified = await reader.verifyFullyRemote(p)
      assert.equal(verified.length, bundle.length)
      assert(await reader.applyProof({ ...p, upgrade: null }), 'JS rejected a Rust block proof')
      assert.deepEqual(await reader.get(p.block.index, { wait: false }), p.block.value)
    }
    console.log(JSON.stringify({ verified: true, direction: 'Rust to JavaScript Hypercore',
      hypercore: require('../../package.json').version, public_key: args[3], length: reader.length,
      verified_blocks: seen.size, tree_hash: (await reader.treeHash()).toString('hex') }))
  } finally {
    if (reader) await reader.close()
    await fs.rm(temp, { recursive: true, force: true })
  }
}
main().catch(error => { console.error(error.stack); process.exitCode = 1 })
