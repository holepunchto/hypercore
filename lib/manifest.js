const defaultCrypto = require('hypercore-crypto')
const b4a = require('b4a')

const { BAD_ARGUMENT } = require('hypercore-errors')

module.exports = parseManifest

function parseManifest (manifest, opts) {
  switch (manifest.type) {
    case 'STATIC':
      return staticTree({ treeHash: manifest.static.treeHash })

    case 'SIGNER':
      return singleSigner(parseSigner(manifest.signer, opts))

    case 'MULTI_SIGNERS': {
      const indexers = []
      for (const signer of manifest.multiSigners.signers) {
        indexers.push(singleSigner(parseSigner(signer)))
      }

      const { quorum, allowPatched } = manifest.multiSigners.quorum
      return multiSigner(indexers, quorum, { ...opts, allowPatched })
    }
  }
}

function parseSigner (signer, opts = {}) {
  if (!signer || !signer.publicKey) throw new Error('Malformed signer.')

  const keyPair = {
    publicKey: signer.publicKey,
    secretKey: opts.keyPair ? opts.keyPair.secretKey : null
  }

  switch (signer.signature) {
    case 'ED_25519':
      return {
        keyPair,
        crypto: defaultCrypto,
        entropy: signer.entropy
      }

    default:
      throw new Error('Only Ed25519 signatures are supported')
  }
}

function singleSigner (opts = {}) {
  const crypto = opts.crypto || defaultCrypto

  const keyPair = opts.keyPair
  if (!keyPair || (keyPair.secretKey && !crypto.validateKeyPair(keyPair))) {
    throw BAD_ARGUMENT('Invalid key pair')
  }

  const sign = opts.sign
    ? opts.sign
    : keyPair.secretKey
      ? (signable) => crypto.sign(signable, keyPair.secretKey)
      : undefined

  return {
    sign,
    verify (signable, signature) {
      return crypto.verify(signable, signature, keyPair.publicKey)
    }
  }
}

function staticTree ({ treeHash }, opts = {}) {
  if (!treeHash) throw BAD_ARGUMENT('Invalid tree hash')

  return {
    verify (signable) {
      return b4a.equals(signable, treeHash)
    }
  }
}

function multiSigner (indexers, threshold, opts = {}) {
  if (typeof threshold === 'object') return multiSigner(indexers, null, opts)
  if (!threshold) threshold = defaultQuorum(indexers.length)

  const allowPatched = !!opts.allowPatched

  return {
    sign: opts.sign,
    verify: allowPatched ? verifyPatched : verifySingle
  }

  function verifySingle (signable, signature) {
    if (b4a.isBuffer(signature)) return verifySingle(signable, MultiSigner.decode(signature))

    if (signature.proofs.length < threshold) return false

    let valid = 0
    const idx = indexers.slice(0)

    for (const proof of signature.proofs) {
      if (signed(signable, proof.signature, idx)) valid++
    }

    return valid >= threshold
  }

  function verifyPatched (_, signature, batch) {
    if (b4a.isBuffer(signature)) return verifyPatched(_, MultiSigner.decode(signature), batch)

    if (signature.proofs.length < threshold) return false

    let valid = 0
    const idx = indexers.slice(0)

    for (const proof of signature.proofs) {
      const ref = batch.clone()

      if (proof.patch) {
        const patch = unpack(proof.patch, signature.nodes)

        try {
          if (!ref.verifyUpgrade(patch)) continue
        } catch {
          continue
        }
      }

      if (signed(ref.signable(), proof.signature, idx)) valid++
    }

    return valid >= threshold
  }
}

// 51% majority
function defaultQuorum (n) {
  if (typeof n !== 'number') throw new Error('bad type.')
  return (n >> 1) + 1
}

function signed (signable, signature, idx) {
  for (let i = 0; i < idx.length; i++) {
    const indexer = idx[i]
    if (!indexer.verify(signable, signature)) continue

    const swap = idx.pop()
    if (indexer !== swap) idx[i] = swap

    return true
  }

  return false
}

function unpack (s, nodes) {
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
