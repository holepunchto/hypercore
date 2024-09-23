// This runner is auto-generated by Brittle

runTests()

async function runTests () {
  const test = (await import('brittle')).default

  test.pause()

  await import('./basic.js') // todo: implement storageInfo API
  await import('./batch.js') // todo: implement batch api
  await import('./bitfield.js')
  await import('./clear.js') // todo: replace Info.bytesUsed API
  // await import('./compat.js') // todo: how to test compat?
  await import('./conflicts.js')
  await import('./core.js')
  await import('./draft.js')
  await import('./encodings.js')
  await import('./encryption.js')
  await import('./extension.js')
  await import('./manifest.js')
  await import('./merkle-tree.js')
  await import('./mutex.js')
  // await import('./oplog.js')
  await import('./preload.js')
  // await import('./purge.js') // todo: implement purge
  await import('./remote-bitfield.js')
  await import('./remote-length.js')
  await import('./replicate.js') // todo: append event timing
  await import('./sessions.js')
  await import('./snapshots.js')
  // await import('./storage.js')
  await import('./streams.js')
  await import('./timeouts.js')
  await import('./user-data.js')

  test.resume()
}
