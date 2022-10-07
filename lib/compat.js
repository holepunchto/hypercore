// Export the appropriate version of `quickbit-universal` as the plain import
// may resolve to an older version in some environments
let quickbit = require('quickbit-universal')
if (
  typeof quickbit.findFirst !== 'function' ||
  typeof quickbit.findLast !== 'function'
) {
  // This should always load the fallback from the locally installed version
  quickbit = require('quickbit-universal/fallback')
}
exports.quickbit = quickbit
