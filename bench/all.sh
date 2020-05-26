run () {
  echo '> node write-16kb-blocks.js'
  node write-16kb-blocks.js
  echo
  echo '> node write-512b-blocks.js'
  node write-512b-blocks.js
  echo
  echo '> node write-64kb-blocks-static.js'
  node write-64kb-blocks-static.js
  echo
  echo '> node write-64kb-blocks.js'
  node write-64kb-blocks.js
  echo
  echo '> node copy-64kb-blocks.js'
  node copy-64kb-blocks.js
  echo
  echo '> node read-16kb-blocks-proof.js'
  node read-16kb-blocks-proof.js
  echo
  echo '> node read-16kb-blocks.js'
  node read-16kb-blocks.js
  echo
  echo '> node read-512b-blocks.js'
  node read-512b-blocks.js
  echo
  echo '> node read-64kb-blocks-linear.js'
  node read-64kb-blocks-linear.js
  echo
  echo '> node read-64kb-blocks-linear-batch.js'
  node read-64kb-blocks-linear-batch.js
  echo
  echo '> node read-64kb-blocks-proof.js'
  node read-64kb-blocks-proof.js
  echo
  echo '> node read-64kb-blocks.js'
  node read-64kb-blocks.js
  echo
  echo '> node replicate-16kb-blocks.js'
  node replicate-16kb-blocks.js
  echo
  echo '> node replicate-64kb-blocks.js'
  node replicate-64kb-blocks.js
  echo
}

clear_cache () {
  echo '> rm -rf cores'
  rm -rf cores
  echo
}

echo "> git checkout $(git log --pretty=format:'%h' -n 1)"
echo

echo '# clearing cache'
echo
clear_cache

echo '# running all benchmarks'
echo
run

echo '# re-running all benchmarks'
echo
run
