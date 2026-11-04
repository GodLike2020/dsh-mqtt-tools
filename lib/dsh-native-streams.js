// DSH ≤ 0.2.2-alpha.2 compatibility shim for plugins whose dependency tree
// includes readable-stream@4.x (mqtt, aedes, mqtt-packet, bl, concat-stream, ...).
//
// Why: readable-stream loads its Node-stream shims eagerly enough that a
// `require('process/')` (trailing slash, intentional npm-polyfill request) is
// executed from lib/internal/shims/process.js. DSH's profile resolution router
// normalizes that request to the builtin name `process`, then calls
// createRequire(parent).resolve.paths('process'), which returns null for
// builtins and crashes the router with:
//   TypeError: createRequire.resolve.paths is not a function or its return value is not iterable
//
// Fix that lives here (plugin side, transitional): readable-stream's own
// lib/ours/index.js exports the native Node stream implementation directly when
//   process.env.READABLE_STREAM === 'disable'
// and in that branch it never requires lib/internal/shims/process.js, so the
// trailing-slash request is never made.
//
// The preload MUST be synchronous: the env change must not survive an await
// boundary, otherwise concurrent imports could observe READABLE_STREAM=disable.
//
// This package pins readable-stream@4.8.0 while mqtt declares ^4.7.0 and
// mqtt-packet -> bl declares ^4.2.0. A future reinstall can resolve a second 4.x
// copy for those consumers; the extra warm-up below primes whatever copy mqtt
// resolves as well, so that split does not bring the crash back.
//
// This is a no-op on DSH versions that fix the resolver; delete it (and the
// dependency entry) once the fix is generally available. Same approach is used
// in the wild by @jcs130/dsh-minecraft-agent (lib/plugins/dsh-native-streams.mjs).

import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
let prepared = false

/**
 * Synchronously prime readable-stream with the native Node stream mode.
 * Safe to call more than once; never throws.
 */
export function prepareDshNativeStreams() {
  if (prepared) return
  const previous = process.env.READABLE_STREAM
  try {
    process.env.READABLE_STREAM = 'disable'
    const own = require.resolve('readable-stream')
    require('readable-stream')
    try {
      // mqtt (and bl behind mqtt-packet) resolve their own readable-stream edge;
      // if an install splits the tree, warm that copy while the env is still set.
      const mqttRequire = createRequire(require.resolve('mqtt'))
      const consumer = mqttRequire.resolve('readable-stream')
      if (consumer !== own) require(consumer)
    } catch {
      // mqtt is a declared dependency; never let this break plugin startup.
    }
    prepared = true
  } catch {
    // A compatibility shim must never prevent the plugin from starting.
  } finally {
    if (previous === undefined) delete process.env.READABLE_STREAM
    else process.env.READABLE_STREAM = previous
  }
}

/** Test seam: allow tests to re-run the preload. */
export function resetDshNativeStreamsForTests() {
  prepared = false
}
