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
// This shim deliberately declares no readable-stream dependency of its own.
// The copy that matters is the one each real consumer resolves (`mqtt` and
// `bl` behind `mqtt-packet`), and DSH installs the profile with a hoisted tree,
// so a direct dependency only pins whichever version happened to be resolved
// when the plugin was released. 0.3.13 did that with an exact 4.8.0, which
// forced every profile install/update to keep re-resolving that precise version
// from the registry; a stale lockfile or a slow registry could then fail the
// upgrade of an otherwise healthy plugin. Resolving the consumer edges at
// runtime keeps the preload correct whether the install hoists one copy or
// splits the tree into several.
//
// This is a no-op on DSH versions that fix the resolver; delete it once the fix
// is generally available. Same approach is used in the wild by
// @jcs130/dsh-minecraft-agent (lib/plugins/dsh-native-streams.mjs).

import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
let prepared = false

/**
 * Consumer chains whose dependency edge reaches readable-stream. `bl` sits one
 * level below `mqtt-packet`; `aedes` gets its own `mqtt-packet` edge because an
 * install may place a second copy under it.
 */
const CONSUMER_CHAINS = [
  ['mqtt'],
  ['mqtt-packet'],
  ['mqtt-packet', 'bl'],
  ['aedes'],
  ['aedes', 'mqtt-packet'],
  ['aedes', 'mqtt-packet', 'bl'],
]

/**
 * Walk a chain of package specifiers from the plugin's own location and return
 * the require anchored at the last package, or undefined when any edge is absent.
 * @param {readonly string[]} chain - bare specifiers, outermost first.
 */
function requireFor(chain) {
  let current = require
  for (const specifier of chain) {
    let entry
    try {
      entry = current.resolve(specifier)
    } catch {
      return undefined
    }
    current = createRequire(entry)
  }
  return current
}

/**
 * Absolute paths of every readable-stream copy this dependency tree can load.
 * A hoisted install shares one file; an isolated or split install can leave a
 * different copy under each consumer, and all of them must be primed.
 */
function readableStreamCopies() {
  const copies = new Set()
  try {
    // Harmless when the package is hoisted to the profile root or still listed
    // as a direct dependency by an older profile lockfile.
    copies.add(require.resolve('readable-stream'))
  } catch {
    // No direct edge: the consumer chains below decide which copies exist.
  }
  for (const chain of CONSUMER_CHAINS) {
    const consumer = requireFor(chain)
    if (consumer === undefined) continue
    try {
      copies.add(consumer.resolve('readable-stream'))
    } catch {
      // This consumer has no readable-stream edge in the current install.
    }
  }
  return copies
}

/**
 * Synchronously prime readable-stream with the native Node stream mode.
 * Safe to call more than once; never throws.
 */
export function prepareDshNativeStreams() {
  if (prepared) return
  const previous = process.env.READABLE_STREAM
  try {
    process.env.READABLE_STREAM = 'disable'
    for (const copy of readableStreamCopies()) {
      try {
        require(copy)
      } catch {
        // One broken copy must not keep the remaining copies cold.
      }
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
