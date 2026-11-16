// Host-compat suite: the readable-stream preload must not need a pinned copy.
//
// This is a pure module test. It builds a fake dependency tree with two
// different readable-stream copies (the layout a split install produces) and
// checks that `prepareDshNativeStreams()` primes both of them while
// READABLE_STREAM=disable is set, then restores the environment.
import { Suite, runSuite, assert, eq } from './harness.mjs'
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, copyFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const HERE = dirname(fileURLToPath(import.meta.url))
const PLUGIN_DIR = dirname(dirname(HERE))
const SHIM = join(PLUGIN_DIR, 'lib', 'dsh-native-streams.js')

/** Write a minimal package whose main module is `body`. */
function writePackage(root, name, body) {
  const dir = join(root, 'node_modules', ...name.split('/'))
  mkdirSync(dir, { recursive: true })
  writeFileSync(join(dir, 'package.json'), JSON.stringify({ name, version: '1.0.0', main: 'index.js' }))
  writeFileSync(join(dir, 'index.js'), body)
}

/**
 * Write a fake readable-stream that appends what it saw when it was loaded.
 * @param {string} dir - directory that must contain node_modules/readable-stream.
 * @param {string} marker - file that receives one JSON line per load.
 */
function writeFakeReadableStream(dir, marker) {
  const pkgDir = join(dir, 'node_modules', 'readable-stream')
  mkdirSync(pkgDir, { recursive: true })
  writeFileSync(join(pkgDir, 'package.json'), JSON.stringify({ name: 'readable-stream', version: '4.8.0', main: 'index.js' }))
  writeFileSync(join(pkgDir, 'index.js'), `
const { appendFileSync } = require('node:fs')
appendFileSync(${JSON.stringify(marker)}, JSON.stringify({ copy: ${JSON.stringify(pkgDir)}, env: process.env.READABLE_STREAM ?? null }) + '\\n')
module.exports = { Readable: function Readable() {} }
`)
}

function readMarker(path) {
  try {
    return readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line))
  } catch {
    return []
  }
}

const suite = new Suite('host-compat', 'readable-stream preload without a pinned copy')

await runSuite(suite, async () => {
  suite.group('package metadata')

  await suite.test('the plugin does not declare or pin readable-stream itself', () => {
    const manifest = JSON.parse(readFileSync(join(PLUGIN_DIR, 'package.json'), 'utf8'))
    assert(manifest.dependencies['readable-stream'] === undefined, 'no direct readable-stream dependency')
    assert(manifest.dependencies.mqtt !== undefined, 'mqtt stays a direct dependency')
    assert(manifest.dependencies['mqtt-packet'] !== undefined, 'mqtt-packet stays a direct dependency')
  })

  await suite.test('the plugin declares its DSH host requirement', () => {
    const manifest = JSON.parse(readFileSync(join(PLUGIN_DIR, 'package.json'), 'utf8'))
    const range = manifest.engines?.dsh
    assert(typeof range === 'string' && range.trim() !== '', 'engines.dsh is a non-empty range')
    assert(/^[~^<>=0-9]/.test(range.trim()), 'engines.dsh starts like a semver range')
  })

  suite.group('split install primes every copy')

  await suite.test('both the mqtt and the mqtt-packet -> bl copies are primed', async () => {
    const root = mkdtempSync(join(tmpdir(), 'dsh-mqtt-streams-'))
    const marker = join(root, 'loads.ndjson')
    const before = process.env.READABLE_STREAM
    try {
      writePackage(root, 'mqtt', 'module.exports = { connect() {} }\n')
      writePackage(root, 'mqtt-packet', 'module.exports = {}\n')
      writePackage(root, 'aedes', 'module.exports = {}\n')
      // mqtt's own edge.
      writeFakeReadableStream(join(root, 'node_modules', 'mqtt'), marker)
      // bl's edge below mqtt-packet.
      const packetDir = join(root, 'node_modules', 'mqtt-packet')
      writePackage(packetDir, 'bl', 'module.exports = {}\n')
      writeFakeReadableStream(join(packetDir, 'node_modules', 'bl'), marker)

      // Load a copy of the shim from the fixture so its require anchors here.
      const shimDir = join(root, 'lib')
      mkdirSync(shimDir, { recursive: true })
      const shimCopy = join(shimDir, 'dsh-native-streams.mjs')
      copyFileSync(SHIM, shimCopy)
      const mod = await import(pathToFileURL(shimCopy).href)

      mod.prepareDshNativeStreams()

      const loads = readMarker(marker)
      eq(loads.length, 2, 'both copies were loaded')
      assert(loads.every((load) => load.env === 'disable'), 'both copies loaded in native stream mode')
      assert(new Set(loads.map((load) => load.copy)).size === 2, 'the two loads are distinct files')
      eq(process.env.READABLE_STREAM, before, 'environment is restored')
    } finally {
      if (before === undefined) delete process.env.READABLE_STREAM
      else process.env.READABLE_STREAM = before
      rmSync(root, { recursive: true, force: true })
    }
  })

  await suite.test('a second call is a no-op and still does not throw', async () => {
    const root = mkdtempSync(join(tmpdir(), 'dsh-mqtt-streams-'))
    const before = process.env.READABLE_STREAM
    try {
      writePackage(root, 'mqtt', 'module.exports = {}\n')
      writeFakeReadableStream(join(root, 'node_modules', 'mqtt'), join(root, 'loads.ndjson'))
      const shimCopy = join(root, 'shim.mjs')
      copyFileSync(SHIM, shimCopy)
      const mod = await import(pathToFileURL(shimCopy).href)
      mod.prepareDshNativeStreams()
      mod.prepareDshNativeStreams()
      eq(process.env.READABLE_STREAM, before, 'environment is restored')
    } finally {
      if (before === undefined) delete process.env.READABLE_STREAM
      else process.env.READABLE_STREAM = before
      rmSync(root, { recursive: true, force: true })
    }
  })
})
