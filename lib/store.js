/**
 * Persistent plugin state: the state file lives in `$DSH_HOME` and holds the
 * connection profiles, the broker settings and the log preferences.
 *
 * The defaults and normalisers themselves live in `lib/model.js`; this module
 * only owns the file (load, migrate, debounced atomic write) and re-exports the
 * model helpers so existing imports keep working.
 */
import { promises as fs } from 'node:fs'
import path from 'node:path'
import os from 'node:os'
import { oneLine } from './log.js'
import {
  MAX_PROFILES,
  defaultBroker,
  normalizeBroker,
  normalizeProfile,
  normalizeRetained,
} from './model.js'

export {
  BROKER_PROTOCOLS,
  MAX_PROFILES,
  MAX_STORED_RETAINED,
  defaultBroker,
  defaultProfile,
  normalizeBroker,
  normalizeProfile,
  normalizeQos,
  normalizeRetained,
} from './model.js'

const STATE_VERSION = 1

/** Directory that holds plugin state: `$DSH_HOME`, default `~/.dsh`. */
export function stateHome() {
  return process.env.DSH_HOME || path.join(os.homedir(), '.dsh')
}

/** Where the state file lives: `$DSH_HOME/dsh-mqtt-tools.json`. */
export function statePath() {
  return path.join(stateHome(), 'dsh-mqtt-tools.json')
}

/** File name used before 0.3.0; adopted on first load when the new file is absent. */
export function legacyStatePath() {
  return path.join(stateHome(), 'dsh-mqtt.json')
}

function log(...args) {
  try {
    console.error('[dsh-mqtt-tools]', ...args)
  } catch {
    /* a logging failure is not the plugin's failure */
  }
}

function normalizeState(raw) {
  const state = {
    version: STATE_VERSION,
    clients: [],
    broker: defaultBroker(),
    log: { showSystem: false },
    // Retained topics survive a broker restart: the broker replays them when it
    // starts again, so a subscriber that reconnects still gets the last value.
    retained: [],
  }
  if (raw === null || typeof raw !== 'object') return state
  if (Array.isArray(raw.clients)) {
    const seen = new Set()
    for (const candidate of raw.clients.slice(0, MAX_PROFILES)) {
      const profile = normalizeProfile(candidate)
      if (profile === null || seen.has(profile.id)) continue
      seen.add(profile.id)
      state.clients.push(profile)
    }
  }
  state.broker = normalizeBroker(raw.broker)
  state.retained = normalizeRetained(raw.retained)
  if (raw.log !== null && typeof raw.log === 'object') {
    state.log.showSystem = typeof raw.log.showSystem === 'boolean' ? raw.log.showSystem : false
  }
  return state
}

/** Debounced atomic writer over one JSON state file. */
export class Store {
  constructor(file = statePath()) {
    this.file = file
    this.data = normalizeState(undefined)
    this.timer = null
    this.pending = null
    this.ready = this.load()
  }

  async load() {
    try {
      const raw = JSON.parse(await fs.readFile(this.file, 'utf8'))
      this.data = normalizeState(raw)
      return
    } catch (error) {
      if (error?.code === 'ENOENT') return this.adoptLegacy()
      // The file is there but cannot be parsed (truncated write, hand edit, disk
      // damage). Move it aside before falling back to defaults: the next save
      // would otherwise overwrite the only copy of the user's profiles.
      await this.quarantine()
      log(`cannot read ${this.file}: ${oneLine(error)}; the damaged file was set aside and the previous file is tried`)
      return this.adoptLegacy()
    }
  }

  /** Keep a damaged state file next to the original instead of overwriting it. */
  async quarantine() {
    const stamp = new Date().toISOString().replace(/[:.]/g, '-')
    const aside = `${this.file}.corrupt-${stamp}`
    try {
      await fs.rename(this.file, aside)
      log(`the damaged state file was kept at ${aside}`)
    } catch (error) {
      log(`cannot set the damaged state file aside: ${oneLine(error)}`)
    }
  }

  /** 0.2.x stored the same state in `dsh-mqtt.json`; adopt it once. */
  async adoptLegacy() {
    const legacy = legacyStatePath()
    if (legacy === this.file) return
    try {
      await fs.copyFile(legacy, this.file)
      const raw = JSON.parse(await fs.readFile(this.file, 'utf8'))
      this.data = normalizeState(raw)
      log(`migrated ${legacy} -> ${this.file}`)
    } catch (error) {
      if (error?.code !== 'ENOENT') log(`cannot migrate ${legacy}: ${oneLine(error)}`)
    }
  }

  snapshot() {
    return this.data
  }

  /** Mutate the state and schedule one debounced write. */
  update(mutator) {
    mutator(this.data)
    this.saveSoon()
  }

  saveSoon() {
    if (this.timer !== null) return
    this.timer = setTimeout(() => {
      this.timer = null
      this.flush().catch((error) => log(`cannot save ${this.file}: ${oneLine(error)}`))
    }, 300)
    this.timer.unref?.()
  }

  async flush() {
    const write = async () => {
      await fs.mkdir(path.dirname(this.file), { recursive: true })
      const temporary = `${this.file}.${process.pid}.tmp`
      await fs.writeFile(temporary, JSON.stringify(this.data, null, 2), 'utf8')
      await fs.rename(temporary, this.file)
    }
    this.pending = (this.pending ?? Promise.resolve()).then(write, write)
    return this.pending
  }

  async dispose() {
    if (this.timer !== null) {
      clearTimeout(this.timer)
      this.timer = null
    }
    try {
      await this.flush()
    } catch (error) {
      log(`cannot save ${this.file}: ${oneLine(error)}`)
    }
  }
}
