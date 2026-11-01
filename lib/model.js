/**
 * The plugin's domain model: defaults and normalisation for the values the
 * panel edits, the HTTP API saves and the broker enforces.
 *
 * This module is deliberately persistence-free: `lib/store.js` owns the file
 * and calls in here, `lib/clients.js`/`lib/broker.js` only read and write
 * model-shaped objects. Keeping the two apart means a settings change can be
 * unit-tested without a state file.
 */
import { randomUUID } from 'node:crypto'

/** Maximum number of saved connection profiles. */
export const MAX_PROFILES = 50

/** MQTT protocol versions the embedded broker can accept, in display order. */
export const BROKER_PROTOCOLS = ['3.1', '3.1.1', '5.0']

/** Retained messages kept across broker restarts (bounded so the state file stays sane). */
export const MAX_STORED_RETAINED = 500

/** A single persisted retained payload, base64-encoded (matches maxPayloadBytes' default cap). */
const MAX_RETAINED_STORED_PAYLOAD = Math.ceil((2 * 1024 * 1024 * 4) / 3)

/** QoS is not a suggestion: a call that asks for anything else is a caller bug. */
export function normalizeQos(value) {
  const qos = Number(value ?? 0)
  if (!Number.isInteger(qos) || qos < 0 || qos > 2) throw new Error('qos must be 0, 1 or 2')
  return qos
}

function clampNumber(value, fallback, min, max) {
  const number = Number(value)
  if (!Number.isFinite(number)) return fallback
  return Math.min(max, Math.max(min, Math.round(number)))
}

function text(value, fallback = '', max = 512) {
  if (typeof value !== 'string') return fallback
  return value.length > max ? value.slice(0, max) : value
}

function boolean(value, fallback) {
  return typeof value === 'boolean' ? value : fallback
}

/** One new connection profile with sane defaults. */
export function defaultProfile(overrides = {}) {
  return {
    id: randomUUID(),
    name: 'New connection',
    url: 'mqtt://127.0.0.1:1883',
    clientId: '',
    username: '',
    password: '',
    keepalive: 60,
    clean: true,
    protocolVersion: 4,
    connectTimeout: 10000,
    reconnectPeriod: 3000,
    rejectUnauthorized: true,
    autoConnect: false,
    subscriptions: [],
    ...overrides,
  }
}

/**
 * Broker defaults: stopped until the user starts it, every protocol accepted,
 * generous but finite resource limits so one misbehaving client cannot pin the
 * whole process (all of them can be changed through the command API).
 */
export function defaultBroker() {
  return {
    host: '0.0.0.0',
    port: 1883,
    autoStart: false,
    allowAnonymous: true,
    protocols: [...BROKER_PROTOCOLS],
    users: [],
    /** 0 means unlimited. */
    maxConnections: 200,
    connectTimeoutMs: 10000,
    maxPayloadBytes: 1024 * 1024,
    maxRetainedMessages: 1000,
  }
}

function normalizeSubscription(raw) {
  if (raw === null || typeof raw !== 'object') return null
  const topic = text(raw.topic, '', 512).trim()
  if (!topic) return null
  const qos = clampNumber(raw.qos, 0, 0, 2)
  return { topic, qos }
}

export function normalizeProfile(raw) {
  if (raw === null || typeof raw !== 'object') return null
  const profile = defaultProfile()
  profile.id = text(raw.id, '', 64) || profile.id
  profile.name = text(raw.name, 'Connection', 120) || 'Connection'
  profile.url = text(raw.url, profile.url, 1024).trim() || profile.url
  profile.clientId = text(raw.clientId, '', 256)
  profile.username = text(raw.username, '', 256)
  profile.password = typeof raw.password === 'string' ? raw.password : ''
  profile.keepalive = clampNumber(raw.keepalive, 60, 0, 65535)
  profile.clean = boolean(raw.clean, true)
  profile.protocolVersion = [3, 4, 5].includes(Number(raw.protocolVersion)) ? Number(raw.protocolVersion) : 4
  profile.connectTimeout = clampNumber(raw.connectTimeout, 10000, 1000, 300000)
  profile.reconnectPeriod = clampNumber(raw.reconnectPeriod, 3000, 0, 600000)
  profile.rejectUnauthorized = boolean(raw.rejectUnauthorized, true)
  profile.autoConnect = boolean(raw.autoConnect, false)
  const seen = new Set()
  profile.subscriptions = []
  if (Array.isArray(raw.subscriptions)) {
    for (const candidate of raw.subscriptions) {
      const subscription = normalizeSubscription(candidate)
      if (subscription === null || seen.has(subscription.topic)) continue
      seen.add(subscription.topic)
      profile.subscriptions.push(subscription)
    }
  }
  return profile
}

function normalizeUser(raw) {
  if (raw === null || typeof raw !== 'object') return null
  const username = text(raw.username, '', 256).trim()
  if (!username) return null
  return { username, password: typeof raw.password === 'string' ? raw.password : '' }
}

/** Accept only the known protocol labels, in canonical order; never empty. */
function normalizeProtocols(raw) {
  if (!Array.isArray(raw)) return [...BROKER_PROTOCOLS]
  const wanted = new Set(raw.filter((value) => BROKER_PROTOCOLS.includes(value)))
  const accepted = BROKER_PROTOCOLS.filter((value) => wanted.has(value))
  return accepted.length > 0 ? accepted : [...BROKER_PROTOCOLS]
}

export function normalizeBroker(raw) {
  const broker = defaultBroker()
  if (raw === null || typeof raw !== 'object') return broker
  broker.host = text(raw.host, broker.host, 256).trim() || broker.host
  broker.port = clampNumber(raw.port, 1883, 1, 65535)
  broker.autoStart = boolean(raw.autoStart, false)
  broker.allowAnonymous = boolean(raw.allowAnonymous, true)
  broker.protocols = normalizeProtocols(raw.protocols)
  broker.users = []
  if (Array.isArray(raw.users)) {
    for (const candidate of raw.users) {
      const user = normalizeUser(candidate)
      if (user !== null) broker.users.push(user)
    }
  }
  broker.maxConnections = clampNumber(raw.maxConnections, broker.maxConnections, 0, 5000)
  broker.connectTimeoutMs = clampNumber(raw.connectTimeoutMs, broker.connectTimeoutMs, 500, 120000)
  broker.maxPayloadBytes = clampNumber(raw.maxPayloadBytes, broker.maxPayloadBytes, 1024, 16 * 1024 * 1024)
  broker.maxRetainedMessages = clampNumber(raw.maxRetainedMessages, broker.maxRetainedMessages, 0, 100000)
  return broker
}

/**
 * The retained set the broker replays after a restart. Only what the broker
 * itself accepted is stored, so the payload budget was already enforced; this
 * pass only drops rows that cannot be replayed (bad topic, oversized blob,
 * duplicates) and keeps the state file bounded.
 */
export function normalizeRetained(raw) {
  const entries = []
  if (!Array.isArray(raw)) return entries
  const seen = new Set()
  for (const candidate of raw) {
    if (candidate === null || typeof candidate !== 'object') continue
    const topic = text(candidate.topic, '', 65535).trim()
    if (!topic || topic.startsWith('$SYS/') || seen.has(topic)) continue
    const payload = typeof candidate.payload === 'string' ? candidate.payload : ''
    if (payload.length > MAX_RETAINED_STORED_PAYLOAD) continue
    seen.add(topic)
    entries.push({ topic, payload, qos: clampNumber(candidate.qos, 0, 0, 2) })
    if (entries.length >= MAX_STORED_RETAINED) break
  }
  return entries
}
