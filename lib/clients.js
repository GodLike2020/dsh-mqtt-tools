/** MQTT client connections: connection pool, subscriptions, publish, receive. */
import { normalizeProfile, defaultProfile, normalizeQos } from './store.js'
import { describePayload, oneLine } from './log.js'

const MAX_PROFILES = 50
const PUBLISH_TIMEOUT_MS = 15000

function log(...args) {
  try {
    console.error('[dsh-mqtt-tools]', ...args)
  } catch {
    /* ignore */
  }
}

let mqttModule = null
let mqttLoading = null
let mqttError = null

/** Load mqtt lazily so a missing dependency never prevents the plugin from mounting. */
async function loadMqtt() {
  if (mqttModule !== null) return mqttModule
  if (mqttLoading === null) {
    mqttLoading = import('mqtt')
      .then((mod) => {
        const candidate = mod?.default?.connect ? mod.default : mod
        if (typeof candidate?.connect !== 'function') throw new Error('mqtt package has no connect() export')
        mqttModule = candidate
        mqttError = null
        return candidate
      })
      .catch((error) => {
        mqttLoading = null
        mqttError = oneLine(error)
        throw new Error(`the mqtt package is not available: ${oneLine(error)}`)
      })
  }
  return mqttLoading
}

function connectOptions(profile) {
  const options = {
    keepalive: profile.keepalive,
    clean: profile.clean,
    protocolVersion: profile.protocolVersion,
    connectTimeout: profile.connectTimeout,
    reconnectPeriod: profile.reconnectPeriod,
    rejectUnauthorized: profile.rejectUnauthorized,
  }
  if (profile.clientId) options.clientId = profile.clientId
  if (profile.username) options.username = profile.username
  if (profile.password) options.password = profile.password
  return options
}

function payloadBuffer(text, encoding) {
  const value = typeof text === 'string' ? text : text === undefined || text === null ? '' : String(text)
  return Buffer.from(value, encoding === 'base64' ? 'base64' : 'utf8')
}

/** Fields that decide which broker/session the socket talks to. */
const DIAL_FIELDS = ['url', 'username', 'password', 'clientId', 'keepalive', 'clean', 'protocolVersion', 'connectTimeout', 'reconnectPeriod', 'rejectUnauthorized']

function dialSignature(profile) {
  return DIAL_FIELDS.map((key) => `${key}=${profile[key] === undefined || profile[key] === null ? '' : String(profile[key])}`).join('|')
}

/** All browser-facing connection state plus the MQTT engine behind it. */
export class ClientManager {
  constructor({ store, messages, onChange }) {
    this.store = store
    this.messages = messages
    /** Called whenever connection/profiles change and the page should refresh. */
    this.onChange = onChange ?? (() => {})
    /** Called with every received message entry. */
    this.onMessage = () => {}
    /** id -> { profileId, client, status, error, since, manual, subscriptions } */
    this.connections = new Map()
  }

  library() {
    return { mqtt: mqttModule !== null, busy: mqttLoading !== null, error: mqttError }
  }

  /**
   * Try the import in the background right after mounting. Without this the
   * panel cannot tell "not loaded yet" from "the package is missing".
   */
  warm() {
    loadMqtt()
      .then(() => this.onChange())
      .catch(() => this.onChange())
  }

  find(id) {
    return this.store.data.clients.find((profile) => profile.id === id) ?? null
  }

  view(profile) {
    const connection = this.connections.get(profile.id)
    const { password, ...rest } = profile
    return {
      ...rest,
      hasPassword: typeof password === 'string' && password.length > 0,
      status: connection?.status ?? 'idle',
      error: connection?.error ?? null,
      connectedAt: connection?.since ?? null,
    }
  }

  list() {
    return this.store.data.clients.map((profile) => this.view(profile))
  }

  /** Create or update one connection profile; an empty password keeps the stored one. */
  async save(input) {
    if (input === null || typeof input !== 'object') throw new Error('profile is required')
    await this.store.ready
    const list = this.store.data.clients
    const existing = typeof input.id === 'string' ? list.find((profile) => profile.id === input.id) : undefined
    if (existing === undefined && list.length >= MAX_PROFILES) throw new Error(`at most ${MAX_PROFILES} connections are supported`)
    const merged = {
      ...(existing ?? defaultProfile()),
      ...input,
      id: existing?.id ?? (typeof input.id === 'string' && input.id ? input.id : undefined),
      password:
        typeof input.password === 'string' && input.password !== ''
          ? input.password
          : existing?.password ?? '',
    }
    const profile = normalizeProfile(merged)
    if (existing === undefined) {
      profile.id = typeof input.id === 'string' && input.id ? input.id : profile.id
      this.store.update((state) => {
        state.clients.push(profile)
      })
    } else {
      profile.id = existing.id
      this.store.update((state) => {
        state.clients = state.clients.map((candidate) => (candidate.id === profile.id ? profile : candidate))
      })
      if (this.connections.has(profile.id)) {
        const connection = this.connections.get(profile.id)
        const dialsElsewhere = dialSignature(connection.profile) !== dialSignature(profile)
        connection.profile = profile
        if (dialsElsewhere) {
          // The socket was opened with the old URL or credentials. Keeping it
          // alive while the card shows the new target would publish to the old
          // broker and claim "connected"; drop the session so the card tells
          // the truth and the next connect dials the new settings.
          connection.status = 'idle'
          this.release(connection)
        }
      }
    }
    this.onChange()
    return this.view(profile)
  }

  async remove(id) {
    await this.store.ready
    const existing = this.find(id)
    if (existing === undefined) return
    this.disconnect(id)
    this.store.update((state) => {
      state.clients = state.clients.filter((profile) => profile.id !== id)
    })
    this.onChange()
  }

  /** True when the pooled entry still owns socket work the panel can see. */
  live(connection) {
    if (connection.dialling === true) return true
    if (connection.client === null) return false
    if (connection.status === 'connected' || connection.status === 'connecting' || connection.status === 'reconnecting') return true
    // While mqtt.js keeps retrying on its own a second dial would open a twin
    // session; an offline connection with retries switched off is dead and the
    // caller may replace it.
    return connection.status === 'offline' && Number(connection.profile.reconnectPeriod ?? 0) > 0
  }

  /** Close a pooled entry that no longer represents a live attempt. */
  release(connection) {
    connection.manual = true
    this.connections.delete(connection.profileId)
    try {
      connection.client?.end(true)
    } catch (error) {
      log(`cannot close the stale connection: ${oneLine(error)}`)
    }
  }

  /** Open (or reuse) the MQTT connection for one profile. */
  async connect(id) {
    const profile = this.find(id)
    if (profile === undefined) throw new Error('unknown connection')
    const current = this.connections.get(id)
    // A session that is connected, reconnecting or still being dialled is
    // reused. Calling connect() twice used to open two real MQTT sessions — the
    // second replaced the map entry, so disconnect() could only close one of
    // them and the other kept reconnecting invisibly forever.
    if (current !== undefined && this.live(current)) return this.view(profile)
    // The pooled entry outlived its socket (a refused connect, a dropped session
    // with retries off, a clean end). Rebuild it, otherwise connect would stay a
    // silent no-op until the profile is deleted and recreated.
    if (current !== undefined) {
      current.status = 'idle'
      this.release(current)
    }
    const connection = { profileId: id, profile, client: null, status: 'connecting', error: null, since: null, manual: false, dialling: true, subscriptions: new Set() }
    this.connections.set(id, connection)
    this.onChange()
    let mqtt
    try {
      mqtt = await loadMqtt()
    } catch (error) {
      if (this.connections.get(id) === connection) this.connections.delete(id)
      connection.status = 'error'
      connection.error = oneLine(error)
      this.onChange()
      throw error
    }
    // The user pressed disconnect while the mqtt package was still loading.
    if (this.connections.get(id) !== connection) return this.view(profile)
    let client
    try {
      client = mqtt.connect(profile.url, connectOptions(profile))
    } catch (error) {
      if (this.connections.get(id) === connection) this.connections.delete(id)
      connection.status = 'error'
      connection.error = oneLine(error)
      this.onChange()
      throw new Error(`cannot start the MQTT client: ${oneLine(error)}`)
    }
    connection.dialling = false
    connection.client = client
    // Disconnecting while connect() was in flight must not leave a live socket
    // behind that the panel can no longer reach.
    if (connection.manual || this.connections.get(id) !== connection) {
      try {
        client.end(true)
      } catch (error) {
        log(`cannot close the abandoned connection: ${oneLine(error)}`)
      }
      return this.view(profile)
    }
    client.on('connect', (packet) => {
      connection.status = 'connected'
      connection.error = null
      connection.since = Date.now()
      this.onChange()
      this.subscribeAll(connection)
      if (profile.protocolVersion === 5 && packet?.properties !== undefined) {
        /* server properties are informational only */
      }
    })
    client.on('reconnect', () => {
      connection.status = 'reconnecting'
      this.onChange()
    })
    client.on('offline', () => {
      connection.status = 'offline'
      this.onChange()
    })
    client.on('close', () => {
      if (connection.manual) return
      if (connection.status === 'connected') connection.status = 'disconnected'
      this.onChange()
    })
    client.on('end', () => {
      connection.status = 'idle'
      // The socket is gone for good; drop the entry so the next connect starts a
      // fresh session instead of reusing a dead one.
      if (this.connections.get(connection.profileId) === connection) this.connections.delete(connection.profileId)
      this.onChange()
    })
    client.on('error', (error) => {
      connection.status = 'error'
      connection.error = oneLine(error)
      this.onChange()
    })
    // Read the profile through the connection: renaming or re-saving a live
    // connection replaces connection.profile, and the log must follow the new
    // name instead of the one captured when the socket was opened.
    client.on('message', (topic, payload, packet) => this.receive(connection.profile, topic, payload, packet))
    return this.view(profile)
  }

  /** Subscribe to every topic the profile remembers, after a (re)connect. */
  subscribeAll(connection) {
    const client = connection.client
    if (client === null) return
    for (const subscription of connection.profile.subscriptions ?? []) {
      client.subscribe(subscription.topic, { qos: subscription.qos }, (error) => {
        if (error) log(`cannot subscribe ${subscription.topic}: ${oneLine(error)}`)
        else connection.subscriptions.add(subscription.topic)
      })
    }
  }

  receive(profile, topic, payload, packet) {
    const entry = this.messages.push({
      kind: 'client',
      direction: 'in',
      clientId: profile.id,
      name: profile.name,
      topic: String(topic),
      qos: Number(packet?.qos ?? 0),
      retain: Boolean(packet?.retain),
      ...describePayload(payload),
    })
    this.onMessage(entry)
  }

  /** Add a subscription and remember it on the profile. */
  async subscribe(input) {
    const id = String(input?.id ?? '')
    const topic = String(input?.topic ?? '').trim()
    // An out-of-range QoS is a caller mistake: clamping it silently would make a
    // tool report "subscribed with qos 9" while the broker granted at most 2.
    const qos = normalizeQos(input?.qos ?? 0)
    if (!topic) throw new Error('a topic filter is required')
    const profile = this.find(id)
    if (profile === undefined) throw new Error('unknown connection')
    const connection = this.connections.get(id)
    const client = connection?.client ?? null
    let granted = null
    if (client !== null && connection.status === 'connected') {
      granted = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('subscribe timed out')), PUBLISH_TIMEOUT_MS)
        client.subscribe(topic, { qos }, (error, result) => {
          clearTimeout(timer)
          if (error) reject(error)
          else resolve(result ?? null)
        })
      })
    }
    const existing = profile.subscriptions.find((subscription) => subscription.topic === topic)
    if (existing !== undefined) existing.qos = qos
    else profile.subscriptions.push({ topic, qos })
    this.store.update(() => {})
    this.onChange()
    return { topic, qos, granted }
  }

  /** Drop a subscription and forget it on the profile. */
  async unsubscribe(input) {
    const id = String(input?.id ?? '')
    const topic = String(input?.topic ?? '').trim()
    if (!topic) throw new Error('a topic filter is required')
    const profile = this.find(id)
    if (profile === undefined) throw new Error('unknown connection')
    const connection = this.connections.get(id)
    const client = connection?.client ?? null
    if (client !== null && connection.status === 'connected') {
      await new Promise((resolve) => {
        let timer = null
        const done = () => {
          if (timer !== null) clearTimeout(timer)
          resolve()
        }
        client.unsubscribe(topic, done)
        timer = setTimeout(done, PUBLISH_TIMEOUT_MS)
        timer.unref?.()
      })
      connection.subscriptions.delete(topic)
    }
    profile.subscriptions = profile.subscriptions.filter((subscription) => subscription.topic !== topic)
    this.store.update(() => {})
    this.onChange()
  }

  /** Publish through one connection. */
  async publish(input) {
    const id = String(input?.id ?? '')
    const topic = String(input?.topic ?? '').trim()
    if (!topic) throw new Error('a topic is required')
    const profile = this.find(id)
    if (profile === undefined) throw new Error('unknown connection')
    const connection = this.connections.get(id)
    if (connection === undefined || connection.client === null || connection.status !== 'connected') {
      throw new Error('connect first: this connection is not online')
    }
    const qos = normalizeQos(input?.qos ?? 0)
    const retain = Boolean(input?.retain)
    const payload = payloadBuffer(input?.payload, input?.encoding)
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('publish timed out')), PUBLISH_TIMEOUT_MS)
      connection.client.publish(topic, payload, { qos, retain }, (error) => {
        clearTimeout(timer)
        if (error) reject(error)
        else resolve()
      })
    })
    const entry = this.messages.push({
      kind: 'client',
      direction: 'out',
      clientId: profile.id,
      name: profile.name,
      topic,
      qos,
      retain,
      ...describePayload(payload),
    })
    // The outbound copy must reach SSE listeners exactly like an inbound one;
    // otherwise anything published from the panel or by an agent stays invisible
    // in the live log until the next full state reload.
    this.onMessage(entry)
    return { topic, qos, retain, bytes: payload.byteLength }
  }

  /** Close one connection; the profile stays configured. */
  disconnect(id) {
    const connection = this.connections.get(id)
    if (connection === undefined) return
    connection.status = 'idle'
    this.release(connection)
    this.onChange()
  }

  dispose() {
    for (const id of [...this.connections.keys()]) this.disconnect(id)
  }
}
