/** Embedded MQTT broker (server) built on aedes, with a small management surface. */
import net from 'node:net'
import { normalizeBroker, normalizeQos, normalizeRetained, MAX_STORED_RETAINED } from './store.js'
import { describePayload, oneLine } from './log.js'

const START_TIMEOUT_MS = 10000
const CLOSE_TIMEOUT_MS = 3000

function log(...args) {
  try {
    console.error('[dsh-mqtt-tools]', ...args)
  } catch {
    /* ignore */
  }
}

/** `::ffff:127.0.0.1` is just 127.0.0.1 to a person reading the event log. */
function formatRemote(address) {
  if (typeof address !== 'string') return ''
  return address.startsWith('::ffff:') ? address.slice(7) : address
}

/**
 * Turn the terse failures mqtt-packet produces into something a person can act
 * on: `unacceptable protocol version` is almost always a client configured for
 * TLS or WebSocket pointed at this plain TCP port, not an actual version clash.
 */
function explainBrokerError(error) {
  const text = oneLine(error)
  if (text.includes('unacceptable protocol version')) {
    // sniffFirstPacket already logs what the bytes actually were; this is the
    // generic line mqtt-packet produces for anything that is not 3.1/3.1.1.
    return `${text} — the CONNECT did not use MQTT 3.1 or 3.1.1 (see the line above for what the client actually sent)`
  }
  return text
}

let aedesModule = null
let aedesLoading = null
let aedesError = null

/** Load aedes lazily so a missing dependency never prevents the plugin from mounting. */
async function loadAedes() {
  if (aedesModule !== null) return aedesModule
  if (aedesLoading === null) {
    aedesLoading = import('aedes')
      .then((mod) => {
        const Aedes = mod?.Aedes ?? mod?.default?.Aedes
        if (typeof Aedes?.createBroker !== 'function') throw new Error('aedes package has no Aedes.createBroker()')
        aedesModule = Aedes
        aedesError = null
        return Aedes
      })
      .catch((error) => {
        aedesLoading = null
        aedesError = oneLine(error)
        throw new Error(`the aedes package is not available: ${oneLine(error)}`)
      })
  }
  return aedesLoading
}

function delay(ms) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms)
    timer.unref?.()
  })
}

/**
 * The first bytes of a connection already carry the whole diagnosis: TLS and
 * WebSocket clients never send an MQTT CONNECT, and an MQTT 5 client sends a
 * CONNECT this broker is going to refuse because aedes speaks 3.1/3.1.1 only.
 * Returns null for a healthy CONNECT.
 */
function explainFirstPacket(chunk) {
  if (!Buffer.isBuffer(chunk) || chunk.length === 0) return null
  const first = chunk[0]
  if (first === 0x16) {
    return 'TLS/SSL handshake on a plain TCP listener (first byte 0x16): turn TLS off in the client, or use an MQTT-over-TLS listener (usually port 8883)'
  }
  if (first === 0x10) {
    let index = 1
    let remaining = 0
    let multiplier = 1
    while (index < chunk.length) {
      const byte = chunk[index]
      index += 1
      remaining += (byte & 0x7f) * multiplier
      if ((byte & 0x80) === 0) break
      multiplier *= 128
    }
    if (remaining === 0 || index + 2 > chunk.length) return null
    const nameLength = chunk.readUInt16BE(index)
    index += 2
    if (nameLength === 0 || index + nameLength >= chunk.length) return null
    const protocolName = chunk.subarray(index, index + nameLength).toString('latin1')
    index += nameLength
    const level = chunk[index]
    if (level === 5) {
      return `MQTT 5.0 CONNECT ("${protocolName}") from a client: this broker speaks MQTT 3.1 and 3.1.1 only, so set the client protocol version to 3.1.1`
    }
    if (level !== 3 && level !== 4) {
      return `CONNECT with protocol level ${level}: expected MQTT 3.1 (level 3) or 3.1.1 (level 4)`
    }
    return null
  }
  const head = chunk.subarray(0, 5).toString('latin1')
  if (/^(GET |POST|PUT |HEAD|OPTI|CONN)/.test(head)) {
    return 'HTTP request on the plain MQTT/TCP listener (a WebSocket client): use mqtt:// (tcp://) with no path here, or point the client at a ws:// listener'
  }
  return `not an MQTT packet at all (first byte 0x${first.toString(16).padStart(2, '0')})`
}

/** `3.1` → 3, `3.1.1` → 4, `5.0` → 5; anything else → null. */
function protocolLevel(label) {
  if (label === '3.1') return 3
  if (label === '3.1.1') return 4
  if (label === '5.0') return 5
  return null
}

function protocolLabel(level) {
  if (level === 3) return '3.1'
  if (level === 4) return '3.1.1'
  if (level === 5) return '5.0'
  return `level ${level}`
}

function allowedLevels(protocols) {
  const levels = []
  for (const label of protocols ?? []) {
    const level = protocolLevel(label)
    if (level !== null) levels.push(level)
  }
  return levels.length > 0 ? levels : [3, 4, 5]
}

/**
 * Read the MQTT CONNECT header far enough to learn which protocol version the
 * client speaks. Returns null while more bytes are needed, `{ fatal, hint }`
 * for something that will never become a CONNECT, or `{ level, protocolName }`.
 */
function classifyConnect(buffer) {
  if (buffer.length === 0) return null
  if (buffer[0] !== 0x10) {
    const hint = explainFirstPacket(buffer)
    return { fatal: true, hint: hint ?? 'not an MQTT CONNECT' }
  }
  let index = 1
  let remaining = 0
  let multiplier = 1
  for (;;) {
    if (index >= buffer.length) return null
    const byte = buffer[index]
    index += 1
    remaining += (byte & 0x7f) * multiplier
    if ((byte & 0x80) === 0) break
    multiplier *= 128
    if (multiplier > 128 ** 3) return { fatal: true, hint: 'malformed MQTT remaining length' }
  }
  if (remaining === 0 || remaining > 1024 * 1024) return { fatal: true, hint: 'malformed MQTT CONNECT length' }
  if (index + 2 > buffer.length) return null
  const nameLength = buffer.readUInt16BE(index)
  if (nameLength === 0 || nameLength > 32 || index + 2 + nameLength + 1 > buffer.length) return null
  const protocolName = buffer.subarray(index + 2, index + 2 + nameLength).toString('latin1')
  const level = buffer[index + 2 + nameLength]
  return { level, protocolName }
}

let mqtt5Module = null
let mqtt5Loading = null
let mqttPacketModule = null
let mqttPacketLoading = null

/** Load the MQTT 5 bridge on demand so a missing dependency never blocks mounting. */
async function loadMqtt5() {
  if (mqtt5Module !== null) return mqtt5Module
  if (mqtt5Loading === null) {
    mqtt5Loading = import('./mqtt5.js')
      .then((mod) => {
        mqtt5Module = mod
        return mod
      })
      .catch((error) => {
        mqtt5Loading = null
        throw new Error(`the MQTT 5 bridge is not available: ${oneLine(error)}`)
      })
  }
  return mqtt5Loading
}

async function loadMqttPacket() {
  if (mqttPacketModule !== null) return mqttPacketModule
  if (mqttPacketLoading === null) {
    mqttPacketLoading = import('mqtt-packet')
      .then((mod) => {
        mqttPacketModule = mod?.default ?? mod
        return mqttPacketModule
      })
      .catch((error) => {
        mqttPacketLoading = null
        throw new Error(`the mqtt-packet package is not available: ${oneLine(error)}`)
      })
  }
  return mqttPacketLoading
}

/** The embedded broker: settings, lifecycle, connected clients, publish. */
export class BrokerManager {
  constructor({ store, messages, events, onChange }) {
    this.store = store
    this.messages = messages
    this.events = events
    this.onChange = onChange ?? (() => {})
    this.onMessage = () => {}
    this.onEvent = () => {}
    this.state = { status: 'stopped', error: null, startedAt: null, boundPort: null }
    this.broker = null
    this.server = null
    this.sockets = new Set()
    /** clientId -> { id, connectedAt, remote, clean, version, subscriptions:Set } */
    this.clients = new Map()
    this.stats = { connections: 0, published: 0, system: 0 }
    this.restartHint = false
    /** Serialises start/stop so two transitions can never interleave. */
    this.transition = Promise.resolve()
    /** Topics this broker currently retains; kept in sync for the retained cap. */
    this.retainedTopics = new Set()
    /** topic -> { topic, payload(base64), qos }: replayed after a restart. */
    this.retained = new Map()
    /** Suppresses log rows and persistence while stored retained messages replay. */
    this.restoring = false
  }

  /** Persist the retained set; bounded by the model's stored-retained cap. */
  persistRetained() {
    const entries = normalizeRetained(Array.from(this.retained.values()).slice(-MAX_STORED_RETAINED))
    this.store.update((state) => {
      state.retained = entries
    })
  }

  /** Resource limits that protect the host from one misbehaving client. */
  limits() {
    const broker = this.store.data.broker
    return {
      maxConnections: broker.maxConnections,
      connectTimeoutMs: broker.connectTimeoutMs,
      maxPayloadBytes: broker.maxPayloadBytes,
      maxRetainedMessages: broker.maxRetainedMessages,
    }
  }

  /**
   * Refuse a publish that would exceed the payload or retained-topic budget.
   * Returns an Error (used by `authorizePublish` and the server-side publish),
   * or null when the packet is fine.
   */
  checkPublishLimits(packet) {
    const limits = this.limits()
    const payloadLength = Buffer.isBuffer(packet?.payload) ? packet.payload.length : 0
    if (limits.maxPayloadBytes > 0 && payloadLength > limits.maxPayloadBytes) {
      return new Error(
        `the payload of ${payloadLength} bytes exceeds this broker's ${limits.maxPayloadBytes} byte limit (maxPayloadBytes)`,
      )
    }
    if (packet?.retain === true && limits.maxRetainedMessages > 0) {
      const topic = String(packet.topic ?? '')
      const clears = payloadLength === 0
      if (!clears && !this.retainedTopics.has(topic) && this.retainedTopics.size >= limits.maxRetainedMessages) {
        return new Error(
          `this broker already retains ${limits.maxRetainedMessages} topics; clear one with an empty retained message before adding another (maxRetainedMessages)`,
        )
      }
    }
    return null
  }

  /** Try the import in the background so "not loaded yet" is not mistaken for missing. */
  warm() {
    loadAedes()
      .then(() => this.onChange())
      .catch(() => this.onChange())
  }

  settings() {
    const broker = this.store.data.broker
    return {
      host: broker.host,
      port: broker.port,
      autoStart: broker.autoStart,
      allowAnonymous: broker.allowAnonymous,
      protocols: [...broker.protocols],
      users: broker.users.map((user) => ({ username: user.username, hasPassword: user.password.length > 0 })),
      maxConnections: broker.maxConnections,
      connectTimeoutMs: broker.connectTimeoutMs,
      maxPayloadBytes: broker.maxPayloadBytes,
      maxRetainedMessages: broker.maxRetainedMessages,
    }
  }

  view() {
    return {
      ...this.settings(),
      status: this.state.status,
      error: this.state.error,
      startedAt: this.state.startedAt,
      boundPort: this.state.boundPort,
      boundHost: this.state.boundHost ?? null,
      restartHint: this.restartHint,
      clientCount: this.clients.size,
      clients: Array.from(this.clients.values(), (client) => ({
        id: client.id,
        connectedAt: client.connectedAt,
        remote: client.remote,
        clean: client.clean,
        version: client.version,
        subscriptions: [...client.subscriptions],
      })),
      stats: { ...this.stats },
      retainedCount: this.retainedTopics.size,
      retainedTopics: Array.from(this.retainedTopics).slice(0, 50),
      library: { aedes: aedesModule !== null, error: aedesError },
    }
  }

  /** Persist broker settings; an empty password keeps the stored one per user. */
  saveSettings(input) {
    if (input === null || typeof input !== 'object') throw new Error('broker settings are required')
    const current = this.store.data.broker
    const incoming = Array.isArray(input.users) ? input.users : current.users
    const users = []
    const seen = new Set()
    for (const raw of incoming) {
      const username = String(raw?.username ?? '').trim()
      if (!username || seen.has(username)) continue
      seen.add(username)
      const previous = current.users.find((user) => user.username === username)
      const password = typeof raw?.password === 'string' && raw.password !== '' ? raw.password : previous?.password ?? ''
      users.push({ username, password })
    }
    const candidate = normalizeBroker({ ...current, ...input, users })
    this.store.update((state) => {
      state.broker = candidate
    })
    if (this.state.status === 'running') this.restartHint = true
    this.onChange()
    return this.view()
  }

  /** Push one broker lifecycle entry into the event log and stream it. */
  event(action, extra = {}) {
    const entry = this.events.push({ kind: 'broker', action, ...extra })
    this.onEvent(entry)
  }

  fail(message) {
    this.state = { status: 'error', error: message, startedAt: null, boundPort: null }
    this.event('error', { text: message })
    this.onChange()
  }

  /** Start the TCP listener and the aedes engine. */
  async start() {
    return this.queue(() => this.startNow())
  }

  /**
   * Run one lifecycle transition after the previous one settled, whatever its
   * outcome. Without this a quick stop→start (or a double click in the panel)
   * could bind the port twice or leave a listener aedes no longer knows about.
   */
  queue(task) {
    const run = this.transition.then(task, task)
    this.transition = run.then(
      () => {},
      () => {},
    )
    return run
  }

  async startNow() {
    if (this.server !== null) return this.view()
    const settings = this.store.data.broker
    this.state = { status: 'starting', error: null, startedAt: null, boundPort: null }
    this.onChange()
    let aedes
    try {
      const Aedes = await loadAedes()
      aedes = await Aedes.createBroker({ heartbeatInterval: 30000 })
    } catch (error) {
      this.fail(oneLine(error))
      throw error
    }
    this.installAuthenticate(aedes, settings)
    const server = net.createServer((socket) => {
      this.accept(socket, aedes)
    })
    await new Promise((resolve, reject) => {
      let settled = false
      let timer = null
      server.once('listening', () => {
        if (settled) return
        settled = true
        if (timer !== null) clearTimeout(timer)
        resolve()
      })
      server.on('error', (error) => {
        if (!settled) {
          settled = true
          if (timer !== null) clearTimeout(timer)
          reject(error)
          return
        }
        log(`broker server error: ${oneLine(error)}`)
      })
      timer = setTimeout(() => {
        if (settled) return
        settled = true
        reject(new Error('the broker did not start listening in time'))
      }, START_TIMEOUT_MS)
      timer.unref?.()
      try {
        server.listen(settings.port, settings.host)
      } catch (error) {
        settled = true
        clearTimeout(timer)
        reject(error)
      }
    }).catch((error) => {
      try {
        server.close()
      } catch {
        /* ignore */
      }
      try {
        aedes.close()
      } catch {
        /* ignore */
      }
      this.fail(`cannot listen on ${settings.host}:${settings.port}: ${oneLine(error)}`)
      throw new Error(`cannot start the broker: ${oneLine(error)}`)
    })
    this.broker = aedes
    this.server = server
    this.attach(aedes)
    const address = server.address()
    this.state = {
      status: 'running',
      error: null,
      startedAt: Date.now(),
      boundPort: typeof address === 'object' && address !== null ? address.port : settings.port,
      // Remember where the listener really is: a saved host change while running
      // updates `settings.host`, so reading it back would describe a listener that
      // does not exist yet.
      boundHost: typeof address === 'object' && address !== null ? address.address : settings.host,
    }
    this.restartHint = false
    this.retainedTopics.clear()
    this.retained = new Map()
    await this.restoreRetained()
    this.event('start', { text: `listening on ${settings.host}:${this.state.boundPort}` })
    this.onChange()
    return this.view()
  }

  /**
   * Replay the retained set the previous run saved. Aedes keeps retained
   * messages in memory only, so without this a broker restart (or a settings
   * change that requires one) silently forgets every last-known value.
   */
  async restoreRetained() {
    const broker = this.broker
    const stored = normalizeRetained(this.store.data.retained)
    if (broker === null || stored.length === 0) return
    this.restoring = true
    try {
      for (const entry of stored) {
        this.retained.set(entry.topic, entry)
        this.retainedTopics.add(entry.topic)
        await new Promise((resolve, reject) => {
          broker.publish(
            { cmd: 'publish', topic: entry.topic, payload: Buffer.from(entry.payload, 'base64'), qos: entry.qos, retain: true, dup: false },
            (error) => (error ? reject(error) : resolve()),
          )
        }).catch((error) => log(`cannot replay retained ${entry.topic}: ${oneLine(error)}`))
      }
    } finally {
      this.restoring = false
    }
    log(`restored ${this.retained.size} retained message(s)`)
  }

  /**
   * Anonymous or user/password authentication depending on the settings.
   * Anonymous mode is genuinely open, the way mosquitto's `allow_anonymous true`
   * is: a client that still has a username/password filled in from somewhere
   * else connects too, instead of being rejected for credentials nobody asked
   * for. Users only gate access when anonymous access is switched off.
   */
  installAuthenticate(aedes, settings) {
    aedes.authenticate = (client, username, password, done) => {
      try {
        const name = typeof username === 'string' ? username : ''
        const secret = Buffer.isBuffer(password) ? password.toString('utf8') : typeof password === 'string' ? password : ''
        if (settings.allowAnonymous) return done(null, true)
        const match = settings.users.find((user) => user.username === name && user.password === secret)
        if (match !== undefined) return done(null, true)
        const error = new Error(
          name === ''
            ? 'anonymous access is off and no username was sent (add a user in the Broker tab, or allow anonymous clients)'
            : `bad username or password for "${name}"`,
        )
        error.returnCode = 4
        return done(error, false)
      } catch (failure) {
        const error = new Error(`authentication failed: ${oneLine(failure)}`)
        error.returnCode = 3
        return done(error, false)
      }
    }
    // The payload/retained budgets apply to client publishes (and wills, which
    // aedes authorises through the same hook); aedes disconnects the client
    // when the callback receives an error.
    aedes.authorizePublish = (client, packet, done) => {
      let failure = null
      try {
        failure = this.checkPublishLimits(packet)
      } catch (error) {
        failure = error
      }
      done(failure)
    }
  }

  /**
   * Decide how to hand one incoming TCP connection to aedes.
   *
   * The protocol version has to be known before aedes sees the CONNECT, because
   * aedes refuses level 5 outright: 3.1/3.1.1 sockets go to aedes untouched, an
   * accepted 5.0 socket goes through the translation bridge, and a disabled
   * version gets a proper CONNACK refusal instead of a silent disconnect.
   */
  accept(socket, aedes) {
    const remote = formatRemote(socket.remoteAddress) || 'unknown'
    const limits = this.limits()
    if (limits.maxConnections > 0 && this.sockets.size >= limits.maxConnections) {
      this.event('error', {
        clientId: null,
        text: `refused the connection from ${remote}: the broker already holds ${this.sockets.size} connections (limit ${limits.maxConnections})`,
      })
      socket.destroy()
      return
    }
    this.sockets.add(socket)
    let timer = null
    const forget = () => {
      if (timer !== null) {
        clearTimeout(timer)
        timer = null
      }
      this.sockets.delete(socket)
    }
    socket.on('close', forget)
    socket.on('error', forget)
    let buffered = Buffer.alloc(0)
    let decided = false
    const settle = () => {
      decided = true
      if (timer !== null) {
        clearTimeout(timer)
        timer = null
      }
    }
    // A socket that never sends a CONNECT would otherwise sit here forever.
    timer = setTimeout(() => {
      if (decided) return
      settle()
      socket.pause()
      socket.removeListener('data', onData)
      this.event('error', {
        clientId: null,
        text: `closed the connection from ${remote}: no MQTT CONNECT within ${Math.round(limits.connectTimeoutMs / 1000)} s`,
      })
      socket.destroy()
    }, limits.connectTimeoutMs)
    timer.unref?.()
    const onData = (chunk) => {
      if (decided) return
      buffered = Buffer.concat([buffered, chunk])
      let decision = null
      try {
        decision = classifyConnect(buffered)
      } catch (error) {
        decision = { fatal: true, hint: `unreadable MQTT CONNECT: ${oneLine(error)}` }
      }
      if (decision === null) {
        if (buffered.length > 4096) {
          settle()
          socket.pause()
          socket.removeListener('data', onData)
          this.event('error', { clientId: null, text: `connection from ${remote}: ${explainFirstPacket(buffered) ?? 'not an MQTT CONNECT'}` })
          socket.destroy()
        }
        return
      }
      settle()
      socket.pause()
      socket.removeListener('data', onData)
      if (decision.fatal === true) {
        this.event('error', { clientId: null, text: `connection from ${remote}: ${decision.hint}` })
        socket.destroy()
        return
      }
      // `dispatch` is async; an unhandled rejection is fatal on Node 24, so a
      // failure to route one client must stay local to that connection.
      void this.dispatch(socket, aedes, buffered, decision, remote).catch((error) => {
        this.event('error', { clientId: null, text: `cannot route the connection from ${remote}: ${oneLine(error)}` })
        try {
          socket.destroy()
        } catch {
          /* the socket is already gone */
        }
      })
    }
    socket.on('data', onData)
  }

  /** Route one classified connection: bridge, plain aedes, or a refusal. */
  async dispatch(socket, aedes, buffered, decision, remote) {
    const allowed = allowedLevels(this.store.data.broker.protocols)
    if (!allowed.includes(decision.level)) {
      await this.rejectVersion(socket, decision.level, decision.protocolName, allowed, remote)
      return
    }
    if (decision.level === 5) {
      try {
        const { createMqtt5Bridge } = await loadMqtt5()
        const bridge = createMqtt5Bridge(socket, {
          remoteAddress: socket.remoteAddress ?? null,
          onError: (error) => this.event('error', { clientId: null, text: `MQTT 5.0 connection from ${remote}: ${oneLine(error)}` }),
          // MQTT 5 subscription options cannot travel over the 3.1.1 bridge; say so
          // in the broker log so a client asking for noLocal is not silently ignored.
          onUnsupported: (text) => this.event('warning', { clientId: null, text: `${text} [from ${remote}]` }),
        })
        bridge.ingest(buffered)
        aedes.handle(bridge.stream)
      } catch (error) {
        this.event('error', { clientId: null, text: `cannot accept MQTT 5.0 connection from ${remote}: ${oneLine(error)}` })
        try {
          const mqttPacket = await loadMqttPacket()
          socket.write(mqttPacket.generate({ cmd: 'connack', reasonCode: 0x88, sessionPresent: false, properties: {} }, { protocolVersion: 5 }))
        } catch {
          /* the client is about to be disconnected anyway */
        }
        socket.destroy()
      }
      return
    }
    try {
      socket.unshift(buffered)
    } catch {
      /* nothing was buffered? aedes can still read what follows */
    }
    aedes.handle(socket)
  }

  /** Answer a version the broker is configured not to accept, then close. */
  async rejectVersion(socket, level, protocolName, allowed, remote) {
    const label = protocolLabel(level)
    const accepted = allowed.map((allowedLevel) => protocolLabel(allowedLevel)).join(', ')
    this.event('error', {
      clientId: null,
      text: `refused an MQTT ${label} connection${protocolName ? ` ("${protocolName}")` : ''} from ${remote}: this broker accepts ${accepted} only (change "Protocol versions" in the Broker tab)`,
    })
    try {
      const mqttPacket = await loadMqttPacket()
      if (level === 5 || (typeof level === 'number' && level > 5)) {
        // MQTT 5 and anything newer speaks the v5 CONNACK framing.
        socket.write(
          mqttPacket.generate(
            { cmd: 'connack', reasonCode: 0x84, sessionPresent: false, properties: {} },
            { protocolVersion: 5 },
          ),
        )
      } else {
        // MQTT 3.x, and unknown lower levels: MQTT-3.1.2-2 requires a CONNACK
        // with return code 1 (unacceptable protocol version) before the close,
        // otherwise the client sees a bare TCP reset.
        socket.write(
          mqttPacket.generate(
            { cmd: 'connack', returnCode: 1, sessionPresent: false },
            { protocolVersion: 4 },
          ),
        )
      }
    } catch {
      /* a client that cannot be told is simply disconnected */
    }
    socket.end()
  }

  /** Subscribe to the broker's own lifecycle events. */
  attach(aedes) {
    aedes.on('client', (client) => {
      // A bridged connection looks like 3.1.1 to aedes but is a real MQTT 5
      // client, so the version shown to the user comes from the bridge marker.
      const version = client.conn?.mqtt5 === true ? 'MQTT 5.0' : client.version === 3 ? 'MQTT 3.1' : 'MQTT 3.1.1'
      this.clients.set(client.id, {
        id: client.id,
        connectedAt: Date.now(),
        remote: client.conn?.remoteAddress ?? null,
        clean: client.clean !== false,
        version,
        subscriptions: new Set(),
      })
      this.stats.connections += 1
      const remote = client.conn?.remoteAddress ?? ''
      this.event('connect', {
        clientId: client.id,
        text: `${remote === '' ? '' : `from ${formatRemote(remote)} `}(${version}${client.clean === false ? ', persistent session' : ''})`,
      })
      this.onChange()
    })
    aedes.on('clientDisconnect', (client) => {
      this.clients.delete(client.id)
      this.event('disconnect', { clientId: client.id, text: 'client disconnected' })
      this.onChange()
    })
    aedes.on('subscribe', (subscriptions, client) => {
      const record = this.clients.get(client?.id)
      for (const subscription of subscriptions ?? []) record?.subscriptions.add(subscription.topic)
      this.event('subscribe', {
        clientId: client?.id ?? null,
        text: (subscriptions ?? []).map((subscription) => subscription.topic).join(', '),
      })
      this.onChange()
    })
    aedes.on('unsubscribe', (topics, client) => {
      const record = this.clients.get(client?.id)
      for (const topic of topics ?? []) record?.subscriptions.delete(topic)
      this.event('unsubscribe', { clientId: client?.id ?? null, text: (topics ?? []).join(', ') })
      this.onChange()
    })
    aedes.on('publish', (packet, client) => {
      if (typeof packet?.topic !== 'string') return
      // Replaying stored retained messages must not duplicate log rows or write
      // the same set back to disk.
      if (this.restoring) return
      if (packet.retain === true) {
        const clears = !packet.payload || packet.payload.length === 0
        if (clears) {
          this.retainedTopics.delete(packet.topic)
          this.retained.delete(packet.topic)
        } else {
          this.retainedTopics.add(packet.topic)
          this.retained.set(packet.topic, {
            topic: packet.topic,
            payload: Buffer.from(packet.payload).toString('base64'),
            qos: Number(packet.qos ?? 0),
          })
        }
        this.persistRetained()
      }
      if (packet.topic.startsWith('$SYS/')) {
        this.stats.system += 1
        if (!this.store.data.log.showSystem) return
      }
      this.stats.published += 1
      const entry = this.messages.push({
        kind: 'broker',
        direction: client === null || client === undefined ? 'out' : 'in',
        clientId: client?.id ?? null,
        name: client?.id ?? 'broker',
        topic: packet.topic,
        qos: Number(packet.qos ?? 0),
        retain: Boolean(packet.retain),
        ...describePayload(packet.payload),
      })
      this.onMessage(entry)
    })
    aedes.on('clientError', (client, error) => {
      this.event('error', { clientId: client?.id ?? null, text: explainBrokerError(error) })
    })
    aedes.on('connectionError', (client, error) => {
      this.event('error', { clientId: client?.id ?? null, text: explainBrokerError(error) })
    })
  }

  /** Publish a message into the broker as the server itself. */
  async publish(input) {
    if (this.broker === null || this.state.status !== 'running') throw new Error('start the broker first')
    const topic = String(input?.topic ?? '').trim()
    if (!topic) throw new Error('a topic is required')
    const qos = normalizeQos(input?.qos ?? 0)
    const retain = Boolean(input?.retain)
    const text = typeof input?.payload === 'string' ? input.payload : input?.payload === undefined || input?.payload === null ? '' : String(input.payload)
    const payload = Buffer.from(text, input?.encoding === 'base64' ? 'base64' : 'utf8')
    const limit = this.checkPublishLimits({ topic, payload, retain })
    if (limit !== null) throw limit
    const packet = { cmd: 'publish', topic, payload, qos, retain, dup: false }
    await new Promise((resolve, reject) => {
      this.broker.publish(packet, (error) => (error ? reject(error) : resolve()))
    })
    return { topic, qos, retain, bytes: payload.byteLength }
  }

  /**
   * Drop every retained message: an empty retained publish is the MQTT way to
   * clear a topic, and the broker does it for all of them at once so a user
   * does not have to remember which topics hold a value.
   */
  async clearRetained() {
    if (this.broker === null || this.state.status !== 'running') throw new Error('start the broker first')
    const topics = Array.from(this.retainedTopics)
    for (const topic of topics) {
      await new Promise((resolve, reject) => {
        this.broker.publish(
          { cmd: 'publish', topic, payload: Buffer.alloc(0), qos: 0, retain: true, dup: false },
          (error) => (error ? reject(error) : resolve()),
        )
      }).catch((error) => log(`cannot clear retained ${topic}: ${oneLine(error)}`))
    }
    this.retainedTopics.clear()
    this.retained.clear()
    this.persistRetained()
    this.event('retained-clear', { text: `cleared ${topics.length} retained topic(s)` })
    this.onChange()
    return { cleared: topics.length, topics }
  }

  /** Stop the listener and close the engine. */
  async stop() {
    return this.queue(() => this.stopNow())
  }

  async stopNow() {
    const server = this.server
    const broker = this.broker
    this.server = null
    this.broker = null
    if (server === null && broker === null) {
      if (this.state.status !== 'stopped') {
        this.state = { status: 'stopped', error: null, startedAt: null, boundPort: null }
        this.onChange()
      }
      return this.view()
    }
    this.state = { status: 'stopping', error: null, startedAt: null, boundPort: null }
    this.onChange()
    for (const socket of this.sockets) {
      try {
        socket.destroy()
      } catch {
        /* ignore */
      }
    }
    this.sockets.clear()
    if (server !== null) {
      await Promise.race([
        new Promise((resolve) => {
          try {
            server.close(() => resolve())
          } catch {
            resolve()
          }
        }),
        delay(CLOSE_TIMEOUT_MS),
      ])
    }
    if (broker !== null) {
      await Promise.race([
        new Promise((resolve) => {
          try {
            broker.close(() => resolve())
          } catch {
            resolve()
          }
        }),
        delay(CLOSE_TIMEOUT_MS),
      ])
    }
    this.clients.clear()
    this.retainedTopics.clear()
    this.retained = new Map()
    this.state = { status: 'stopped', error: null, startedAt: null, boundPort: null }
    this.restartHint = false
    this.event('stop', { text: 'broker stopped' })
    this.onChange()
    return this.view()
  }

  /** Stop and wait for the port to be released (disposal path). */
  async dispose() {
    try {
      await this.stop()
    } catch (error) {
      log(`cannot stop the broker: ${oneLine(error)}`)
    }
  }
}
