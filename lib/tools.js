/**
 * Agent tools: the DSH-native counterpart of the MQTT panel. Every tool talks
 * to the same engine the web GUI uses, so a connection or broker configured in
 * the GUI is immediately operable by any agent, and vice versa.
 */
import { oneLine } from './log.js'

const DEFAULT_WAIT_MS = 10000
const MIN_WAIT_MS = 1000
const MAX_WAIT_MS = 30000
const MAX_LIMIT = 200
const MAX_COUNT = 50
const DEFAULT_LIMIT = 20
const POLL_MS = 120

/** One text content block (the only render shape these tools emit). */
function text(value) {
  return [{ type: 'text', text: value }]
}

/**
 * Tool outputs are validated as lossless JSON before a model sees them, and
 * undefined/NaN do not survive that trip. Everything a tool returns goes
 * through this, so one odd library value cannot turn a working call into
 * "returned invalid output".
 */
function losslessJson(value) {
  if (value === undefined || value === null) return null
  if (typeof value === 'number') return Number.isFinite(value) ? value : null
  if (typeof value === 'bigint') return String(value)
  if (Array.isArray(value)) return value.map(losslessJson)
  if (typeof value === 'object') {
    if (typeof value.toJSON === 'function') return losslessJson(value.toJSON())
    const next = {}
    for (const [key, item] of Object.entries(value)) {
      if (item === undefined) continue
      next[key] = losslessJson(item)
    }
    return next
  }
  return value
}

/* ------------------------------------------------------------------ schema helpers */

const reqStr = (description) => ({ type: 'string', required: true, description })
const optStr = (description) => ({ type: 'string', description })
const optInt = (description) => ({ type: 'integer', description })
const optBool = (description) => ({ type: 'boolean', description })
const optEnum = (values, description) => ({ type: 'string', enum: values, description })

const oStr = () => ({ type: 'string', required: true })
const oInt = () => ({ type: 'integer', required: true })
const oBool = () => ({ type: 'boolean', required: true })
const oNullInt = () => ({ oneOf: [{ type: 'integer' }, { type: 'null' }], required: true })
const oNullStr = () => ({ oneOf: [{ type: 'string' }, { type: 'null' }], required: true })
const oArr = (items) => ({ type: 'array', items, required: true })
const oObj = (properties) => ({ type: 'object', additionalProperties: false, properties })

const SUBSCRIPTION_SCHEMA = oObj({ topic: oStr(), qos: oInt() })

const CONNECTION_SCHEMA = oObj({
  id: oStr(),
  name: oStr(),
  url: oStr(),
  status: oStr(),
  autoConnect: oBool(),
  hasPassword: oBool(),
  connectedAt: oNullInt(),
  error: oNullStr(),
  subscriptions: oArr(SUBSCRIPTION_SCHEMA),
})

const MESSAGE_SCHEMA = oObj({
  seq: oInt(),
  ts: oInt(),
  time: oStr(),
  kind: oStr(),
  direction: oStr(),
  connection: oNullStr(),
  clientId: oNullStr(),
  topic: oStr(),
  payload: oStr(),
  encoding: oStr(),
  bytes: oInt(),
  qos: oInt(),
  retain: oBool(),
})

const BROKER_SCHEMA = oObj({
  status: oStr(),
  host: oStr(),
  port: oInt(),
  boundPort: oNullInt(),
  allowAnonymous: oBool(),
  autoStart: oBool(),
  userCount: oInt(),
  clientCount: oInt(),
  restartHint: oBool(),
  maxConnections: oInt(),
  connectTimeoutMs: oInt(),
  maxPayloadBytes: oInt(),
  maxRetainedMessages: oInt(),
  retainedCount: oInt(),
  error: oNullStr(),
})

const USER_SCHEMA = oObj({ username: oStr(), hasPassword: oBool() })

const BROKER_CLIENT_SCHEMA = oObj({
  id: oStr(),
  connectedAt: oInt(),
  remote: oNullStr(),
  clean: oBool(),
  version: oNullInt(),
  subscriptions: oArr({ type: 'string', required: true }),
})

/* ------------------------------------------------------------------ text render helpers */

function clip(value, max) {
  const flat = String(value ?? '').replace(/\s+/g, ' ').trim()
  return flat.length > max ? flat.slice(0, max - 1) + '…' : flat
}

function renderConnections(rows) {
  if (rows.length === 0) return 'no connections configured'
  const lines = ['name | status | url | subscriptions | auto', '--- | --- | --- | --- | ---']
  for (const row of rows) {
    const subscriptions = row.subscriptions.length === 0 ? '-' : row.subscriptions.map((item) => `${item.topic}@${item.qos}`).join(',')
    lines.push([row.name, row.error === null ? row.status : `${row.status} (${clip(row.error, 60)})`, row.url, subscriptions, row.autoConnect ? 'yes' : 'no'].join(' | '))
  }
  return lines.join('\n')
}

function renderMessages(rows) {
  if (rows.length === 0) return 'no messages matched'
  const lines = ['seq | time | source | topic | payload', '--- | --- | --- | --- | ---']
  for (const row of rows) {
    lines.push([row.seq, row.time, `${row.direction}/${row.kind}`, row.topic, clip(row.payload, 120)].join(' | '))
  }
  return lines.join('\n')
}

function renderBroker(row) {
  const parts = [
    `status: ${row.status}${row.error === null ? '' : ` (${row.error})`}`,
    `listener: ${row.host}:${row.port}${row.boundPort === null ? '' : ` (bound ${row.boundPort})`}`,
    `anonymous: ${row.allowAnonymous ? 'yes' : 'no'}, users: ${row.userCount}, clients: ${row.clientCount}`,
    `limits: payload ≤ ${row.maxPayloadBytes} B, retained ≤ ${row.maxRetainedMessages === 0 ? 'unlimited' : row.maxRetainedMessages}, connections ≤ ${row.maxConnections === 0 ? 'unlimited' : row.maxConnections}, CONNECT timeout ${Math.round(row.connectTimeoutMs / 1000)} s`,
  ]
  if (row.restartHint) parts.push('note: settings changed while running; restart to apply')
  return parts.join('\n')
}

/* ------------------------------------------------------------------ resolution helpers */

function summarizeRows(list) {
  return list.map((profile) => `${profile.name} (${profile.id.slice(0, 8)}) ${profile.url}`).join('; ')
}

/** Resolve one configured connection by id or name (exact first, then a unique partial match). */
function resolveConnection(clients, selector) {
  const list = clients.list()
  if (list.length === 0) {
    throw new Error('no connection is configured: call mqtt_connect with a url, or add one in the MQTT panel')
  }
  const raw = typeof selector === 'string' ? selector.trim() : ''
  if (raw === '') {
    if (list.length === 1) return list[0]
    throw new Error('several connections are configured: pass connection (name or id). Available: ' + summarizeRows(list))
  }
  const lower = raw.toLowerCase()
  const exact = list.find((profile) => profile.id === raw) ?? list.find((profile) => profile.name.toLowerCase() === lower)
  if (exact !== undefined) return exact
  const partial = list.filter(
    (profile) => profile.name.toLowerCase().includes(lower) || profile.id.startsWith(raw) || profile.url.toLowerCase().includes(lower),
  )
  if (partial.length === 1) return partial[0]
  if (partial.length > 1) {
    throw new Error(`connection "${raw}" is ambiguous: pass the exact name. Candidates: ` + summarizeRows(partial))
  }
  throw new Error(`unknown connection "${raw}". Available: ` + summarizeRows(list))
}

/** Standard MQTT topic filter matching (`+` one level, `#` the rest). */
function topicMatches(filter, topic) {
  if (typeof filter !== 'string' || filter === '') return true
  const filterLevels = filter.split('/')
  const topicLevels = topic.split('/')
  for (let index = 0; index < filterLevels.length; index += 1) {
    if (filterLevels[index] === '#') return true
    if (index >= topicLevels.length) return false
    if (filterLevels[index] !== '+' && filterLevels[index] !== topicLevels[index]) return false
  }
  return filterLevels.length === topicLevels.length
}

function messageRow(entry) {
  return {
    seq: entry.seq,
    ts: entry.ts,
    time: new Date(entry.ts).toISOString().slice(11, 23),
    kind: entry.kind,
    direction: entry.direction,
    connection: entry.kind === 'client' ? entry.name : null,
    clientId: entry.clientId ?? null,
    topic: entry.topic,
    payload: entry.payload,
    encoding: entry.encoding,
    bytes: entry.bytes,
    qos: entry.qos,
    retain: entry.retain,
  }
}

/** Build the message filter shared by mqtt_messages and mqtt_wait. */
function messageFilter(clients, input) {
  const filter = { connectionId: null, source: 'any', direction: 'any', topic: '' }
  const source = typeof input.source === 'string' ? input.source : 'any'
  if (source === 'client' || source === 'broker') filter.source = source
  const direction = typeof input.direction === 'string' ? input.direction : 'any'
  if (direction === 'in' || direction === 'out') filter.direction = direction
  if (typeof input.topic === 'string' && input.topic.trim() !== '') filter.topic = input.topic.trim()
  if (typeof input.connection === 'string' && input.connection.trim() !== '') {
    const profile = resolveConnection(clients, input.connection)
    filter.connectionId = profile.id
  }
  return filter
}

function rowMatches(row, filter) {
  if (filter.source !== 'any' && row.kind !== filter.source) return false
  if (filter.direction !== 'any' && row.direction !== filter.direction) return false
  if (filter.connectionId !== null && (row.kind !== 'client' || row.clientId !== filter.connectionId)) return false
  return topicMatches(filter.topic, row.topic)
}

function connectionSummary(view) {
  return {
    id: view.id,
    name: view.name,
    url: view.url,
    status: view.status,
    autoConnect: Boolean(view.autoConnect),
    hasPassword: Boolean(view.hasPassword),
    connectedAt: view.connectedAt ?? null,
    error: view.error ?? null,
    subscriptions: (view.subscriptions ?? []).map((subscription) => ({ topic: String(subscription.topic), qos: Number(subscription.qos ?? 0) })),
  }
}

function brokerSummary(view) {
  return {
    status: view.status,
    host: view.host,
    port: view.port,
    boundPort: view.boundPort ?? null,
    allowAnonymous: Boolean(view.allowAnonymous),
    autoStart: Boolean(view.autoStart),
    userCount: Array.isArray(view.users) ? view.users.length : 0,
    clientCount: view.clientCount ?? 0,
    restartHint: Boolean(view.restartHint),
    maxConnections: Number(view.maxConnections ?? 0),
    connectTimeoutMs: Number(view.connectTimeoutMs ?? 0),
    maxPayloadBytes: Number(view.maxPayloadBytes ?? 0),
    maxRetainedMessages: Number(view.maxRetainedMessages ?? 0),
    retainedCount: Number(view.retainedCount ?? 0),
    error: view.error ?? null,
  }
}

function clampInt(value, min, max, fallback) {
  const number = Number(value)
  if (!Number.isFinite(number)) return fallback
  return Math.min(max, Math.max(min, Math.trunc(number)))
}

function clampTimeout(value) {
  return clampInt(value, MIN_WAIT_MS, MAX_WAIT_MS, DEFAULT_WAIT_MS)
}

function sleep(ms) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms)
    timer.unref?.()
  })
}

function deriveName(url) {
  const stripped = url.replace(/^[a-z][a-z0-9+.-]*:\/\//i, '')
  const host = stripped.split('/')[0]
  return host === '' ? 'mqtt' : host
}

/** Poll the connection state until it is connected, failed, or out of time. */
async function waitForStatus(clients, id, timeoutMs, signal) {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const profile = clients.find(id)
    if (profile === null) return null
    const view = clients.view(profile)
    if (view.status === 'connected' || view.status === 'error') return view
    if (signal?.aborted === true || Date.now() >= deadline) return view
    await sleep(POLL_MS)
  }
}

/* ------------------------------------------------------------------ tools */

function listTool({ store, clients, broker }) {
  return {
    name: 'mqtt_list',
    description:
      'List the MQTT connections configured in the DSH MQTT panel with their live status and subscriptions, plus the embedded broker state. Every other mqtt_* tool takes a connection name or id from this list. Read-only.',
    parameters: {
      query: optStr('Optional fuzzy match against connection name, id, or URL.'),
      verbose: optBool('Also include the embedded broker user list and its connected clients (default false).'),
    },
    output: {
      schema: oObj({
        ok: oBool(),
        count: oInt(),
        connections: oArr(CONNECTION_SCHEMA),
        broker: BROKER_SCHEMA,
        stateFile: oStr(),
        users: oArr(USER_SCHEMA),
        brokerClients: oArr(BROKER_CLIENT_SCHEMA),
      }),
      render: (_args, value) => text(renderConnections(value.connections) + '\n\nbroker\n' + renderBroker(value.broker)),
    },
    async execute(args) {
      const input = args ?? {}
      try {
        const query = typeof input.query === 'string' ? input.query.trim().toLowerCase() : ''
        let rows = clients.list().map(connectionSummary)
        if (query !== '') {
          rows = rows.filter(
            (row) => row.name.toLowerCase().includes(query) || row.id.toLowerCase().includes(query) || row.url.toLowerCase().includes(query) || row.status.includes(query),
          )
        }
        const brokerView = broker.view()
        const result = {
          ok: true,
          count: rows.length,
          connections: rows,
          broker: brokerSummary(brokerView),
          stateFile: store.file,
          users: [],
          brokerClients: [],
        }
        if (input.verbose === true) {
          result.users = (brokerView.users ?? []).map((user) => ({ username: String(user.username), hasPassword: Boolean(user.hasPassword) }))
          result.brokerClients = (brokerView.clients ?? []).map((client) => ({
            id: String(client.id),
            connectedAt: Number(client.connectedAt ?? 0),
            remote: client.remote ?? null,
            clean: Boolean(client.clean),
            version: client.version === null || client.version === undefined ? null : String(client.version),
            subscriptions: (client.subscriptions ?? []).map((topic) => String(topic)),
          }))
        }
        return result
      } catch (error) {
        return { ok: false, count: 0, connections: [], broker: brokerSummary(broker.view()), stateFile: store.file, users: [], brokerClients: [] }
      }
    },
  }
}

function connectTool({ clients }) {
  return {
    name: 'mqtt_connect',
    description:
      'Connect a configured MQTT connection, or create and save a new one from a URL (tcp/mqtt, or ws/wss for a WebSocket broker) and connect it. Waits until the connection is online or has failed, then reports the status. Example: mqtt_connect { url: "mqtt://broker.example.com:1883", username: "u", password: "p" }.',
    parameters: {
      connection: optStr('Name or id of an existing connection (from mqtt_list). Omit when passing url.'),
      url: optStr('Broker URL for a new connection, e.g. mqtt://127.0.0.1:1883, mqtts://host:8883, ws://host:8083/mqtt.'),
      name: optStr('Display name for the new connection (defaults to host:port of the url).'),
      username: optStr('Username for the new connection.'),
      password: optStr('Password for the new connection (stored locally; never returned by the API).'),
      clientId: optStr('MQTT client id for the new connection (empty lets the library pick one).'),
      timeoutMs: optInt(`How long to wait for the connection (${MIN_WAIT_MS}-${MAX_WAIT_MS}, default ${DEFAULT_WAIT_MS}).`),
    },
    output: {
      schema: oObj({
        ok: oBool(),
        status: oStr(),
        error: oNullStr(),
        connection: CONNECTION_SCHEMA,
        note: oStr(),
      }),
      render: (_args, value) =>
        text(value.ok ? `connected: ${value.connection.name} (${value.connection.url})\n${value.note}`.trim() : `connect failed: ${value.error}`),
    },
    async execute(args, exec) {
      const input = args ?? {}
      const timeoutMs = clampTimeout(input.timeoutMs)
      try {
        let profile
        if (typeof input.connection === 'string' && input.connection.trim() !== '') {
          profile = resolveConnection(clients, input.connection)
        } else if (typeof input.url === 'string' && input.url.trim() !== '') {
          const url = input.url.trim()
          // Reuse a profile that already points at the same URL: creating a new
          // one on every call leaves junk cards for failed attempts and a second
          // copy of a working connection without its subscriptions.
          const known = clients.list().find((candidate) => String(candidate.url).trim() === url)
          const patch = { url }
          if (typeof input.name === 'string' && input.name.trim() !== '') patch.name = input.name.trim()
          else if (known === undefined) patch.name = deriveName(url)
          if (typeof input.username === 'string') patch.username = input.username
          if (typeof input.password === 'string') patch.password = input.password
          if (typeof input.clientId === 'string') patch.clientId = input.clientId
          if (known !== undefined) patch.id = known.id
          profile = await clients.save(patch)
        } else {
          const list = clients.list()
          if (list.length !== 1) throw new Error('pass connection (name or id) or url')
          profile = list[0]
        }
        await clients.connect(profile.id)
        const view = await waitForStatus(clients, profile.id, timeoutMs, exec?.signal)
        if (view === null) throw new Error('the connection disappeared while connecting')
        const summary = connectionSummary(view)
        const ok = summary.status === 'connected'
        const note = ok
          ? summary.subscriptions.length === 0
            ? 'no subscriptions yet: call mqtt_subscribe, then mqtt_wait.'
            : `subscriptions active: ${summary.subscriptions.map((item) => `${item.topic}@${item.qos}`).join(', ')}.`
          : summary.status === 'error'
            ? 'the broker rejected or the transport failed; check url, credentials, and tls settings.'
            : `still ${summary.status} after ${timeoutMs} ms.`
        return { ok, status: summary.status, error: summary.error, connection: summary, note }
      } catch (error) {
        return {
          ok: false,
          status: 'error',
          error: oneLine(error),
          connection: { id: '', name: '', url: '', status: 'error', autoConnect: false, hasPassword: false, connectedAt: null, error: oneLine(error), subscriptions: [] },
          note: '',
        }
      }
    },
  }
}

function disconnectTool({ clients }) {
  return {
    name: 'mqtt_disconnect',
    description: 'Close one MQTT connection. The connection stays configured (subscriptions are remembered) and can be reopened with mqtt_connect.',
    parameters: { connection: optStr('Name or id of the connection (from mqtt_list); omit when only one is configured.') },
    output: {
      schema: oObj({ ok: oBool(), connection: CONNECTION_SCHEMA, error: oNullStr() }),
      render: (_args, value) => text(value.ok ? `disconnected: ${value.connection.name}` : `disconnect failed: ${value.error}`),
    },
    async execute(args) {
      const input = args ?? {}
      try {
        const profile = resolveConnection(clients, input.connection)
        clients.disconnect(profile.id)
        const view = clients.find(profile.id)
        return { ok: true, connection: connectionSummary(view === null ? profile : clients.view(view)), error: null }
      } catch (error) {
        return { ok: false, connection: { id: '', name: '', url: '', status: 'idle', autoConnect: false, hasPassword: false, connectedAt: null, error: null, subscriptions: [] }, error: oneLine(error) }
      }
    },
  }
}

function subscribeTool({ clients }) {
  return {
    name: 'mqtt_subscribe',
    description:
      'Subscribe one configured connection to a topic filter (MQTT wildcards + and # are allowed). The subscription is remembered and re-applied whenever the connection comes back, so it can be added while offline. After subscribing, use mqtt_wait to observe messages.',
    parameters: {
      connection: optStr('Name or id of the connection; omit when only one is configured.'),
      topic: reqStr('Topic filter, e.g. sensors/+/temperature or devices/#.'),
      qos: optInt('Quality of service 0, 1, or 2 (default 0).'),
    },
    output: {
      schema: oObj({
        ok: oBool(),
        connection: oStr(),
        topic: oStr(),
        qos: oInt(),
        online: oBool(),
        granted: oNullInt(),
        error: oNullStr(),
      }),
      render: (_args, value) =>
        text(value.ok ? `subscribed ${value.connection} -> ${value.topic} (qos ${value.qos}, ${value.online ? 'online' : 'offline: applies on next connect'})` : `subscribe failed: ${value.error}`),
    },
    async execute(args) {
      const input = args ?? {}
      try {
        const profile = resolveConnection(clients, input.connection)
        const result = await clients.subscribe({ id: profile.id, topic: input.topic, qos: input.qos })
        const view = clients.find(profile.id)
        const status = view === null ? 'idle' : clients.view(view).status
        return {
          ok: true,
          connection: profile.name,
          topic: result.topic,
          qos: result.qos,
          online: status === 'connected',
          granted: grantedQos(result.granted),
          error: null,
        }
      } catch (error) {
        return { ok: false, connection: '', topic: typeof input.topic === 'string' ? input.topic : '', qos: 0, online: false, granted: null, error: oneLine(error) }
      }
    },
  }
}

function unsubscribeTool({ clients }) {
  return {
    name: 'mqtt_unsubscribe',
    description: 'Remove one topic filter from a configured connection and forget it, so it is not re-applied on the next connect.',
    parameters: {
      connection: optStr('Name or id of the connection; omit when only one is configured.'),
      topic: reqStr('Topic filter to remove.'),
    },
    output: {
      schema: oObj({ ok: oBool(), connection: oStr(), topic: oStr(), error: oNullStr() }),
      render: (_args, value) => text(value.ok ? `unsubscribed ${value.connection} from ${value.topic}` : `unsubscribe failed: ${value.error}`),
    },
    async execute(args) {
      const input = args ?? {}
      try {
        const profile = resolveConnection(clients, input.connection)
        await clients.unsubscribe({ id: profile.id, topic: input.topic })
        return { ok: true, connection: profile.name, topic: String(input.topic ?? ''), error: null }
      } catch (error) {
        return { ok: false, connection: '', topic: String(input.topic ?? ''), error: oneLine(error) }
      }
    },
  }
}

function publishTool({ clients, broker }) {
  return {
    name: 'mqtt_publish',
    description:
      'Publish one message. Use via "client" (default) to publish through an online connection (as a normal MQTT client), or via "broker" to publish from the embedded broker/server side into its own subscribers. Payload is UTF-8 text unless encoding is "base64".',
    parameters: {
      via: optEnum(['client', 'broker'], 'client = through an online connection (default); broker = from the embedded broker.'),
      connection: optStr('Connection name or id when via is "client"; omit when only one is configured.'),
      topic: reqStr('Topic to publish to (no wildcards).'),
      payload: reqStr('Message payload (text, or base64 when encoding is "base64").'),
      qos: optInt('Quality of service 0, 1, or 2 (default 0).'),
      retain: optBool('Set the retained flag (default false).'),
      encoding: optEnum(['utf8', 'base64'], 'Payload encoding (default utf8).'),
    },
    output: {
      schema: oObj({ ok: oBool(), via: oStr(), connection: oNullStr(), topic: oStr(), qos: oInt(), retain: oBool(), bytes: oInt(), error: oNullStr() }),
      render: (_args, value) =>
        text(value.ok ? `published ${value.topic} (${value.bytes} bytes, qos ${value.qos}, retain ${value.retain}, via ${value.via}${value.connection === null ? '' : ' ' + value.connection})` : `publish failed: ${value.error}`),
    },
    async execute(args) {
      const input = args ?? {}
      // Never coerce a typo into a working publish: an unknown `via` is a caller
      // error, not a request to use the default.
      if (input.via !== undefined && input.via !== null && input.via !== 'client' && input.via !== 'broker') {
        return { ok: false, via: 'client', connection: null, topic: String(input.topic ?? ''), qos: 0, retain: false, bytes: 0, error: `unknown via ${JSON.stringify(input.via)}: use "client" or "broker"` }
      }
      const via = input.via === 'broker' ? 'broker' : 'client'
      try {
        const shared = {
          topic: input.topic,
          payload: input.payload,
          qos: input.qos,
          retain: input.retain,
          encoding: input.encoding,
        }
        if (via === 'broker') {
          const result = await broker.publish(shared)
          return { ok: true, via, connection: null, topic: result.topic, qos: result.qos, retain: result.retain, bytes: result.bytes, error: null }
        }
        const profile = resolveConnection(clients, input.connection)
        const result = await clients.publish({ ...shared, id: profile.id })
        return { ok: true, via, connection: profile.name, topic: result.topic, qos: result.qos, retain: result.retain, bytes: result.bytes, error: null }
      } catch (error) {
        return { ok: false, via, connection: null, topic: String(input.topic ?? ''), qos: 0, retain: false, bytes: 0, error: oneLine(error) }
      }
    },
  }
}

function messagesTool({ clients, messages }) {
  return {
    name: 'mqtt_messages',
    description:
      'Read recent MQTT traffic from the in-memory log: messages received or published by configured connections, and messages that passed through the embedded broker. Newest last. Use mqtt_wait instead when you expect a message to arrive shortly.',
    parameters: {
      connection: optStr('Only messages of this connection (name or id).'),
      source: optEnum(['any', 'client', 'broker'], 'client = connection traffic, broker = traffic through the embedded broker (default any).'),
      direction: optEnum(['any', 'in', 'out'], 'in = received, out = published (default any).'),
      topic: optStr('Topic filter with MQTT wildcards, or an exact topic.'),
      since: optInt('Only entries with a sequence number greater than this one (from a previous nextSince).'),
      limit: optInt(`Maximum rows to return (1-${MAX_LIMIT}, default ${DEFAULT_LIMIT}).`),
    },
    output: {
      schema: oObj({ ok: oBool(), count: oInt(), nextSince: oInt(), entries: oArr(MESSAGE_SCHEMA), error: oNullStr() }),
      render: (_args, value) => text(value.ok ? `${value.count} message(s), nextSince ${value.nextSince}\n` + renderMessages(value.entries) : `cannot read messages: ${value.error}`),
    },
    async execute(args) {
      const input = args ?? {}
      try {
        const filter = messageFilter(clients, input)
        const limit = clampInt(input.limit, 1, MAX_LIMIT, DEFAULT_LIMIT)
        const rows = messages.tail(MAX_LIMIT).map(messageRow).filter((row) => rowMatches(row, filter))
        const since = Number.isFinite(Number(input.since)) ? Number(input.since) : null
        const windowed = since === null ? rows : rows.filter((row) => row.seq > since)
        const entries = windowed.slice(-limit)
        const last = messages.tail(1)[0]
        const nextSince = entries.length > 0 ? entries[entries.length - 1].seq : (last?.seq ?? 0)
        return { ok: true, count: entries.length, nextSince, entries, error: null }
      } catch (error) {
        return { ok: false, count: 0, nextSince: 0, entries: [], error: oneLine(error) }
      }
    },
  }
}

function waitTool({ clients, messages }) {
  return {
    name: 'mqtt_wait',
    description:
      'Wait until matching MQTT messages arrive, up to timeoutMs, then return them. This is the tool to use after mqtt_subscribe and mqtt_publish when you need to observe the response. Filters work like mqtt_messages. A message that arrived during the last lookbackMs before this call still counts as a match, so a fast reply is never missed. Returns timedOut true when nothing matched in time.',
    parameters: {
      connection: optStr('Only messages of this connection (name or id).'),
      source: optEnum(['any', 'client', 'broker'], 'client = connection traffic, broker = traffic through the embedded broker (default any).'),
      direction: optEnum(['any', 'in', 'out'], 'in = received, out = published (default any).'),
      topic: optStr('Topic filter with MQTT wildcards, or an exact topic.'),
      since: optInt('Start after this sequence number instead of using the lookback window.'),
      lookbackMs: optInt('Also count messages that arrived during the last N ms before this call (0-60000, default 5000; 0 waits only for new ones).'),
      count: optInt(`How many matching messages to wait for (1-${MAX_COUNT}, default 1).`),
      timeoutMs: optInt(`How long to wait (${MIN_WAIT_MS}-${MAX_WAIT_MS}, default ${DEFAULT_WAIT_MS}).`),
      limit: optInt(`Maximum rows to return (1-${MAX_LIMIT}, default ${DEFAULT_LIMIT}).`),
    },
    output: {
      schema: oObj({ ok: oBool(), timedOut: oBool(), count: oInt(), waitedMs: oInt(), nextSince: oInt(), entries: oArr(MESSAGE_SCHEMA), error: oNullStr() }),
      render: (_args, value) => {
        if (value.error !== null) return text(`cannot wait for messages: ${value.error}`)
        if (value.count === 0) return text(`no matching message within ${value.waitedMs} ms (nextSince ${value.nextSince})`)
        return text(`${value.count} message(s) after ${value.waitedMs} ms, nextSince ${value.nextSince}\n` + renderMessages(value.entries))
      },
    },
    async execute(args, exec) {
      const input = args ?? {}
      const startedAt = Date.now()
      const timeoutMs = clampTimeout(input.timeoutMs)
      const wanted = clampInt(input.count, 1, MAX_COUNT, 1)
      const limit = clampInt(input.limit, 1, MAX_LIMIT, Math.max(wanted, DEFAULT_LIMIT))
      const signal = exec?.signal
      try {
        const filter = messageFilter(clients, input)
        const lookbackMs = clampInt(input.lookbackMs, 0, 60000, 5000)
        let cursor
        if (Number.isFinite(Number(input.since))) {
          cursor = Number(input.since)
        } else {
          const window = messages.tail(MAX_LIMIT)
          const cutoff = startedAt - lookbackMs
          const recent = window.filter((entry) => lookbackMs > 0 && entry.ts >= cutoff)
          cursor = recent.length > 0 ? recent[0].seq - 1 : (messages.tail(1)[0]?.seq ?? 0)
        }
        const collected = []
        const deadline = startedAt + timeoutMs
        for (;;) {
          const scanned = messages.since(cursor, MAX_LIMIT)
          for (const row of scanned.map(messageRow)) {
            cursor = row.seq
            if (rowMatches(row, filter)) collected.push(row)
          }
          if (collected.length >= wanted) break
          if (signal?.aborted === true) {
            return { ok: false, timedOut: false, count: collected.length, waitedMs: Date.now() - startedAt, nextSince: cursor, entries: collected.slice(-limit), error: 'cancelled' }
          }
          if (Date.now() >= deadline) break
          await sleep(POLL_MS)
        }
        const entries = collected.slice(-limit)
        return {
          ok: entries.length > 0,
          timedOut: entries.length === 0,
          count: entries.length,
          waitedMs: Date.now() - startedAt,
          nextSince: cursor,
          entries,
          error: null,
        }
      } catch (error) {
        return { ok: false, timedOut: false, count: 0, waitedMs: Date.now() - startedAt, nextSince: 0, entries: [], error: oneLine(error) }
      }
    },
  }
}

function brokerTool({ broker }) {
  return {
    name: 'mqtt_broker',
    description:
      'Control the embedded MQTT broker (server) of DSH: check its status, start it on a TCP port for other MQTT software, stop it, or save its settings (bind host/port, anonymous access, users, auto-start). A running broker listens on plain MQTT/TCP so any MQTT client or tool can connect to it.',
    parameters: {
      action: reqStr('One of: status, start, stop, save, clear-retained. clear-retained drops every retained message.'),
      host: optStr('Bind address, e.g. 0.0.0.0 or 127.0.0.1 (save/start).'),
      port: optInt('TCP port to listen on, e.g. 1883 (save/start).'),
      allowAnonymous: optBool('Allow clients without credentials (save/start).'),
      autoStart: optBool('Start the broker whenever DSH starts (save).'),
      maxConnections: optInt('Maximum simultaneous TCP connections; 0 means unlimited (save).'),
      connectTimeoutMs: optInt('How long a new socket may take to send CONNECT, in milliseconds (save).'),
      maxPayloadBytes: optInt('Largest accepted PUBLISH payload in bytes (save).'),
      maxRetainedMessages: optInt('Maximum retained topics; 0 means unlimited (save).'),
      users: {
        type: 'array',
        required: false,
        description: 'Full user list [{ username, password? }]; an omitted password keeps the stored one (save).',
        items: {
          type: 'object',
          additionalProperties: false,
          properties: { username: { type: 'string', required: true }, password: { type: 'string' } },
        },
      },
    },
    output: {
      schema: oObj({
        ok: oBool(),
        action: oStr(),
        note: oStr(),
        broker: BROKER_SCHEMA,
        users: oArr(USER_SCHEMA),
        clients: oArr(BROKER_CLIENT_SCHEMA),
        error: oNullStr(),
      }),
      render: (_args, value) => text((value.ok ? '' : `failed: ${value.error}\n`) + renderBroker(value.broker) + (value.note === '' ? '' : '\n' + value.note)),
    },
    async execute(args) {
      const input = args ?? {}
      const action = typeof input.action === 'string' ? input.action.trim().toLowerCase() : ''
      try {
        if (!['status', 'start', 'stop', 'save', 'clear-retained'].includes(action)) {
          throw new Error('action must be one of: status, start, stop, save, clear-retained')
        }
        const settings = {}
        if (typeof input.host === 'string' && input.host.trim() !== '') settings.host = input.host.trim()
        if (input.port !== undefined && input.port !== null) settings.port = Number(input.port)
        if (typeof input.allowAnonymous === 'boolean') settings.allowAnonymous = input.allowAnonymous
        if (typeof input.autoStart === 'boolean') settings.autoStart = input.autoStart
        if (input.maxConnections !== undefined && input.maxConnections !== null) settings.maxConnections = Number(input.maxConnections)
        if (input.connectTimeoutMs !== undefined && input.connectTimeoutMs !== null) settings.connectTimeoutMs = Number(input.connectTimeoutMs)
        if (input.maxPayloadBytes !== undefined && input.maxPayloadBytes !== null) settings.maxPayloadBytes = Number(input.maxPayloadBytes)
        if (input.maxRetainedMessages !== undefined && input.maxRetainedMessages !== null) {
          settings.maxRetainedMessages = Number(input.maxRetainedMessages)
        }
        if (Array.isArray(input.users)) {
          settings.users = input.users.map((user) => ({
            username: String(user?.username ?? '').trim(),
            password: typeof user?.password === 'string' ? user.password : '',
          }))
        }
        // Snapshot the live listener before saveSettings rewrites the in-memory
        // settings: the note must describe where the server is actually listening,
        // not the host the user just asked for.
        const before = broker.view()
        const hasSettings = Object.keys(settings).length > 0
        let note = ''
        if (action === 'save' || (action !== 'status' && action !== 'stop' && hasSettings)) {
          broker.saveSettings(settings)
          note = broker.view().status === 'running' ? 'saved; the broker is running, so restart it to apply the new listener settings' : 'saved'
        }
        if (action === 'start') {
          await broker.start()
          const view = broker.view()
          if (view.status === 'running' && before.status === 'running') {
            // start() is a no-op while the listener is already up; saying
            // "listening on <new host>:<old port>" would both lie and hide the
            // restart hint that saveSettings just produced.
            note = `${hasSettings ? `saved; restart the broker to apply the new listener settings — it is still listening on ${before.boundHost ?? before.host}:${before.boundPort}` : `already listening on ${before.boundHost ?? before.host}:${before.boundPort}`}`
          } else if (view.status === 'running') {
            note = `listening on ${view.host}:${view.boundPort}; point any MQTT client at that address`
          }
        }
        if (action === 'stop') {
          await broker.stop()
          note = 'broker stopped'
        }
        if (action === 'clear-retained') {
          const cleared = await broker.clearRetained()
          note = cleared.cleared === 0 ? 'no retained messages to clear' : `cleared ${cleared.cleared} retained topic(s)`
        }
        const view = broker.view()
        return {
          ok: view.status !== 'error',
          action,
          note,
          broker: brokerSummary(view),
          users: (view.users ?? []).map((user) => ({ username: String(user.username), hasPassword: Boolean(user.hasPassword) })),
          clients: (view.clients ?? []).map((client) => ({
            id: String(client.id),
            connectedAt: Number(client.connectedAt ?? 0),
            remote: client.remote ?? null,
            clean: Boolean(client.clean),
            version: client.version === null || client.version === undefined ? null : String(client.version),
            subscriptions: (client.subscriptions ?? []).map((topic) => String(topic)),
          })),
          error: view.error ?? null,
        }
      } catch (error) {
        const view = broker.view()
        return { ok: false, action, note: '', broker: brokerSummary(view), users: [], clients: [], error: oneLine(error) }
      }
    },
  }
}

/**
 * Turn the record-of-descriptors used by the tool builders below into the JSON
 * Schema object the tool registry hands to models: the `required: true`
 * shorthand on a property becomes the object's own `required` list.
 */
function jsonParameters(record) {
  const properties = {}
  const required = []
  for (const [name, spec] of Object.entries(record ?? {})) {
    const { required: isRequired, ...node } = spec ?? {}
    properties[name] = normalizeNode(node)
    if (isRequired === true) required.push(name)
  }
  const schema = { type: 'object', additionalProperties: false, properties }
  if (required.length > 0) schema.required = required
  return schema
}

/** Lift property-level `required: true` markers into each object node's own list. */
function normalizeNode(node) {
  if (node === null || typeof node !== 'object') return node
  if (Array.isArray(node)) return node.map(normalizeNode)
  const next = {}
  const required = Array.isArray(node.required) ? [...node.required] : []
  for (const [key, value] of Object.entries(node)) {
    if (key === 'required') continue
    if (key === 'properties' && value !== null && typeof value === 'object') {
      const properties = {}
      for (const [name, spec] of Object.entries(value)) {
        const { required: isRequired, ...rest } = spec ?? {}
        if (isRequired === true && required.includes(name) === false) required.push(name)
        properties[name] = normalizeNode(rest)
      }
      next.properties = properties
      continue
    }
    if (key === 'items') {
      // An item schema is a position, not a member: `required` there means nothing.
      if (Array.isArray(value)) {
        next.items = value.map(normalizeNode)
      } else {
        const { required: _positional, ...rest } = value ?? {}
        next.items = normalizeNode(rest)
      }
      continue
    }
    next[key] = normalizeNode(value)
  }
  if (required.length > 0) next.required = required
  return next
}

/** mqtt.js acknowledges a subscribe with an array of `{topic, qos}`; report the first granted qos. */
function grantedQos(value) {
  const first = Array.isArray(value) ? value[0] : value
  const qos = first !== null && typeof first === 'object' ? first.qos : first
  const number = Number(qos)
  return Number.isFinite(number) ? number : null
}

/** The registry wants full JSON Schema for `parameters`, and so does the model. */
function registryShape(definition) {
  return {
    ...definition,
    parameters: jsonParameters(definition.parameters),
    output: { ...definition.output, schema: normalizeNode(definition.output.schema) },
    execute: async (args, exec) => losslessJson(await definition.execute(args, exec)),
  }
}

/** Build the full tool list for one mounted engine. */
export function createTools(deps) {
  return [
    listTool(deps),
    connectTool(deps),
    disconnectTool(deps),
    subscribeTool(deps),
    unsubscribeTool(deps),
    publishTool(deps),
    messagesTool(deps),
    waitTool(deps),
    brokerTool(deps),
  ].map(registryShape)
}
