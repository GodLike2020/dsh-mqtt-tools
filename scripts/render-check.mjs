// Scratch render check for client.js (not shipped): renders the panel's
// sub-views with react-dom/server so undefined components or bad props throw
// here instead of in the browser.
import { createRequire } from 'node:module'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

const here = path.dirname(fileURLToPath(import.meta.url))
const requireFromTools = createRequire(path.join(here, '..', '.testtools', 'noop.js'))
const requireFromRoot = createRequire(path.join(here, '..', 'package.json'))
// The dev packages live in `.testtools/` in this checkout, or in the root
// `devDependencies` after `npm install`; try both.
const devRequire = (id) => {
  for (const load of [requireFromTools, requireFromRoot]) {
    try {
      return load(id)
    } catch {
      /* try the next location */
    }
  }
  throw new Error(`${id} is not installed (run npm install, or install it in .testtools)`)
}
const React = devRequire('react')
const { renderToString } = devRequire('react-dom/server')
const h = React.createElement

let captured = null
globalThis.__DSH_MQTT_TEST__ = true
globalThis.window = {
  __ModuleLoader__: { load(spec) { captured = spec } },
  location: { origin: 'http://127.0.0.1:14086' },
  localStorage: { getItem: () => null, setItem: () => {} },
  addEventListener: () => {},
  removeEventListener: () => {},
}
globalThis.document = { documentElement: { lang: 'zh-CN' }, addEventListener: () => {}, removeEventListener: () => {} }
Object.defineProperty(globalThis, 'navigator', { value: { language: 'zh-CN' }, configurable: true })

await import('../client.js')
if (!captured) throw new Error('client.js did not call window.__ModuleLoader__.load')
console.log('loaded module id:', captured.id)

const plugin = captured.factory((id) => {
  if (id === 'react') return React
  throw new Error(`unexpected require: ${id}`)
})
console.log('plugin keys:', Object.keys(plugin))
if (typeof plugin.apply !== 'function' || !Array.isArray(plugin.inject)) throw new Error('bad plugin shape')

const components = plugin.__components
const now = Date.now()
const state = {
  clients: [
    {
      id: 'c1',
      name: 'Local broker',
      url: 'mqtt://127.0.0.1:1883',
      clientId: '',
      username: 'demo',
      hasPassword: true,
      keepalive: 60,
      clean: true,
      protocolVersion: 4,
      connectTimeout: 10000,
      reconnectPeriod: 3000,
      rejectUnauthorized: true,
      autoConnect: false,
      status: 'connected',
      error: null,
      subscriptions: [{ topic: 'sensors/#', qos: 1 }],
    },
    { id: 'c2', name: 'Cloud', url: 'mqtts://broker.example.com:8883', status: 'error', error: 'ECONNREFUSED', subscriptions: [] },
  ],
  broker: {
    host: '0.0.0.0',
    port: 1883,
    autoStart: true,
    allowAnonymous: false,
    protocols: ['3.1', '3.1.1', '5.0'],
    users: [{ username: 'demo', hasPassword: true }],
    status: 'running',
    error: null,
    startedAt: now - 65000,
    boundPort: 1883,
    restartHint: true,
    clientCount: 1,
    clients: [{ id: 'mqttjs_abc', remote: '127.0.0.1:52344', clean: true, version: 'MQTT 5.0', subscriptions: ['sensors/#'] }],
    stats: { connections: 1, published: 4, system: 0 },
    library: { aedes: true },
  },
  settings: { showSystem: false },
  stateFile: 'C:\\Users\\wmd\\.dsh\\dsh-mqtt-tools.json',
  library: { mqtt: true, aedes: true },
}
const messages = [
  { seq: 1, ts: now - 2000, kind: 'client', direction: 'in', clientId: 'c1', name: 'Local broker', topic: 'sensors/room-1', qos: 1, retain: false, payload: '{"t":21.5}', encoding: 'utf8', bytes: 11 },
  { seq: 2, ts: now - 1000, kind: 'client', direction: 'out', clientId: 'c1', name: 'Local broker', topic: 'cmd/light', qos: 0, retain: true, payload: 'on', encoding: 'utf8', bytes: 2 },
  { seq: 3, ts: now, kind: 'broker', direction: 'in', topic: 'x/y', qos: 0, retain: false, payload: 'AAEC', encoding: 'base64', bytes: 3 },
]
const events = [
  { seq: 1, ts: now - 3000, action: 'start', clientId: null, text: '' },
  { seq: 2, ts: now - 2000, action: 'connect', clientId: 'mqttjs_abc', text: 'connected' },
  { seq: 3, ts: now - 1000, action: 'error', clientId: 'mqttjs_abc', text: 'boom' },
]
const manyMessages = Array.from({ length: 250 }, (_, index) => ({
  seq: index + 1,
  ts: now - (250 - index) * 100,
  kind: 'client',
  direction: index % 2 === 0 ? 'in' : 'out',
  clientId: 'c1',
  name: 'Local broker',
  topic: `sensors/${index}`,
  qos: 1,
  retain: false,
  payload: `{"n":${index}}`,
  encoding: 'utf8',
  bytes: 8,
}))
const run = async () => ({ id: 'created' })

const cases = [
  ['ConnectionsTab/selected', h(components.ConnectionsTab, { state, messages, run, selectedId: 'c1', setSelectedId() {} })],
  ['ConnectionsTab/error-profile', h(components.ConnectionsTab, { state, messages, run, selectedId: 'c2', setSelectedId() {} })],
  ['ConnectionsTab/empty', h(components.ConnectionsTab, { state: { ...state, clients: [] }, messages: [], run, selectedId: null, setSelectedId() {} })],
  ['BrokerTab/running', h(components.BrokerTab, { state, messages, events, run, showSystem: false })],
  ['BrokerTab/stopped', h(components.BrokerTab, { state: { ...state, broker: { ...state.broker, status: 'stopped', error: null, restartHint: false, clients: [], clientCount: 0 } }, messages: [], events: [], run, showSystem: true })],
  ['BrokerTab/error', h(components.BrokerTab, { state: { ...state, broker: { ...state.broker, status: 'error', error: 'EADDRINUSE' } }, messages: [], events, run, showSystem: false })],
  ['MessageLog/rows', h(components.MessageLog, { entries: messages, onClear() {} })],
  ['MessageLog/empty', h(components.MessageLog, { entries: [], onClear() {} })],
  ['Publisher', h(components.Publisher, { onPublish: async () => {}, disabledReason: null })],
  ['Publisher/disabled', h(components.Publisher, { onPublish: async () => {}, disabledReason: 'connect first' })],
  ['Panel/loading', h(components.Panel)],
  ['PanelIcon', h(components.PanelIcon, { size: 16, active: true })],
]

let failed = 0
for (const [label, element] of cases) {
  try {
    const html = renderToString(element)
    if (html.length < 20) throw new Error(`suspiciously small output (${html.length} bytes)`)
    console.log(`ok   ${label} (${html.length} bytes)`)
  } catch (error) {
    failed += 1
    console.log(`FAIL ${label}: ${error?.stack ?? error}`)
  }
}

// Pagination: a busy log shows one page and offers the older rows on demand.
try {
  const pageHtml = renderToString(h(components.MessageLog, { entries: manyMessages, onClear() {} }))
  const rowCount = (pageHtml.match(/class="mqtt-log-row mqtt-msg/g) ?? []).length
  if (rowCount !== 100) throw new Error(`expected 100 visible rows, got ${rowCount}`)
  if (!pageHtml.includes('更早')) throw new Error('missing the older-pages control')
  if (!pageHtml.includes('100 / 250')) throw new Error('missing the page counter')
  console.log(`ok   MessageLog/pagination (${rowCount} of ${manyMessages.length} rows)`)
} catch (error) {
  failed += 1
  console.log(`FAIL MessageLog/pagination: ${error?.message ?? error}`)
}

// The apply() path must register exactly the two slots with the expected shapes.
const registered = []
const disposed = []
const ctx = {
  slots: {
    inject: (key, callback) => {
      const dispose = callback()
      disposed.push(typeof dispose)
      return () => {}
    },
    register: (spec, component) => {
      registered.push({ key: spec.name, spec, component })
      return () => {}
    },
  },
  effect: (fn) => {
    fn()
    return () => {}
  },
}
plugin.apply(ctx)
console.log('registrations:', registered.map((entry) => `${entry.key}:${entry.spec.id ?? entry.spec.key} order=${entry.spec.order ?? '-'}`))
if (registered.length !== 2) throw new Error('expected 2 slot registrations')

if (failed > 0) {
  console.log('RENDER FAILURES:', failed)
  process.exit(1)
}
console.log('OK')
