// dsh-mqtt-tools UI suite: SSR of the panel state space, jsdom interaction tests and
// static audits of client.js (dark-theme contrast, i18n parity, structure).
//
// The browser half is loaded the same way the DSH web client loads it: stub
// `window.__ModuleLoader__`, set `__DSH_MQTT_TEST__` so the factory additionally
// returns `__components`, then call `factory(require)` once per language (each
// call is a fresh module instance with its own module-scope `lastTab`).
//
// Deliberate stubs (jsdom is a DOM, not the DSH shell):
//   * EventSource: the panel subscribes to /dsh-mqtt-tools/api/events. The stub records
//     instances and replays one `hello` frame built from `globalThis.__SSE_HELLO__`
//     when the test calls instance.open(); further frames use instance.emit().
//     Nothing is scheduled automatically, so the pre-hello loading state is a real,
//     observable DOM state instead of a race against a timer.
//   * fetch: /dsh-mqtt-tools/api/command belongs to the host. The stub records requests
//     and answers `{ ok: true }`; tests that check a command inspect the recording
//     or inject their own `run` function into the components.
//
// Order matters: jsdom globals must exist before react-dom/client is required,
// otherwise React takes its pre-DOM event path (`canUseDOM` false) and form
// events never reach onChange.
//
// The dev packages live either in `.testtools/` (the way this checkout was
// developed, kept out of the plugin's dependency graph) or as the root
// `devDependencies` after a plain `npm install`; both are tried in that order.
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Suite, runSuite, LIVE_BASE } from './harness.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const PLUGIN_DIR = dirname(dirname(HERE));
const CLIENT_PATH = join(PLUGIN_DIR, 'client.js');
const TOOLS_DIR = join(PLUGIN_DIR, '.testtools');
const requireTools = createRequire(join(TOOLS_DIR, 'noop.cjs'));
const requireRoot = createRequire(join(PLUGIN_DIR, 'package.json'));

function tool(id) {
  for (const [where, load] of [['.testtools', requireTools], ['node_modules', requireRoot]]) {
    try {
      return load(id);
    } catch {
      /* try the next location */
    }
  }
  throw new Error(`ui suite needs '${id}' installed in .testtools or as a dev dependency (npm install)`);
}

const nodeFetch = globalThis.fetch;
const CLIENT_SRC = readFileSync(CLIENT_PATH, 'utf8');
const FIXTURE_TICK = 1730000000000;

// --------------------------------------------------------------- jsdom setup
const { JSDOM } = tool('jsdom');
const dom = new JSDOM('<!doctype html><html lang="zh-CN"><head></head><body></body></html>', {
  url: 'http://127.0.0.1:14086/',
  pretendToBeVisual: true,
});
globalThis.window = dom.window;
globalThis.document = dom.window.document;
Object.defineProperty(globalThis, 'navigator', { value: dom.window.navigator, configurable: true });
// Must be set before react-dom/client is required, see header comment.
globalThis.IS_REACT_ACT_ENVIRONMENT = true;

// ------------------------------------------------ EventSource / fetch stubs
class StubEventSource {
  static instances = [];

  constructor(url) {
    this.url = String(url);
    this.readyState = 1;
    this.closed = false;
    this.listeners = new Map();
    this.onopen = null;
    this.onerror = null;
    StubEventSource.instances.push(this);
  }

  addEventListener(type, listener) {
    const list = this.listeners.get(type) ?? [];
    list.push(listener);
    this.listeners.set(type, list);
  }

  removeEventListener(type, listener) {
    const list = this.listeners.get(type) ?? [];
    this.listeners.set(type, list.filter((item) => item !== listener));
  }

  open() {
    if (this.closed) return;
    if (this.onopen) this.onopen();
    this.emit('hello', globalThis.__SSE_HELLO__ ?? { state: null, messages: [], events: [] });
  }

  emit(type, payload) {
    const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
    for (const listener of this.listeners.get(type) ?? []) listener({ type, data });
  }

  close() {
    this.closed = true;
    this.readyState = 2;
  }
}

const fetchCalls = [];
function stubFetch(url, options = {}) {
  let body = null;
  try {
    body = options.body ? JSON.parse(options.body) : null;
  } catch {
    body = options.body ?? null;
  }
  fetchCalls.push({ url: String(url), method: options.method ?? 'GET', body });
  const reply = globalThis.__FETCH_REPLY__;
  const envelope = reply?.body ?? { ok: true, state: null, result: null };
  const status = reply?.status ?? 200;
  return Promise.resolve({
    status,
    ok: status < 400,
    json: () => Promise.resolve(envelope),
  });
}

globalThis.EventSource = StubEventSource;
globalThis.fetch = stubFetch;

function resetStubs() {
  for (const instance of StubEventSource.instances) {
    try {
      instance.close();
    } catch {
      /* already closed */
    }
  }
  StubEventSource.instances.length = 0;
  fetchCalls.length = 0;
  globalThis.__SSE_HELLO__ = { state: null, messages: [], events: [] };
  globalThis.__FETCH_REPLY__ = null;
}

// ------------------------------------- React from .testtools, then client.js
const React = tool('react');
const { renderToStaticMarkup } = tool('react-dom/server');
const { createRoot } = tool('react-dom/client');
const h = React.createElement;

let loaderSpec = null;

async function bootstrap() {
  globalThis.__DSH_MQTT_TEST__ = true;
  globalThis.window.__ModuleLoader__ = {
    load(spec) {
      loaderSpec = spec;
    },
  };
  await import(pathToFileURL(CLIENT_PATH).href);
  if (!loaderSpec) throw new Error('client.js never called window.__ModuleLoader__.load');
  if (loaderSpec.id !== 'dsh-mqtt-tools') throw new Error(`loader id ${JSON.stringify(loaderSpec.id)}, expected 'dsh-mqtt-tools'`);
}

function loadPlugin(lang = 'zh-CN') {
  dom.window.document.documentElement.lang = lang;
  return loaderSpec.factory((id) => {
    if (id === 'react') return React;
    throw new Error(`client.js required unexpected module '${id}' (only 'react' is allowed)`);
  });
}

// ------------------------------------------------------------------ fixtures
function brokerFixture(patch = {}) {
  return {
    host: '0.0.0.0',
    port: 1883,
    boundPort: 1883,
    autoStart: true,
    allowAnonymous: false,
    protocols: ['3.1', '3.1.1', '5.0'],
    users: [{ username: 'demo', hasPassword: true }],
    status: 'running',
    error: null,
    startedAt: FIXTURE_TICK - 65000,
    restartHint: false,
    clientCount: 1,
    clients: [
      {
        id: 'mqttjs_abc',
        remote: '127.0.0.1:52344',
        clean: true,
        version: 'MQTT 5.0',
        subscriptions: ['sensors/#'],
      },
    ],
    stats: { connections: 1, published: 4, system: 0 },
    library: { aedes: true },
    ...patch,
  };
}

function clientsFixture() {
  return [
    {
      id: 'c1',
      name: 'Local broker',
      url: 'mqtt://127.0.0.1:1883',
      clientId: '',
      username: '',
      hasPassword: true,
      keepalive: 60,
      clean: true,
      protocolVersion: 5,
      connectTimeout: 30000,
      reconnectPeriod: 1000,
      rejectUnauthorized: true,
      autoConnect: true,
      status: 'connected',
      error: null,
      subscriptions: [
        { topic: 'sensors/#', qos: 1 },
        { topic: 'alerts/+', qos: 0 },
      ],
    },
    {
      id: 'c2',
      name: 'Cloud',
      url: 'mqtts://broker.example.com:8883',
      status: 'error',
      error: 'ECONNREFUSED',
      subscriptions: [],
    },
  ];
}

function makeState(patch = {}) {
  return {
    clients: clientsFixture(),
    broker: brokerFixture({ restartHint: true }),
    settings: { showSystem: false },
    library: { mqtt: true, aedes: true },
    stateFile: 'C:\\dsh-mqtt-tools\\state.json',
    ...patch,
  };
}

function makeMessages() {
  return [
    { seq: 1, ts: FIXTURE_TICK, direction: 'in', topic: 'sensors/room-1/temp', payload: '21.5', qos: 1, retain: false, bytes: 4, kind: 'client', clientId: 'c1' },
    { seq: 2, ts: FIXTURE_TICK + 1000, direction: 'out', topic: 'sensors/room-1/cmd', payload: '{"on":true}', qos: 0, retain: false, bytes: 11, kind: 'client', clientId: 'c1' },
    { seq: 3, ts: FIXTURE_TICK + 2000, direction: 'in', topic: 'sensors/room-1/hum', payload: '48', qos: 0, retain: true, bytes: 2, kind: 'client', clientId: 'c1' },
    { seq: 4, ts: FIXTURE_TICK + 3000, direction: 'out', topic: 'aedes/start', payload: 'broker ready', qos: 0, retain: false, bytes: 12, kind: 'broker' },
  ];
}

function makeEvents() {
  return [
    { seq: 1, ts: FIXTURE_TICK, action: 'client.connect', clientId: 'mqttjs_abc', text: '127.0.0.1:52344' },
    { seq: 2, ts: FIXTURE_TICK + 1000, action: 'client.subscribe', clientId: 'mqttjs_abc', text: 'sensors/#' },
  ];
}

function makeEntries(count) {
  return Array.from({ length: count }, (_, index) => ({
    seq: index + 1,
    ts: FIXTURE_TICK + index * 1000,
    direction: index % 2 === 0 ? 'in' : 'out',
    topic: `sensors/${index}`,
    payload: `p${index}`,
    qos: index % 3,
    retain: index % 50 === 0,
    bytes: index + 1,
  }));
}

const stubRun = async () => ({});

// --------------------------------------------------------------- assertions
function has(html, needle, label) {
  if (!String(html).includes(needle)) {
    throw new Error(`${label}: expected rendered HTML to contain ${JSON.stringify(needle)}`);
  }
}

function notHas(html, needle, label) {
  if (String(html).includes(needle)) {
    throw new Error(`${label}: rendered HTML must not contain ${JSON.stringify(needle)}`);
  }
}

function countOf(html, pattern) {
  return (String(html).match(pattern) ?? []).length;
}

function ssr(Component, props) {
  return renderToStaticMarkup(h(Component, props));
}

const CJK = /[\u3400-\u9fff]/;

// ------------------------------------------------------------ jsdom harness
function makeHarness() {
  const container = dom.window.document.createElement('div');
  dom.window.document.body.appendChild(container);
  const root = createRoot(container);
  let rendered = false;
  let disposed = false;

  async function render(node) {
    rendered = true;
    await React.act(async () => {
      root.render(node);
    });
  }

  async function act(fn) {
    await React.act(async () => {
      await fn();
    });
  }

  async function flush(ms = 0) {
    await React.act(async () => {
      await new Promise((resolve) => setTimeout(resolve, ms));
    });
  }

  async function unmount() {
    if (!rendered || disposed) return;
    disposed = true;
    await React.act(async () => {
      root.unmount();
    });
    container.remove();
  }

  async function dispose() {
    try {
      await unmount();
    } finally {
      if (!rendered) container.remove();
    }
  }

  const q = (selector) => container.querySelector(selector);
  const qa = (selector) => Array.from(container.querySelectorAll(selector));

  return { container, root, render, act, flush, unmount, dispose, q, qa };
}

function setInputValue(harness, element, value) {
  const proto = element instanceof dom.window.HTMLTextAreaElement
    ? dom.window.HTMLTextAreaElement.prototype
    : dom.window.HTMLInputElement.prototype;
  const setter = Object.getOwnPropertyDescriptor(proto, 'value').set;
  setter.call(element, value);
  element.dispatchEvent(new dom.window.Event('input', { bubbles: true }));
}

function setSelectValue(element, value) {
  element.value = value;
  element.dispatchEvent(new dom.window.Event('change', { bubbles: true }));
}

function buttonByText(harness, text) {
  return harness.qa('button').find((button) => button.textContent.trim().startsWith(text)) ?? null;
}

function protocolInputs(harness) {
  return harness
    .qa('.mqtt-check')
    .filter((label) => /^MQTT/.test(label.textContent.trim()))
    .map((label) => label.querySelector('input[type="checkbox"]'));
}

function checkedStates(inputs) {
  return inputs.map((input) => input.checked);
}

function eqStates(actual, expected, label) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label}: checked=${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`);
  }
}

// -------------------------------------------------------------- CSS parsing
const CSS_LIGHT_SURFACE = /#(?:fff|ffffff|fafafa|f5f5f5|eeeeee|eee)\b|rgba?\(\s*255\s*,\s*255\s*,\s*255[^)]*\)|\bwhite\b/i;

function extractCss(source) {
  const marker = 'const CSS = `';
  const start = source.indexOf(marker);
  if (start < 0) throw new Error('client.js: CSS template literal not found');
  const from = start + marker.length;
  const end = source.indexOf('`', from);
  if (end < 0) throw new Error('client.js: unterminated CSS template literal');
  return source.slice(from, end);
}

function parseRules(css) {
  const withoutComments = css.replace(/\/\*[\s\S]*?\*\//g, '');
  const rules = [];
  const re = /([^{}]+)\{([^{}]*)\}/g;
  let match;
  while ((match = re.exec(withoutComments)) !== null) {
    const decls = match[2]
      .split(';')
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => {
        const colon = part.indexOf(':');
        if (colon < 0) return { prop: part, value: '' };
        return { prop: part.slice(0, colon).trim().toLowerCase(), value: part.slice(colon + 1).trim() };
      });
    rules.push({ selector: match[1].trim(), decls });
  }
  return rules;
}

function hexToRgb(hex) {
  let value = String(hex).replace('#', '');
  if (!/^[0-9a-f]{3}$|^[0-9a-f]{6}$/i.test(value)) return null;
  if (value.length === 3) value = value.split('').map((ch) => ch + ch).join('');
  const number = Number.parseInt(value, 16);
  return [((number >> 16) & 255) / 255, ((number >> 8) & 255) / 255, (number & 255) / 255];
}

function luminance(rgb) {
  const channel = (value) => (value <= 0.03928 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4);
  return 0.2126 * channel(rgb[0]) + 0.7152 * channel(rgb[1]) + 0.0722 * channel(rgb[2]);
}

function contrastRatio(hexA, hexB) {
  const a = luminance(hexToRgb(hexA));
  const b = luminance(hexToRgb(hexB));
  const [high, low] = a > b ? [a, b] : [b, a];
  return (high + 0.05) / (low + 0.05);
}

function firstHex(value) {
  const match = /#(?:[0-9a-f]{6}|[0-9a-f]{3})\b/i.exec(String(value));
  return match ? match[0] : null;
}

function fallbackHex(value) {
  const call = /var\([^,]+,\s*([^)]*)\)/.exec(String(value));
  return call ? firstHex(call[1]) : null;
}

function lineOf(source, index) {
  return source.slice(0, index).split('\n').length;
}

// ----------------------------------------------------------- i18n parsing
function skipSpace(source, index) {
  let cursor = index;
  while (cursor < source.length && /\s/.test(source[cursor])) cursor += 1;
  return cursor;
}

function readQuoted(source, index) {
  const quote = source[index];
  if (quote !== "'" && quote !== '"') return null;
  let value = '';
  let cursor = index + 1;
  while (cursor < source.length) {
    const ch = source[cursor];
    if (ch === '\\') {
      value += source[cursor + 1] ?? '';
      cursor += 2;
      continue;
    }
    if (ch === quote) return { value, end: cursor + 1 };
    value += ch;
    cursor += 1;
  }
  return null;
}

function parseTranslationPairs(source) {
  const pairs = [];
  for (let index = 0; index < source.length; index += 1) {
    if (!source.startsWith('t(', index)) continue;
    const previous = index > 0 ? source[index - 1] : '';
    if (/[A-Za-z0-9_$.]/.test(previous)) continue;
    const line = lineOf(source, index);
    const first = readQuoted(source, skipSpace(source, index + 2));
    if (!first) throw new Error(`client.js:${line}: t() first argument is not a string literal`);
    const comma = skipSpace(source, first.end);
    if (source[comma] !== ',') throw new Error(`client.js:${line}: t() is missing its second argument`);
    const second = readQuoted(source, skipSpace(source, comma + 1));
    if (!second) throw new Error(`client.js:${line}: t() second argument is not a string literal`);
    const afterSecond = skipSpace(source, second.end);
    // a multi-line t( ... ) call may carry a trailing comma after the last argument
    const close = source[afterSecond] === ',' ? skipSpace(source, afterSecond + 1) : afterSecond;
    if (source[close] !== ')') throw new Error(`client.js:${line}: t() call has more than two arguments`);
    pairs.push({ en: first.value, zh: second.value, start: index, end: close + 1, line });
  }
  return pairs;
}

function maskRanges(source, ranges) {
  const chars = source.split('');
  for (const { start, end } of ranges) {
    for (let index = start; index < end && index < chars.length; index += 1) {
      if (chars[index] !== '\n') chars[index] = ' ';
    }
  }
  return chars.join('');
}

// ------------------------------------------------------------------- suite
const suite = new Suite('ui', 'dsh-mqtt-tools UI suite', {
  note:
    'SSR + jsdom interaction + static audits of client.js. jsdom stubs EventSource/fetch ' +
    '(documented in ui.test.mjs); react/react-dom/jsdom come from node_modules (.testtools works too). ' +
    'The optional headless-Edge check is skipped when the GUI app shell needs a token URL.',
});

function renderSSR(components, name, props) {
  try {
    return ssr(components[name], props);
  } catch (error) {
    throw new Error(`render of ${name} threw: ${error?.stack ?? error}`);
  }
}

await runSuite(suite, async () => {
  await bootstrap();
  resetStubs();

  // ---------------------------------------------------------------- SSR
  suite.group('SSR: panel states through __components (zh)');
  {
    const components = loadPlugin('zh-CN').__components;
    if (!components) throw new Error('client.js did not expose __components even though __DSH_MQTT_TEST__ was true');

    await suite.test('Panel: loading state', () => {
      const html = renderSSR(components, 'Panel', {});
      has(html, 'data-dsh-plugin="mqtt"', 'loading panel');
      has(html, '正在加载 MQTT…', 'loading panel');
      has(html, '<style>', 'loading panel');
      has(html, '.mqtt-btn.primary', 'loading panel');
      notHas(html, 'class="mqtt-tabs"', 'loading panel (tabs need state)');
      return `${html.length} bytes`;
    });

    await suite.test('ConnectionsTab: empty list and empty pane', () => {
      const html = renderSSR(components, 'ConnectionsTab', {
        state: makeState({ clients: [] }),
        messages: [],
        run: stubRun,
        selectedId: null,
        setSelectedId: () => {},
      });
      has(html, '还没有连接', 'empty connections');
      has(html, '新建一个到 MQTT broker 的连接开始使用。', 'empty pane');
      has(html, 'mqtt-list-empty', 'empty connections');
      has(html, 'title="新建连接"', 'new connection button');
      return 'list=0 selected=none';
    });

    await suite.test('ConnectionsTab: selected profile renders the detail form', () => {
      const html = renderSSR(components, 'ConnectionsTab', {
        state: makeState(),
        messages: makeMessages(),
        run: stubRun,
        selectedId: 'c1',
        setSelectedId: () => {},
      });
      has(html, 'Local broker', 'selected profile');
      has(html, 'mqtt://127.0.0.1:1883', 'selected profile');
      has(html, '已连接', 'status badge');
      // A live connection opens with the drawer folded, so its form starts hidden:
      // the button that reopens it must be present, and the form itself must not.
      has(html, 'title="连接设置"', 'settings toggle');
      notHas(html, '>保存<', 'folded drawer hides the save button');
      notHas(html, '>还原<', 'folded drawer hides the revert button');
      notHas(html, '>删除<', 'folded drawer hides the delete button');
      has(html, '>断开<', 'disconnect button');
      has(html, '订阅', 'subscriptions section');
      has(html, 'sensors/#', 'subscription row');
      has(html, '>发送<', 'composer send button');
      notHas(html, '请先连接', 'online connection has no publisher disabled notice');
      has(html, 'mqtt-log-row', 'connection message log');
      if (countOf(html, /class="mqtt-log-row mqtt-msg/g) !== 3) {
        throw new Error(`connection log rows: expected 3 client messages, got ${countOf(html, /class="mqtt-log-row mqtt-msg/g)}`);
      }
      return 'rows=3 subscriptions=2 drawer=folded';
    });

    await suite.test('ClientPane: error banner and refused publisher', () => {
      const profile = clientsFixture()[1];
      const html = renderSSR(components, 'ClientPane', { profile, messages: [], run: stubRun });
      has(html, 'mqtt-banner err', 'error banner');
      has(html, 'ECONNREFUSED', 'error text');
      has(html, '出错', 'error badge');
      has(html, '>连接<', 'connect button for offline profile');
      has(html, '请先连接', 'publisher disabled notice');
      if (!/<button[^>]*\bdisabled\b[^>]*>发送<\/button>/.test(html)) {
        throw new Error('composer send button must be disabled when disabledReason is set');
      }
      return 'banner + disabled publisher';
    });

    await suite.test('BrokerTab: running broker folds its settings and shows the log', () => {
      const html = renderSSR(components, 'BrokerTab', {
        state: makeState(),
        messages: makeMessages(),
        events: makeEvents(),
        run: stubRun,
        showSystem: false,
      });
      has(html, '内嵌 Broker', 'broker heading');
      has(html, '运行中', 'running badge');
      has(html, '>停止<', 'stop button');
      has(html, '0.0.0.0:1883', 'bound address');
      has(html, '客户端 1', 'client count in the status bar');
      has(html, '设置', 'settings toggle');
      has(html, '设置已修改 — 重启 broker 后生效', 'restart hint banner');
      has(html, 'broker ready', 'broker log payload');
      // A running server shows messages first; the configuration is one click away.
      notHas(html, '监听设置', 'running broker starts with the settings folded');
      notHas(html, '保存设置', 'folded drawer hides the save button too');
      return 'status bar + folded settings + log';
    });

    await suite.test('BrokerTab: stopped broker opens the settings drawer and blocks the publisher', () => {
      const state = makeState({
        broker: brokerFixture({ status: 'stopped', startedAt: null, boundPort: null, clients: [], clientCount: 0 }),
      });
      const html = renderSSR(components, 'BrokerTab', {
        state,
        messages: [],
        events: [],
        run: stubRun,
        showSystem: false,
      });
      has(html, '本地 MQTT 服务端当前未启动', 'stopped hint');
      has(html, '>启动<', 'start button');
      has(html, '收起设置', 'drawer starts open while the broker is stopped');
      has(html, '监听设置', 'listener section');
      has(html, '协议版本', 'protocol section');
      has(html, 'MQTT 3.1.1', 'protocol checkbox');
      has(html, '在线客户端 (0)', 'client count');
      has(html, '暂无客户端连接', 'no clients');
      has(html, 'Broker 事件', 'events section');
      has(html, '保存设置', 'save settings button');
      has(html, '显示 $SYS 消息', 'show system toggle');
      has(html, '＋ 添加用户', 'add user control');
      // The user list only gates CONNECT with anonymous access switched off.
      has(html, '用户名单在 CONNECT 时校验', 'user list explanation');
      has(html, '请先启动 broker', 'published blocked hint');
      if (!/<button[^>]*\bdisabled\b[^>]*>发布<\/button>/.test(html)) {
        throw new Error('publisher button must be disabled while the broker is stopped');
      }
      return 'start + open settings + disabled publisher';
    });

    await suite.test('BrokerTab: broker error banner', () => {
      const state = makeState({
        broker: brokerFixture({ status: 'error', error: 'EADDRINUSE', restartHint: false }),
      });
      const html = renderSSR(components, 'BrokerTab', {
        state,
        messages: [],
        events: [],
        run: stubRun,
        showSystem: false,
      });
      has(html, 'EADDRINUSE', 'broker error text');
      has(html, '出错', 'error badge');
      has(html, '>启动<', 'start button after error');
      // An errored server is not running, so its configuration is unfolded.
      has(html, '监听设置', 'settings stay reachable after an error');
      return 'error banner + start';
    });

    await suite.test('MessageLog: rows, meta, empty state and 100-row window', () => {
      const rows = renderSSR(components, 'MessageLog', { entries: makeEntries(3) });
      if (countOf(rows, /class="mqtt-log-row mqtt-msg/g) !== 3) throw new Error('expected 3 log rows');
      has(rows, `QoS 0`, 'qos badge');
      has(rows, 'RETAIN', 'retain badge');
      has(rows, '1 B', 'payload size');
      has(rows, '3 / 3', 'counter');
      notHas(rows, '更早', 'no older button at 3 rows');
      // emptyText wins over the default
      const empty = renderSSR(components, 'MessageLog', { entries: [], emptyText: '自定义空状态' });
      has(empty, '自定义空状态', 'custom empty text');
      notHas(empty, '还没有消息', 'default empty text must be overridden');
      // 250 rows -> newest 100 are rendered, older button carries the rest
      const big = renderSSR(components, 'MessageLog', { entries: makeEntries(250) });
      if (countOf(big, /class="mqtt-log-row mqtt-msg/g) !== 100) throw new Error('expected the newest 100 rows at 250 entries');
      has(big, '更早 (150)', 'older button');
      has(big, '100 / 250', 'counter');
      has(big, 'sensors/150', 'newest page starts at entry 150');
      return 'rows 0/3/100, window=100';
    });

    await suite.test('Publisher: fields, and disabledReason disables the button', () => {
      const plain = renderSSR(components, 'Publisher', { onPublish: stubRun });
      has(plain, '主题', 'topic label');
      has(plain, '消息内容', 'payload label');
      has(plain, 'sensors/room-1/temperature', 'topic placeholder');
      has(plain, '文本或 JSON', 'payload placeholder');
      has(plain, '保留消息 (retain)', 'retain label');
      has(plain, '>发布<', 'publish button');
      notHas(plain, '请先连接', 'no disabled notice');
      const blocked = renderSSR(components, 'Publisher', { onPublish: stubRun, disabledReason: '请先连接' });
      has(blocked, '请先连接', 'disabled notice');
      return 'fields + disabled reason';
    });

    await suite.test('PanelIcon: entry marker, viewBox, size, aria-hidden', () => {
      const html = renderSSR(components, 'PanelIcon', { size: 22 });
      has(html, 'data-dsh-panel-entry="mqtt"', 'icon marker');
      has(html, 'viewBox="0 0 16 16"', 'icon viewBox');
      has(html, 'width="22"', 'icon width');
      has(html, 'aria-hidden="true"', 'icon aria');
      if (countOf(html, /<path /g) !== 4) throw new Error('expected 4 icon paths');
      return 'svg + 4 paths';
    });

    await suite.test('SSR matrix: no component/state/language combination throws', () => {
      const cases = [
        ['Panel', {}],
        ['ConnectionsTab', { state: makeState({ clients: [] }), messages: [], run: stubRun, selectedId: null, setSelectedId: () => {} }],
        ['ConnectionsTab', { state: makeState(), messages: [], run: stubRun, selectedId: null, setSelectedId: () => {} }],
        ['ConnectionsTab', { state: makeState(), messages: makeMessages(), run: stubRun, selectedId: 'c1', setSelectedId: () => {} }],
        ['ClientPane', { profile: clientsFixture()[0], messages: makeMessages(), run: stubRun }],
        ['ClientPane', { profile: clientsFixture()[1], messages: [], run: stubRun }],
        ['BrokerTab', { state: makeState(), messages: makeMessages(), events: makeEvents(), run: stubRun, showSystem: false }],
        ['BrokerTab', { state: makeState({ broker: brokerFixture({ status: 'stopped', startedAt: null, clients: [], clientCount: 0 }) }), messages: [], events: [], run: stubRun, showSystem: true }],
        ['BrokerTab', { state: makeState({ broker: brokerFixture({ status: 'error', error: 'EADDRINUSE' }) }), messages: [], events: [], run: stubRun, showSystem: false }],
        ['MessageLog', { entries: [] }],
        ['MessageLog', { entries: makeEntries(3) }],
        ['MessageLog', { entries: makeEntries(101) }],
        ['Publisher', { onPublish: stubRun }],
        ['Publisher', { onPublish: stubRun, disabledReason: '请先启动 broker' }],
        ['PanelIcon', { size: 16 }],
      ];
      const failures = [];
      let renders = 0;
      for (const lang of ['zh-CN', 'en-US']) {
        const components = loadPlugin(lang).__components;
        for (const [name, props] of cases) {
          try {
            const html = ssr(components[name], props);
            if (typeof html !== 'string') throw new Error('render did not return a string');
            renders += 1;
          } catch (error) {
            failures.push(`${lang} ${name}: ${error?.message ?? error}`);
          }
        }
      }
      if (failures.length > 0) throw new Error(`${failures.length}/${cases.length * 2} renders threw: ${failures.slice(0, 3).join(' | ')}`);
      return `${renders} renders across 2 languages`;
    });

    await suite.test('en locale: English labels and no Chinese leakage', () => {
      const en = loadPlugin('en-US').__components;
      const loading = renderSSR(en, 'Panel', {});
      has(loading, 'loading MQTT…', 'en loading');
      const connections = renderSSR(en, 'ConnectionsTab', {
        state: makeState({ clients: [] }),
        messages: [],
        run: stubRun,
        selectedId: null,
        setSelectedId: () => {},
      });
      has(connections, 'no connections yet', 'en empty list');
      has(connections, 'create a connection to an MQTT broker to start.', 'en empty pane');
      const broker = renderSSR(en, 'BrokerTab', {
        // A stopped server unfolds its settings, which is where the labels live.
        state: makeState({
          broker: brokerFixture({ status: 'stopped', startedAt: null, boundPort: null, clients: [], clientCount: 0 }),
        }),
        messages: makeMessages(),
        events: makeEvents(),
        run: stubRun,
        showSystem: false,
      });
      has(broker, 'Embedded broker', 'en broker heading');
      has(broker, 'hide settings', 'en collapsible drawer');
      has(broker, 'listener', 'en listener');
      has(broker, 'protocol versions', 'en protocols');
      has(broker, 'connected clients (0)', 'en clients');
      has(broker, 'broker events', 'en events');
      const log = renderSSR(en, 'MessageLog', { entries: makeEntries(250) });
      has(log, 'older (150)', 'en older button');
      notHas(log, '更早', 'en log must not show Chinese');
      const combined = loading + connections + broker + log;
      if (CJK.test(combined)) {
        const lines = combined.split('\n').filter((line) => CJK.test(line));
        throw new Error(`en SSR leaked Chinese text: ${lines.slice(0, 3).join(' | ').slice(0, 300)}`);
      }
      return '4 components, no CJK';
    });
  }

  // ------------------------------------------------------ jsdom interaction
  suite.group('jsdom: real clicks and DOM assertions');

  await suite.test('Panel: hello frame, tab switch, lastTab survives remount', async () => {
    resetStubs();
    const { Panel } = loadPlugin('zh-CN').__components;
    globalThis.__SSE_HELLO__ = { state: makeState(), messages: makeMessages(), events: makeEvents() };
    const harness = makeHarness();
    try {
      await harness.render(h(Panel, {}));
      if (!harness.q('.mqtt-empty')?.textContent.includes('正在加载 MQTT')) {
        throw new Error('Panel must show the loading text until the first SSE frame');
      }
      if (StubEventSource.instances.length !== 1) throw new Error('Panel must open exactly one EventSource');
      if (StubEventSource.instances[0].url !== '/dsh-mqtt-tools/api/events') {
        throw new Error(`EventSource url ${StubEventSource.instances[0].url}, expected /dsh-mqtt-tools/api/events`);
      }
      // The stub does not auto-connect: the first frame is delivered explicitly.
      await harness.act(() => StubEventSource.instances[0].open());
      const tabs = harness.qa('.mqtt-tab');
      if (tabs.length !== 2) throw new Error(`expected 2 tabs, got ${tabs.length}`);
      if (!tabs[0].classList.contains('active')) throw new Error('connections tab must start active');
      if (!harness.q('.mqtt-live')?.textContent.includes('实时')) throw new Error('live indicator must switch to 实时 after hello');
      if (!harness.q('.mqtt-list')) throw new Error('connections list missing after hello');
      await harness.act(() => tabs[1].click());
      if (!tabs[1].classList.contains('active')) throw new Error('broker tab must be active after clicking 服务端');
      if (!harness.container.textContent.includes('内嵌 Broker')) throw new Error('broker content missing after switch');
      if (harness.q('.mqtt-list')) throw new Error('connections list must not render on the broker tab');
      await harness.unmount();
      // remount the same plugin instance: module-scope lastTab must be 'broker'
      const second = makeHarness();
      try {
        await second.render(h(Panel, {}));
        if (StubEventSource.instances.length !== 2) throw new Error(`remount must open a new EventSource (instances=${StubEventSource.instances.length})`);
        await second.act(() => StubEventSource.instances[1].open());
        const again = second.qa('.mqtt-tab');
        if (!again[1].classList.contains('active')) throw new Error('lastTab persistence: expected 服务端 still active after remount');
        if (!second.container.textContent.includes('内嵌 Broker')) throw new Error('lastTab persistence: broker content missing after remount');
      } finally {
        await second.dispose();
      }
      return 'loading -> hello -> broker -> remount keeps 服务端';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('Panel: a scoped clear leaves the other ring alone', async () => {
    resetStubs();
    const { Panel } = loadPlugin('zh-CN').__components;
    globalThis.__SSE_HELLO__ = { state: makeState(), messages: makeMessages(), events: makeEvents() };
    const harness = makeHarness();
    try {
      await harness.render(h(Panel, {}));
      await harness.act(() => StubEventSource.instances[0].open());
      const tabs = harness.qa('.mqtt-tab');
      await harness.act(() => tabs[1].click());
      // A running broker opens folded: the event list lives in the drawer.
      const settingsButton = harness.qa('button').find((button) => button.textContent.trim() === '设置');
      if (!settingsButton) throw new Error('settings button missing on the broker bar');
      await harness.act(() => settingsButton.click());
      const text = () => harness.container.textContent;
      if (!text().includes('aedes/start')) throw new Error('broker message missing after hello');
      if (!text().includes('client.connect')) throw new Error('broker event missing after hello');
      // Clearing the events ring must not touch the message log.
      await harness.act(() => StubEventSource.instances[0].emit('cleared', { kind: 'events' }));
      if (text().includes('client.connect')) throw new Error('an events-only clear left the event row in place');
      if (!text().includes('aedes/start')) throw new Error('an events-only clear removed the message log');
      // Clearing the message ring must not touch the events ring.
      await harness.act(() =>
        StubEventSource.instances[0].emit('broker-event', { seq: 9, ts: FIXTURE_TICK + 4000, action: 'client.disconnect', clientId: 'mqttjs_abc', text: 'gone' }),
      );
      await harness.flush(120);
      await harness.act(() => StubEventSource.instances[0].emit('cleared', { kind: 'messages' }));
      if (text().includes('aedes/start')) throw new Error('a messages-only clear left the message row in place');
      if (!text().includes('client.disconnect')) throw new Error('a messages-only clear removed broker events');
      // A host that predates scoped clears sends an empty frame: both rings go.
      await harness.act(() => StubEventSource.instances[0].emit('cleared', {}));
      if (text().includes('client.disconnect')) throw new Error('a legacy cleared frame did not clear the events ring');
      return 'messages-only / events-only / legacy both';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('MessageLog: pagination boundaries 0 / 100 / 101 / 250', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;

    async function mountLog(count) {
      const harness = makeHarness();
      await harness.render(h(components.MessageLog, { entries: makeEntries(count) }));
      return harness;
    }

    const cases = [];
    try {
      let harness = await mountLog(0);
      try {
        const rows = harness.qa('.mqtt-log-row').length;
        if (rows !== 0) throw new Error(`0 entries: ${rows} rows`);
        if (!harness.q('.mqtt-empty')?.textContent.includes('还没有消息')) throw new Error('0 entries: default empty text missing');
        if (harness.q('.mqtt-log-count')?.textContent !== '0 / 0') throw new Error('0 entries: counter mismatch');
        if (buttonByText(harness, '更早')) throw new Error('0 entries: older button must not render');
        cases.push('0');
      } finally {
        await harness.dispose();
      }

      harness = await mountLog(100);
      try {
        const rows = harness.qa('.mqtt-log-row').length;
        if (rows !== 100) throw new Error(`100 entries: ${rows} rows`);
        if (buttonByText(harness, '更早')) throw new Error('100 entries: older button must not render');
        if (harness.q('.mqtt-log-count')?.textContent !== '100 / 100') throw new Error('100 entries: counter mismatch');
        cases.push('100');
      } finally {
        await harness.dispose();
      }

      harness = await mountLog(101);
      try {
        if (harness.qa('.mqtt-log-row').length !== 100) throw new Error('101 entries: expected 100 visible rows');
        const older = buttonByText(harness, '更早');
        if (!older) throw new Error('101 entries: older button missing');
        if (older.textContent.trim() !== '更早 (1)') throw new Error(`101 entries: older text ${JSON.stringify(older.textContent)}`);
        if (harness.q('.mqtt-log-count')?.textContent !== '100 / 101') throw new Error('101 entries: counter mismatch');
        cases.push('101');
      } finally {
        await harness.dispose();
      }

      harness = await mountLog(250);
      try {
        if (harness.qa('.mqtt-log-row').length !== 100) throw new Error('250 entries: expected 100 visible rows');
        if (harness.qa('.mqtt-msg-topic')[0].textContent !== 'sensors/150') throw new Error('250 entries: newest page must start at sensors/150');
        const firstOlder = buttonByText(harness, '更早');
        if (firstOlder.textContent.trim() !== '更早 (150)') throw new Error(`250 entries: older text ${JSON.stringify(firstOlder.textContent)}`);
        if (harness.q('.mqtt-log-count')?.textContent !== '100 / 250') throw new Error('250 entries: counter mismatch');
        await harness.act(() => firstOlder.click());
        if (harness.qa('.mqtt-log-row').length !== 200) throw new Error('250 entries: one page back must show 200 rows');
        if (buttonByText(harness, '更早').textContent.trim() !== '更早 (50)') throw new Error('250 entries: second page older count mismatch');
        if (harness.q('.mqtt-log-count')?.textContent !== '200 / 250') throw new Error('250 entries: second counter mismatch');
        await harness.act(() => buttonByText(harness, '更早').click());
        if (harness.qa('.mqtt-log-row').length !== 250) throw new Error('250 entries: last page back must show all 250 rows');
        if (buttonByText(harness, '更早')) throw new Error('250 entries: older button must disappear once everything is shown');
        if (harness.q('.mqtt-log-count')?.textContent !== '250 / 250') throw new Error('250 entries: final counter mismatch');
        cases.push('250');
      } finally {
        await harness.dispose();
      }
      return `pages verified: ${cases.join(', ')}`;
    } catch (error) {
      throw error;
    }
  });

  await suite.test('MessageLog: filter and direction reset the page limit', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const harness = makeHarness();
    try {
      await harness.render(h(components.MessageLog, { entries: makeEntries(250) }));
      if (harness.qa('.mqtt-log-row').length !== 100) throw new Error('initial page must be 100 rows');
      await harness.act(() => buttonByText(harness, '更早').click());
      if (harness.qa('.mqtt-log-row').length !== 200) throw new Error('one page back must show 200 rows');

      const filterInput = harness.q('.mqtt-log-bar input.mqtt-input');
      await harness.act(() => setInputValue(harness, filterInput, 'sensors/1'));
      if (harness.qa('.mqtt-log-row').length !== 100) throw new Error('filter change must reset the visible limit to 100');
      if (harness.q('.mqtt-log-count')?.textContent !== '100 / 111 · 250 总计') {
        throw new Error(`filter counter ${JSON.stringify(harness.q('.mqtt-log-count')?.textContent)}`);
      }
      const filterOlder = buttonByText(harness, '更早');
      if (!filterOlder || filterOlder.textContent.trim() !== '更早 (11)') throw new Error('filter older button mismatch');
      for (const topic of harness.qa('.mqtt-msg-topic')) {
        if (!topic.textContent.includes('sensors/1')) throw new Error(`filtered row ${topic.textContent} does not match`);
      }

      await harness.act(() => setInputValue(harness, filterInput, ''));
      if (harness.q('.mqtt-log-count')?.textContent !== '100 / 250') throw new Error('clearing the filter must show 100 / 250');

      const directionSelect = harness.q('.mqtt-log-bar select.mqtt-select');
      await harness.act(() => setSelectValue(directionSelect, 'in'));
      if (harness.qa('.mqtt-log-row').length !== 100) throw new Error('direction change must reset the visible limit to 100');
      if (harness.q('.mqtt-log-count')?.textContent !== '100 / 125 · 250 总计') {
        throw new Error(`direction counter ${JSON.stringify(harness.q('.mqtt-log-count')?.textContent)}`);
      }
      if (harness.qa('.mqtt-msg.in').length !== 100) throw new Error('incoming direction must render only ↓ rows');
      if (harness.qa('.mqtt-msg.out').length !== 0) throw new Error('incoming direction must not render any ↑ rows');

      await harness.act(() => setInputValue(harness, filterInput, 'definitely-no-match'));
      if (harness.q('.mqtt-log-count')?.textContent !== '0 / 0 · 250 总计') {
        throw new Error(`no-match counter ${JSON.stringify(harness.q('.mqtt-log-count')?.textContent)}, expected "0 / 0 · 250 总计"`);
      }
      if (!harness.q('.mqtt-empty')?.textContent.includes('还没有消息')) throw new Error('no-match empty text missing');
      return 'limit reset by filter and direction';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('BrokerTab: protocol draft re-seed, last-version guard and save payload', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const calls = [];
    const run = async (cmd, params = {}) => {
      calls.push({ cmd, params });
      return {};
    };
    const renderBroker = async (harness, broker) => {
      await harness.render(
        h(components.BrokerTab, {
          state: makeState({ broker }),
          messages: [],
          events: [],
          run,
          showSystem: false,
        }),
      );
    };
    const harness = makeHarness();
    // A running broker starts with the drawer folded; its controls live inside.
    const openSettings = async () => {
      const toggle = harness.qa('button').find((button) => button.textContent.trim() === '设置');
      if (toggle) await harness.act(() => toggle.click());
    };
    try {
      // only MQTT 3.1.1 enabled: it must not be possible to switch it off
      await renderBroker(harness, brokerFixture({ protocols: ['3.1.1'], restartHint: false, users: [] }));
      await openSettings();
      let boxes = protocolInputs(harness);
      if (boxes.length !== 3) throw new Error(`expected 3 protocol checkboxes, got ${boxes.length}`);
      eqStates(checkedStates(boxes), [false, true, false], 'initial protocols');
      await harness.act(() => boxes[1].click());
      boxes = protocolInputs(harness);
      eqStates(checkedStates(boxes), [false, true, false], 'last protocol cannot be unchecked');

      // enabling a second version, then removing the first, is allowed
      await harness.act(() => boxes[0].click());
      eqStates(checkedStates(protocolInputs(harness)), [true, true, false], 'enable 3.1');
      await harness.act(() => protocolInputs(harness)[1].click());
      eqStates(checkedStates(protocolInputs(harness)), [true, false, false], 'remove 3.1.1 while two are enabled');

      // the host pushes changed persisted settings -> re-seed from them
      await renderBroker(harness, brokerFixture({ protocols: ['3.1', '5.0'], restartHint: false, users: [] }));
      eqStates(checkedStates(protocolInputs(harness)), [true, false, true], 're-seed after host protocol push');

      // a local edit, then an unrelated state push (status/count only): draft must survive
      await harness.act(() => protocolInputs(harness)[0].click());
      eqStates(checkedStates(protocolInputs(harness)), [false, false, true], 'local edit before unrelated push');
      await renderBroker(
        harness,
        brokerFixture({ protocols: ['3.1', '5.0'], status: 'stopped', startedAt: null, clientCount: 0, clients: [], restartHint: false, users: [] }),
      );
      eqStates(checkedStates(protocolInputs(harness)), [false, false, true], 'unrelated push must keep the local draft');

      // saving must send the draft, not the host state
      const saveButton = harness.qa('button').find((button) => button.textContent.trim() === '保存设置');
      if (!saveButton) throw new Error('保存设置 button missing');
      await harness.act(() => saveButton.click());
      await harness.flush();
      const saveCall = calls.find((call) => call.cmd === 'broker.save');
      if (!saveCall) throw new Error(`broker.save was not sent (calls: ${calls.map((call) => call.cmd).join(', ')})`);
      const saved = saveCall.params.broker;
      if (JSON.stringify(saved.protocols) !== JSON.stringify(['5.0'])) {
        throw new Error(`saved protocols ${JSON.stringify(saved.protocols)}, expected ["5.0"]`);
      }
      if (saved.port !== 1883 || saved.host !== '0.0.0.0') throw new Error(`saved listener ${saved.host}:${saved.port}`);
      if (typeof saved.port !== 'number') throw new Error('saved port must be a number');
      if (!harness.container.textContent.includes('已保存 — 重启 broker 后生效')) throw new Error('save toast missing');

      // port changes are persisted settings too: they re-seed the draft
      await renderBroker(harness, brokerFixture({ port: 1885, protocols: ['3.1.1'], restartHint: false, users: [] }));
      const portInput = harness.qa('.mqtt-input').find((input) => input.type === 'number' && input.value === '1885');
      if (!portInput) throw new Error('port input was not re-seeded to 1885');
      eqStates(checkedStates(protocolInputs(harness)), [false, true, false], 're-seed after host port push');
      return 'guard + re-seed + save payload';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('BrokerTab: resource limits render, seed and save', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const calls = [];
    const run = async (cmd, params = {}) => {
      calls.push({ cmd, params });
      return {};
    };
    const harness = makeHarness();
    try {
      const broker = brokerFixture({
        maxConnections: 12,
        connectTimeoutMs: 5000,
        maxPayloadBytes: 262144,
        maxRetainedMessages: 50,
        restartHint: false,
        users: [],
      });
      await harness.render(
        h(components.BrokerTab, { state: makeState({ broker }), messages: [], events: [], run, showSystem: false }),
      );
      // The drawer is folded while the broker runs; unfold it for the controls.
      const settingsToggle = harness.qa('button').find((button) => button.textContent.trim() === '设置');
      if (!settingsToggle) throw new Error('设置 toggle missing');
      await harness.act(() => settingsToggle.click());
      if (!harness.container.textContent.includes('资源上限')) throw new Error('resource limits section missing');
      const numbers = harness.qa('input[type="number"]').map((input) => input.value);
      for (const expected of ['12', '5000', '262144', '50']) {
        if (!numbers.includes(expected)) throw new Error(`limit input ${expected} not seeded (got ${numbers.join(',')})`);
      }
      const saveButton = harness.qa('button').find((button) => button.textContent.trim() === '保存设置');
      await harness.act(() => saveButton.click());
      await harness.flush();
      const saveCall = calls.find((call) => call.cmd === 'broker.save');
      const saved = saveCall.params.broker;
      if (saved.maxConnections !== 12 || saved.connectTimeoutMs !== 5000) throw new Error('connection limits not saved');
      if (saved.maxPayloadBytes !== 262144 || saved.maxRetainedMessages !== 50) throw new Error('payload/retained limits not saved');
      return 'limits seeded and saved';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('BrokerTab: the settings drawer folds and unfolds', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const harness = makeHarness();
    try {
      const broker = brokerFixture({ status: 'stopped', startedAt: null, boundPort: null, clients: [], clientCount: 0 });
      await harness.render(
        h(components.BrokerTab, { state: makeState({ broker }), messages: [], events: [], run: stubRun, showSystem: false }),
      );
      if (!harness.container.textContent.includes('监听设置')) throw new Error('a stopped broker must open its settings');
      const fold = buttonByText(harness, '收起设置');
      if (!fold) throw new Error('收起设置 toggle missing');
      await harness.act(() => fold.click());
      if (harness.container.textContent.includes('监听设置')) throw new Error('folding must hide the listener section');
      const unfold = buttonByText(harness, '设置');
      if (!unfold) throw new Error('the folded drawer must offer a way back');
      await harness.act(() => unfold.click());
      if (!harness.container.textContent.includes('监听设置')) throw new Error('unfolding must bring the listener section back');
      return 'open -> folded -> open';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('BrokerTab: starting the broker saves the draft first', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const calls = [];
    const run = async (cmd, params = {}) => {
      calls.push({ cmd, params });
      return {};
    };
    const harness = makeHarness();
    try {
      const broker = brokerFixture({ status: 'stopped', startedAt: null, boundPort: null, clients: [], clientCount: 0, port: 1883 });
      await harness.render(
        h(components.BrokerTab, { state: makeState({ broker }), messages: [], events: [], run, showSystem: false }),
      );
      const portInput = harness.qa('input[type="number"]').find((input) => input.value === '1883');
      if (!portInput) throw new Error('port input missing');
      setInputValue(harness, portInput, '1884');
      await harness.flush();
      const startButton = buttonByText(harness, '启动');
      if (!startButton) throw new Error('start button missing');
      await harness.act(() => startButton.click());
      await harness.flush();
      if (calls.length < 2) throw new Error(`start must save then start, got ${JSON.stringify(calls.map((call) => call.cmd))}`);
      if (calls[0].cmd !== 'broker.save') throw new Error(`first command must be broker.save, got ${calls[0].cmd}`);
      if (Number(calls[0].params.broker.port) !== 1884) throw new Error(`the draft port was not saved (${calls[0].params.broker.port})`);
      if (calls[1].cmd !== 'broker.start') throw new Error(`second command must be broker.start, got ${calls[1].cmd}`);
      // Started: the drawer folds away so the log and the send card get the room.
      if (harness.container.textContent.includes('监听设置')) throw new Error('starting must fold the settings away');
      return 'broker.save(1884) -> broker.start -> folded';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('a11y: tabs, log region and resize separator expose roles', () => {
    const components = loadPlugin('zh-CN').__components;
    const state = makeState({ clients: clientsFixture(), broker: brokerFixture() });
    const clients = renderSSR(components, 'ConnectionsTab', { state, messages: makeMessages(), run: stubRun, selectedId: 'c1', setSelectedId: () => {} });
    has(clients, 'role="separator"', 'resize handle role');
    has(clients, 'aria-valuenow', 'resize handle value');
    const log = renderSSR(components, 'MessageLog', { entries: makeMessages(), onClear: () => {} });
    has(log, 'role="log"', 'message log region');
    return 'separator + log roles';
  });

  await suite.test('composer: message box resizes by drag and arrow keys', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const harness = makeHarness();
    try {
      await harness.render(h(components.ClientPane, { profile: clientsFixture()[0], messages: [], run: stubRun }));
      await harness.flush();
      const grip = harness.q('.mqtt-composer-grip');
      if (!grip) throw new Error('composer resize grip missing');
      const start = Number(grip.getAttribute('aria-valuenow'));
      if (!Number.isFinite(start) || start <= 0) throw new Error(`grip needs an aria-valuenow, got ${grip.getAttribute('aria-valuenow')}`);
      await harness.act(() => {
        grip.dispatchEvent(new dom.window.MouseEvent('mousedown', { bubbles: true, clientY: 600, button: 0 }));
      });
      await harness.act(() => {
        dom.window.dispatchEvent(new dom.window.MouseEvent('mousemove', { clientY: 500 }));
        dom.window.dispatchEvent(new dom.window.MouseEvent('mouseup', { clientY: 500 }));
      });
      const grown = Number(harness.q('.mqtt-composer-grip').getAttribute('aria-valuenow'));
      if (grown !== start + 100) throw new Error(`dragging up 100px should grow the box (${start} -> ${grown})`);
      await harness.act(() => {
        harness.q('.mqtt-composer-grip').dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
      });
      const shrunk = Number(harness.q('.mqtt-composer-grip').getAttribute('aria-valuenow'));
      if (shrunk !== grown - 16) throw new Error(`ArrowDown should shave 16px (${grown} -> ${shrunk})`);
      const textarea = harness.q('.mqtt-composer textarea');
      if (!textarea) throw new Error('composer textarea missing');
      if (textarea.style.height !== `${shrunk}px`) {
        throw new Error(`textarea height should follow the grip (${textarea.style.height} vs ${shrunk}px)`);
      }
      return `grip ${start}px -> ${grown}px -> ${shrunk}px`;
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('composer: Enter sends, Shift+Enter and IME keep the draft', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const calls = [];
    const run = async (cmd, params = {}) => {
      calls.push({ cmd, params });
      return {};
    };
    const harness = makeHarness();
    const press = (element, init, extra) =>
      harness.act(() => {
        const event = new dom.window.KeyboardEvent('keydown', { key: init.key, bubbles: true, cancelable: true, ...init });
        // isComposing is a getter on KeyboardEvent in jsdom, so it needs defineProperty
        if (extra) for (const [key, value] of Object.entries(extra)) Object.defineProperty(event, key, { value, configurable: true });
        element.dispatchEvent(event);
        return null;
      });
    try {
      await harness.render(h(components.ClientPane, { profile: clientsFixture()[0], messages: [], run }));
      await harness.flush();
      const topic = harness.q('.mqtt-composer-topic input');
      const textarea = harness.q('.mqtt-composer textarea');
      if (!topic || !textarea) throw new Error('composer topic or message box missing');
      setInputValue(harness, textarea, 'hello');
      await harness.flush();

      // no topic yet: the send button is disabled and Enter must not publish —
      // it points at the topic field instead of doing nothing
      const sendButton = buttonByText(harness, '发送');
      if (!sendButton) throw new Error('composer send button missing');
      if (!sendButton.disabled) throw new Error('send must stay disabled until a topic is set');
      await press(textarea, { key: 'Enter' });
      await harness.flush(10);
      if (calls.length !== 0) throw new Error(`Enter without a topic published anyway: ${JSON.stringify(calls)}`);
      if (dom.window.document.activeElement !== topic) throw new Error('Enter without a topic must move the focus to the topic field');

      // Shift+Enter asks for a new line, and a composing Enter belongs to the IME
      await press(textarea, { key: 'Enter', shiftKey: true });
      await press(textarea, { key: 'Enter' }, { isComposing: true });
      await harness.flush(10);
      if (calls.length !== 0) throw new Error(`Shift+Enter / IME Enter must not publish: ${JSON.stringify(calls)}`);
      if (textarea.value !== 'hello') throw new Error(`the draft must survive Shift+Enter (got ${JSON.stringify(textarea.value)})`);

      // with a topic, Enter publishes exactly what the card shows and clears the box
      setInputValue(harness, topic, 'sensors/room-1/temp');
      await harness.flush();
      if (buttonByText(harness, '发送').disabled) throw new Error('send must enable once a topic is set');
      await press(textarea, { key: 'Enter' });
      await harness.flush(10);
      const publish = calls.find((call) => call.cmd === 'client.publish');
      if (!publish) throw new Error(`Enter must publish, calls=${JSON.stringify(calls)}`);
      const params = publish.params;
      if (params.topic !== 'sensors/room-1/temp' || params.payload !== 'hello') {
        throw new Error(`publish carried ${JSON.stringify(params)}`);
      }
      if (params.id !== 'c1' || params.qos !== 0 || params.retain !== false) {
        throw new Error(`publish must carry the connection and the option chips: ${JSON.stringify(params)}`);
      }
      if (textarea.value !== '') throw new Error(`the box must be cleared after sending (got ${JSON.stringify(textarea.value)})`);
      if (dom.window.document.activeElement !== textarea) throw new Error('the focus must stay in the box for the next message');

      // caret on the first line: ArrowUp hops to the topic and selects it
      textarea.selectionStart = 0;
      textarea.selectionEnd = 0;
      await press(textarea, { key: 'ArrowUp' });
      if (dom.window.document.activeElement !== topic) throw new Error('ArrowUp on the first line must focus the topic');
      if (topic.selectionStart !== 0 || topic.selectionEnd !== topic.value.length) throw new Error('the topic must be selected for a quick overwrite');
      // and ArrowDown returns to the message box
      await press(topic, { key: 'ArrowDown' });
      if (dom.window.document.activeElement !== textarea) throw new Error('ArrowDown must return to the message box');
      return 'topic gate, Shift+Enter, IME, publish params, focus round-trip';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('ClientPane: delete requires a second click', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const calls = [];
    const run = async (cmd, params = {}) => {
      calls.push({ cmd, params });
      return {};
    };
    const harness = makeHarness();
    try {
      await harness.render(h(components.ClientPane, { profile: clientsFixture()[0], messages: [], run }));
      await harness.flush();
      // The settings drawer is collapsible: open it if an earlier test left it closed.
      if (!buttonByText(harness, '删除')) {
        const settingsButton = buttonByText(harness, '设置');
        if (settingsButton) {
          await harness.act(() => settingsButton.click());
          await harness.flush();
        }
      }
      const deleteButton = buttonByText(harness, '删除');
      if (!deleteButton) throw new Error('delete button missing');
      await harness.act(() => deleteButton.click());
      if (calls.length !== 0) throw new Error('first click must not send client.delete');
      const confirmButton = buttonByText(harness, '确认删除');
      if (!confirmButton) throw new Error('second-click confirmation button missing');
      if (!confirmButton.classList.contains('danger')) throw new Error('confirmation button must use the danger variant');
      await harness.act(() => confirmButton.click());
      await harness.flush();
      if (calls.length !== 1 || calls[0].cmd !== 'client.delete') {
        throw new Error(`expected one client.delete, got ${JSON.stringify(calls)}`);
      }
      if (calls[0].params.id !== 'c1') throw new Error('client.delete must carry the profile id');
      if (!harness.container.textContent.includes('已删除')) throw new Error('delete toast missing');
      return 'ghost 删除 -> danger 确认删除 -> client.delete';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('ClientPane: connect saves the edited settings before dialling out', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const calls = [];
    const run = async (cmd, params = {}) => {
      calls.push({ cmd, params });
      return {};
    };
    const harness = makeHarness();
    try {
      const profile = clientsFixture()[1];
      await harness.render(h(components.ClientPane, { profile, messages: [], run }));
      await harness.flush();
      const urlInput = harness
        .qa('.mqtt-settings input.mqtt-input')
        .find((input) => input.value === 'mqtts://broker.example.com:8883');
      if (!urlInput) throw new Error('broker url field missing');
      setInputValue(harness, urlInput, 'mqtt://10.0.0.5:1883');
      await harness.flush();
      const connectButton = buttonByText(harness, '连接');
      if (!connectButton) throw new Error('connect button missing');
      await harness.act(() => connectButton.click());
      await harness.flush();
      const save = calls.find((call) => call.cmd === 'client.save');
      if (!save) throw new Error(`connecting must persist the draft (calls: ${JSON.stringify(calls.map((call) => call.cmd))})`);
      if (save.params.profile.id !== 'c2') throw new Error('client.save must carry the profile id');
      if (save.params.profile.url !== 'mqtt://10.0.0.5:1883') {
        throw new Error(`the edited url was not saved (${save.params.profile.url})`);
      }
      const connect = calls.find((call) => call.cmd === 'client.connect');
      if (!connect || connect.params.id !== 'c2') throw new Error('client.connect must run after the save');
      if (calls.indexOf(save) > calls.indexOf(connect)) throw new Error('the save must happen before the connect');
      return 'client.save(url) -> client.connect';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('ClientPane: connecting re-seeds the form from the saved profile, not the stale one', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const profile = clientsFixture()[1];
    const savedProfile = { ...profile, url: 'mqtt://10.0.0.9:1884' };
    const run = async (cmd) => {
      if (cmd === 'client.save') return savedProfile;
      return {};
    };
    const harness = makeHarness();
    try {
      await harness.render(h(components.ClientPane, { profile, messages: [], run }));
      await harness.flush();
      const urlInput = harness
        .qa('.mqtt-settings input.mqtt-input')
        .find((input) => input.value === 'mqtts://broker.example.com:8883');
      if (!urlInput) throw new Error('broker url field missing');
      setInputValue(harness, urlInput, 'mqtt://10.0.0.9:1884');
      await harness.flush();
      await harness.act(() => buttonByText(harness, '连接').click());
      await harness.flush();
      // Connecting folds the drawer away by design; reopen it the way a user would.
      const settingsButton = buttonByText(harness, '设置');
      if (settingsButton) await harness.act(() => settingsButton.click());
      await harness.flush();
      const reopened = harness
        .qa('.mqtt-settings input.mqtt-input')
        .find((input) => String(input.value).startsWith('mqtt://') || String(input.value).startsWith('mqtts://'));
      if (!reopened) throw new Error('settings drawer did not reopen');
      if (reopened.value !== 'mqtt://10.0.0.9:1884') {
        throw new Error(`the drawer re-seeded a stale url: ${reopened.value}`);
      }
      return `drawer reseeded with ${reopened.value}`;
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('ClientPane: a scoped clear only empties this connection', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const calls = [];
    const run = async (cmd, params = {}) => {
      calls.push({ cmd, params });
      return {};
    };
    const harness = makeHarness();
    try {
      const profile = clientsFixture()[1];
      await harness.render(h(components.ClientPane, { profile, messages: [], run }));
      await harness.flush();
      const clearButton = buttonByText(harness, '清空');
      if (!clearButton) throw new Error('clear button missing');
      await harness.act(() => clearButton.click());
      await harness.flush();
      const clear = calls.find((call) => call.cmd === 'log.clear');
      if (!clear) throw new Error('clear must send log.clear');
      if (clear.params.kind !== 'messages') throw new Error('a message log clears messages, not events');
      if (clear.params.clientId !== 'c2') throw new Error(`clear must name the connection (${JSON.stringify(clear.params)})`);
      return 'log.clear { kind: messages, clientId: c2 }';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('Composer: Enter is left to the browser when sending is impossible', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const harness = makeHarness();
    try {
      const profile = clientsFixture()[1];
      const offline = { ...profile, status: 'disconnected' };
      const run = async () => ({});
      await harness.render(h(components.ClientPane, { profile: offline, messages: [], run }));
      await harness.flush();
      const area = harness.q('.mqtt-composer textarea');
      if (!area) throw new Error('composer textarea missing');
      setInputValue(harness, area, 'kept draft');
      await harness.flush();
      const event = new window.KeyboardEvent('keydown', { key: 'Enter', bubbles: true, cancelable: true });
      await harness.act(() => {
        area.dispatchEvent(event);
      });
      if (event.defaultPrevented) throw new Error('offline Enter must not be swallowed');
      if (area.value !== 'kept draft') throw new Error('the draft must survive an offline Enter');
      return 'offline Enter keeps the newline and the draft';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('BrokerTab: an error is labelled as an error, not as stopped', async () => {
    resetStubs();
    const { Panel } = loadPlugin('zh-CN').__components;
    const state = makeState();
    state.clients = [];
    state.broker = { ...state.broker, status: 'error', error: 'EADDRINUSE: listen 0.0.0.0:1883' };
    globalThis.__SSE_HELLO__ = { state, messages: [], events: [] };
    const harness = makeHarness();
    try {
      await harness.render(h(Panel, {}));
      await harness.act(() => StubEventSource.instances[0].open());
      const badge = harness.qa('.mqtt-badge').find((node) => node.textContent.includes('Broker'));
      if (!badge) throw new Error('broker badge missing');
      if (!badge.textContent.includes('出错')) throw new Error(`broker failure still reads as ${badge.textContent}`);
      if (!String(badge.getAttribute('title') ?? '').includes('EADDRINUSE: listen 0.0.0.0:1883')) throw new Error('the error must be reachable as a tooltip');
      return 'badge says 出错 and carries the host error as its title';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('ClientPane: transport errors are restated in the panel language', () => {
    const components = loadPlugin('zh-CN').__components;
    const profile = { ...clientsFixture()[1], error: 'connect ECONNREFUSED 127.0.0.1:1884' };
    const zh = renderSSR(components, 'ClientPane', { profile, messages: [], run: stubRun });
    has(zh, '连接被拒绝', 'refusal explained in Chinese');
    has(zh, 'ECONNREFUSED', 'the raw error stays reachable');
    const en = loadPlugin('en').__components;
    try {
      const html = renderSSR(en, 'ClientPane', { profile, messages: [], run: stubRun });
      has(html, 'connection refused', 'refusal explained in English');
      notHas(html, '连接被拒绝', 'English panel must not show the Chinese sentence');
    } finally {
      loadPlugin('zh-CN');
    }
    return 'zh + en errors localized, raw text kept';
  });

  await suite.test('BrokerTab: a retained set offers a one-click clear', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const calls = [];
    const run = async (cmd, params = {}) => {
      calls.push({ cmd, params });
      return cmd === 'broker.clearRetained' ? { cleared: 2, topics: ['a', 'b'] } : {};
    };
    const harness = makeHarness();
    try {
      await harness.render(
        h(components.BrokerTab, {
          state: makeState({ broker: brokerFixture({ retainedCount: 2 }) }),
          messages: [],
          events: [],
          run,
          showSystem: false,
        }),
      );
      // A running broker opens folded; the action lives in the drawer.
      await harness.act(() => buttonByText(harness, '设置').click());
      const clear = buttonByText(harness, '保留消息 (2)');
      if (!clear) throw new Error('retained counter/clear button missing from the drawer');
      await harness.act(() => clear.click());
      if (!calls.some((call) => call.cmd === 'broker.clearRetained')) {
        throw new Error('the retained button did not ask the host to clear');
      }
      return 'retained (2) -> broker.clearRetained';
    } finally {
      await harness.dispose();
    }
  });


  await suite.test('Publisher: empty topic is refused, disabledReason blocks, trimmed publish works', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const published = [];
    const onPublish = async (input) => {
      published.push(input);
      return { ok: true };
    };
    const harness = makeHarness();
    try {
      await harness.render(h(components.Publisher, { onPublish }));
      const publishButton = harness.q('.mqtt-btn.primary');
      if (!publishButton) throw new Error('publish button missing');
      if (!publishButton.disabled) throw new Error('publish button must be disabled while the topic is empty');
      await harness.act(() => publishButton.click());
      await harness.flush();
      if (published.length !== 0) throw new Error('empty-topic click must not publish');

      // the same card as the clients' composer: topic line, message box, chips
      const topicInput = harness.q('.mqtt-composer-topic input.mqtt-input');
      const payloadInput = harness.q('.mqtt-composer textarea');
      const qosSelect = harness.q('.mqtt-composer-opts select.mqtt-select');
      const retainBox = harness.q('.mqtt-composer-opts .mqtt-check input[type="checkbox"]');
      if (!topicInput || !payloadInput || !qosSelect || !retainBox) throw new Error('publisher fields missing');
      await harness.act(() => {
        setInputValue(harness, topicInput, '  sensors/room-1/temp  ');
        setInputValue(harness, payloadInput, '{"t":21.5}');
        setSelectValue(qosSelect, '1');
        retainBox.click();
      });
      await harness.act(() => harness.q('.mqtt-btn.primary').click());
      await harness.flush();
      if (published.length !== 1) throw new Error(`expected one publish, got ${published.length}`);
      const payload = published[0];
      if (payload.topic !== 'sensors/room-1/temp') throw new Error(`topic ${JSON.stringify(payload.topic)}, expected trimming`);
      if (payload.payload !== '{"t":21.5}') throw new Error('payload mismatch');
      if (payload.qos !== 1) throw new Error(`qos ${payload.qos}, expected 1`);
      if (payload.retain !== true) throw new Error('retain flag was not forwarded');
      if (!harness.qa('.mqtt-hint').some((hint) => hint.textContent === '已发布')) throw new Error('published toast missing');

      // a disabled publisher (no connection / broker stopped) must not fire at all
      await harness.render(h(components.Publisher, { onPublish, disabledReason: '请先连接' }));
      const disabledButton = harness.q('.mqtt-btn.primary');
      if (!disabledButton.disabled) throw new Error('publisher must be disabled when disabledReason is set');
      await harness.act(() => disabledButton.click());
      await harness.flush();
      if (published.length !== 1) throw new Error('disabled publisher must not publish');
      return 'refused -> trimmed publish -> disabled';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('Publisher: message box resizes by drag and arrow keys', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const harness = makeHarness();
    try {
      await harness.render(h(components.Publisher, { onPublish: async () => ({}) }));
      const grip = harness.q('.mqtt-composer-grip');
      if (!grip) throw new Error('publisher resize grip missing');
      const start = Number(grip.getAttribute('aria-valuenow'));
      if (!Number.isFinite(start) || start <= 0) throw new Error(`grip needs an aria-valuenow, got ${grip.getAttribute('aria-valuenow')}`);
      await harness.act(() => {
        grip.dispatchEvent(new dom.window.MouseEvent('mousedown', { bubbles: true, clientY: 600, button: 0 }));
      });
      await harness.act(() => {
        dom.window.dispatchEvent(new dom.window.MouseEvent('mousemove', { clientY: 500 }));
        dom.window.dispatchEvent(new dom.window.MouseEvent('mouseup', { clientY: 500 }));
      });
      const grown = Number(harness.q('.mqtt-composer-grip').getAttribute('aria-valuenow'));
      if (grown !== start + 100) throw new Error(`dragging up 100px should grow the box (${start} -> ${grown})`);
      await harness.act(() => {
        harness.q('.mqtt-composer-grip').dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
      });
      const shrunk = Number(harness.q('.mqtt-composer-grip').getAttribute('aria-valuenow'));
      if (shrunk !== grown - 16) throw new Error(`ArrowDown should shave 16px (${grown} -> ${shrunk})`);
      const textarea = harness.q('.mqtt-composer textarea');
      if (!textarea) throw new Error('publisher textarea missing');
      if (textarea.style.height !== `${shrunk}px`) {
        throw new Error(`textarea height should follow the grip (${textarea.style.height} vs ${shrunk}px)`);
      }
      // A height the user dragged survives a send; an auto-grown one falls back.
      await harness.act(() => {
        setInputValue(harness, harness.q('.mqtt-composer-topic input.mqtt-input'), 'a/b');
        setInputValue(harness, textarea, 'hello');
      });
      await harness.act(() => harness.q('.mqtt-btn.primary').click());
      await harness.flush();
      const after = Number(harness.q('.mqtt-composer-grip').getAttribute('aria-valuenow'));
      if (after !== shrunk) throw new Error(`a dragged height must survive a send (${shrunk} -> ${after})`);
      return `publisher grip ${start}px -> ${grown}px -> ${shrunk}px`;
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('Publisher requirement: publish is disabled while the topic is empty', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const harness = makeHarness();
    try {
      await harness.render(h(components.Publisher, { onPublish: async () => ({}) }));
      const publishButton = harness.q('.mqtt-btn.primary');
      if (!publishButton.disabled) {
        throw new Error(
          'client.js:487: publish button is enabled with an empty topic; the task requires it to be disabled ' +
            '(the send() guard still refuses, but the control invites a no-op click)',
        );
      }
      return 'disabled while topic empty';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('ConnectionsTab: clicking a row opens the selected profile', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    const state = makeState();
    function Host() {
      const [selectedId, setSelectedId] = React.useState(null);
      return h(components.ConnectionsTab, {
        state,
        messages: [],
        run: stubRun,
        selectedId,
        setSelectedId,
      });
    }
    const harness = makeHarness();
    try {
      await harness.render(h(Host));
      if (harness.q('.mqtt-item.active')) throw new Error('no row must be active before a click');
      const rows = harness.qa('.mqtt-item');
      if (rows.length !== 2) throw new Error(`expected 2 connection rows, got ${rows.length}`);
      await harness.act(() => rows[1].click());
      const active = harness.q('.mqtt-item.active');
      if (!active) throw new Error('clicked row must become active');
      if (!active.textContent.includes('Cloud')) throw new Error('clicked row is not the Cloud profile');
      if (!harness.container.textContent.includes('ECONNREFUSED')) throw new Error('detail pane did not switch to the clicked profile');
      return '2 rows, click selects detail';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('stream: a burst of message frames is batched and the newest rows land', async () => {
    resetStubs();
    const { Panel } = loadPlugin('zh-CN').__components;
    globalThis.__SSE_HELLO__ = { state: makeState(), messages: [], events: [] };
    const harness = makeHarness();
    try {
      await harness.render(h(Panel, {}));
      await harness.act(() => StubEventSource.instances[0].open());
      const items = harness.qa('.mqtt-item');
      if (items.length === 0) throw new Error('connection list missing after hello');
      await harness.act(() => items[0].click());
      if (!harness.q('.mqtt-clientpane')) throw new Error('clients pane missing after selecting a connection');
      const source = StubEventSource.instances[0];
      let ts = FIXTURE_TICK;
      await harness.act(async () => {
        for (let index = 0; index < 120; index += 1) {
          source.emit('message', {
            seq: 1000 + index,
            ts: (ts += 100),
            direction: index % 2 === 0 ? 'in' : 'out',
            topic: `burst/${index}`,
            payload: `p${index}`,
            qos: 0,
            retain: false,
            bytes: 2,
            kind: 'client',
            clientId: 'c1',
            name: 'Local broker',
          });
        }
        // The stream flushes on a short timer; let it run inside act so React
        // sees the batched update in the same act scope.
        await new Promise((resolve) => setTimeout(resolve, 150));
      });
      await harness.flush();
      const rows = harness.qa('.mqtt-log-row');
      if (rows.length !== 100) throw new Error(`expected the newest 100 rows, got ${rows.length}`);
      if (!harness.container.textContent.includes('100 / 120')) {
        throw new Error('page counter must show 100 / 120 after the burst');
      }
      return '120 frames -> 100 rows in one batch';
    } finally {
      await harness.dispose();
    }
  });

  await suite.test('MessageLog: pause toggle and clear callback', async () => {
    resetStubs();
    const components = loadPlugin('zh-CN').__components;
    let cleared = 0;
    const harness = makeHarness();
    try {
      await harness.render(h(components.MessageLog, { entries: makeEntries(3), onClear: () => { cleared += 1; } }));
      const pauseButton = buttonByText(harness, '暂停');
      if (!pauseButton) throw new Error('pause button missing');
      await harness.act(() => pauseButton.click());
      if (!buttonByText(harness, '继续')) throw new Error('pause button must switch to 继续');
      const clearButton = buttonByText(harness, '清空');
      if (!clearButton) throw new Error('clear button missing');
      await harness.act(() => clearButton.click());
      if (cleared !== 1) throw new Error(`onClear called ${cleared} times, expected 1`);
      return '暂停/继续 + 清空';
    } finally {
      await harness.dispose();
    }
  });

  // -------------------------------------------------------- static audits
  suite.group('static: contrast, i18n, structure');

  await suite.test('structure: dead view removed, shared action hook, memo rows, batched stream', () => {
    if (CLIENT_SRC.includes('function ConnectionDetail(')) {
      throw new Error('the legacy ConnectionDetail view must stay removed');
    }
    if (/const act = async \(label, action\)/.test(CLIENT_SRC)) {
      throw new Error('act() helpers must be deduped through useBusyAction');
    }
    has(CLIENT_SRC, 'function useBusyAction()', 'shared busy/action hook');
    has(CLIENT_SRC, 'const LogRow = React.memo(', 'memoised log rows');
    has(CLIENT_SRC, 'const EventRow = React.memo(', 'memoised event rows');
    has(CLIENT_SRC, 'LOG_FLUSH_MS', 'batched stream flush');
    return 'dedupe + memo + batching present';
  });

  const css = extractCss(CLIENT_SRC);
  const rules = parseRules(css);

  await suite.test('CSS: no hardcoded light surface can put inherited text in white-on-white', () => {
    const violations = [];
    for (const rule of rules) {
      const backgrounds = rule.decls.filter((decl) => decl.prop === 'background' || decl.prop === 'background-color');
      if (backgrounds.length === 0) continue;
      const light = backgrounds.find((decl) => CSS_LIGHT_SURFACE.test(decl.value));
      if (!light) continue;
      const color = rule.decls.find((decl) => decl.prop === 'color');
      if (!color || CSS_LIGHT_SURFACE.test(color.value) || /inherit/i.test(color.value)) {
        violations.push(`${rule.selector}: background:${light.value}${color ? ` color:${color.value}` : ' (text colour inherited)'}`);
      }
    }
    if (violations.length > 0) {
      throw new Error(`hardcoded light surfaces with light/inherited text: ${violations.join(' | ')}`);
    }
    // the dark-theme fix: adaptive surfaces derive from currentColor
    const adaptive = countOf(css, /color-mix\(in srgb,\s*currentColor/gi);
    if (adaptive < 10) throw new Error(`expected the color-mix(currentColor) dark-theme fix (>=10 uses), found ${adaptive}`);
    return `light surfaces=0, currentColor surfaces=${adaptive}`;
  });

  await suite.test('CSS: white text only on coloured backgrounds with WCAG AA contrast', () => {
    const problems = [];
    const details = [];
    for (const rule of rules) {
      const color = rule.decls.find((decl) => decl.prop === 'color');
      if (!color || /var\(/i.test(color.value)) continue;
      const hex = firstHex(color.value);
      if (!hex) continue;
      const backgrounds = rule.decls.filter((decl) => decl.prop === 'background' || decl.prop === 'background-color');
      if (backgrounds.length === 0) {
        problems.push(`${rule.selector}: color:${color.value} with no explicit background (inherits the surface)`);
        continue;
      }
      const bg = firstHex(backgrounds[0].value);
      if (!bg) continue;
      const ratio = contrastRatio(hex, bg);
      details.push(`${rule.selector} ${hex} on ${bg} = ${ratio.toFixed(2)}:1`);
      if (ratio < 4.5) problems.push(`${rule.selector}: ${hex} on ${bg} is ${ratio.toFixed(2)}:1 (AA needs 4.5)`);
    }
    if (problems.length > 0) throw new Error(problems.join(' | '));
    // light-theme fallbacks inside var() are only used when the DSH theme vars are
    // missing; report them (risk) but do not fail the suite on the embedded host.
    const fallbacks = [];
    for (const rule of rules) {
      const color = rule.decls.find((decl) => decl.prop === 'color');
      if (!color || !/var\(/i.test(color.value)) continue;
      const hex = fallbackHex(color.value);
      if (!hex) continue;
      const lum = luminance(hexToRgb(hex));
      if (lum < 0.42) fallbacks.push(`${rule.selector} -> ${hex}`);
    }
    return `${details.join('; ')}; light-theme fallback text colours (risk if theme vars are absent): ${fallbacks.length}`;
  });

  await suite.test('i18n: every t() call has two non-empty literal strings', () => {
    const pairs = parseTranslationPairs(CLIENT_SRC);
    if (pairs.length < 50) throw new Error(`only ${pairs.length} t() pairs found, expected the bilingual panel to have more`);
    for (const pair of pairs) {
      if (!pair.en || !pair.zh) throw new Error(`client.js:${pair.line}: t() has an empty string`);
      if (CJK.test(pair.en)) throw new Error(`client.js:${pair.line}: English t() argument contains Chinese: ${JSON.stringify(pair.en)}`);
      if (pair.en !== pair.zh && !CJK.test(pair.zh)) {
        throw new Error(`client.js:${pair.line}: Chinese t() argument is not translated: ${JSON.stringify(pair.zh)}`);
      }
    }
    const pairCount = pairs.length;
    // untranslated visible literals: placeholders/titles that bypass t()
    const literalPlaceholders = [...CLIENT_SRC.matchAll(/\bplaceholder:\s*'([^']*)'/g)].map((match) => match[1]);
    const literalTitles = [...CLIENT_SRC.matchAll(/\btitle:\s*'([^']*)'/g)].map((match) => match[1]);
    const literals = literalPlaceholders.concat(literalTitles);
    for (const literal of literals) {
      if (!/^[\x20-\x7e]*$/.test(literal)) throw new Error(`untranslated non-ASCII literal in a visible attribute: ${JSON.stringify(literal)}`);
    }
    // Chinese may only appear inside t() pairs or the STATUS_LABEL zh table.
    const statusStart = CLIENT_SRC.indexOf('const STATUS_LABEL');
    const statusEnd = CLIENT_SRC.indexOf('const statusLabel');
    if (statusStart < 0 || statusEnd < statusStart) throw new Error('client.js: STATUS_LABEL block not found');
    const masked = maskRanges(CLIENT_SRC, pairs.map((pair) => ({ start: pair.start, end: pair.end })).concat([{ start: statusStart, end: statusEnd }]));
    const outside = [];
    for (let index = 0; index < masked.length; index += 1) {
      if (CJK.test(masked[index])) {
        outside.push(`line ${lineOf(masked, index)}: ${masked.slice(index, index + 24).replace(/\n/g, ' ')}`);
      }
    }
    if (outside.length > 0) throw new Error(`Chinese text outside t()/STATUS_LABEL: ${outside.slice(0, 4).join(' | ')}`);
    return `${pairCount} bilingual pairs, ${literals.length} language-neutral literals, 0 stray CJK`;
  });

  await suite.test('structure: panel markers, no HTML injection, only react is required', () => {
    has(CLIENT_SRC, "'data-dsh-plugin': 'mqtt'", 'client.js');
    has(CLIENT_SRC, "'data-dsh-panel-entry': 'mqtt'", 'client.js');
    if (/\bdangerouslySetInnerHTML\b/.test(CLIENT_SRC)) throw new Error('client.js uses dangerouslySetInnerHTML');
    if (/\binnerHTML\b/.test(CLIENT_SRC)) throw new Error('client.js uses innerHTML');
    if (/<iframe/i.test(CLIENT_SRC)) throw new Error('client.js contains an iframe');
    const requireCalls = [...CLIENT_SRC.matchAll(/\brequire\s*\(\s*([^)]*?)\s*\)/g)].map((match) => match[1]);
    if (requireCalls.length === 0) throw new Error('client.js never calls require()');
    for (const argument of requireCalls) {
      if (argument !== "'react'") throw new Error(`client.js required a module other than react: ${argument}`);
    }
    const loaderId = loaderSpec?.id;
    if (loaderId !== 'dsh-mqtt-tools') throw new Error(`loader id ${JSON.stringify(loaderId)}`);
    return `requires=${requireCalls.length} (react only), markers + no innerHTML/iframe`;
  });

  await suite.test('plugin: slots registration and test-only surface', () => {
    const plugin = loadPlugin('zh-CN');
    if (JSON.stringify(plugin.inject) !== JSON.stringify(['slots'])) throw new Error(`inject ${JSON.stringify(plugin.inject)}`);
    const events = [];
    const disposals = [];
    const ctx = {
      slots: {
        inject(name, register) {
          events.push({ type: 'inject', name });
          const dispose = register();
          return typeof dispose === 'function' ? dispose : () => {};
        },
        register(spec, component) {
          events.push({ type: 'register', spec, component });
          return () => disposals.push(spec);
        },
      },
      effect(fn, label) {
        events.push({ type: 'effect', label, fn });
      },
    };
    plugin.apply(ctx);
    const injections = events.filter((event) => event.type === 'inject').map((event) => event.name);
    if (JSON.stringify(injections) !== JSON.stringify(['sidebar.panellist', 'main'])) {
      throw new Error(`slots injected: ${JSON.stringify(injections)}`);
    }
    const registrations = events.filter((event) => event.type === 'register');
    if (registrations.length !== 2) throw new Error(`expected 2 slot registrations, got ${registrations.length}`);
    const [iconRegistration, panelRegistration] = registrations;
    const iconSpec = iconRegistration.spec;
    if (iconSpec.name !== 'sidebar.panellist' || iconSpec.id !== 'mqtt' || iconSpec.order !== 42) {
      throw new Error(`sidebar spec ${JSON.stringify(iconSpec)}`);
    }
    if (typeof iconSpec.label !== 'function' || iconSpec.label() !== 'MQTT') throw new Error('sidebar label must be () => "MQTT"');
    if (iconRegistration.component !== plugin.__components.PanelIcon) throw new Error('sidebar panellist component must be PanelIcon');
    if (panelRegistration.spec.name !== 'main' || panelRegistration.spec.key !== 'mqtt') {
      throw new Error(`main spec ${JSON.stringify(panelRegistration.spec)}`);
    }
    if (panelRegistration.component !== plugin.__components.Panel) throw new Error('main component must be Panel');
    const effects = events.filter((event) => event.type === 'effect');
    if (effects.length !== 1 || effects[0].label !== 'dsh-mqtt-tools: panel') throw new Error(`effect registration ${JSON.stringify(effects.map((effect) => effect.label))}`);
    const cleanup = effects[0].fn();
    if (typeof cleanup !== 'function') throw new Error('ctx.effect must register a cleanup function');
    cleanup();
    if (disposals.length !== 2) throw new Error(`cleanup must dispose both slot registrations, disposed ${disposals.length}`);
    const expectedKeys = ['BrokerTab', 'ClientPane', 'ConnectionsTab', 'MessageLog', 'Panel', 'PanelIcon', 'Publisher'];
    for (const key of expectedKeys) {
      if (typeof plugin.__components[key] !== 'function') throw new Error(`__components.${key} is not a component`);
    }
    globalThis.__DSH_MQTT_TEST__ = false;
    try {
      const bare = loadPlugin('zh-CN');
      if (bare.__components !== undefined) throw new Error('__components must be absent without __DSH_MQTT_TEST__');
    } finally {
      globalThis.__DSH_MQTT_TEST__ = true;
    }
    return '2 slots (id=order, key), effect cleanup, 7 test components';
  });

  // ---------------------------------------------- optional real-browser step
  suite.group('optional: real browser');
  {
    let reason = '';
    try {
      const stateResponse = await nodeFetch(`${LIVE_BASE}/dsh-mqtt-tools/api/state`);
      const stateText = await stateResponse.text();
      const apiOk = stateResponse.status === 200 && stateText.includes('"ok":true');
      const rootResponse = await nodeFetch(`${LIVE_BASE}/`);
      const rootStatus = rootResponse.status;
      if (apiOk && rootStatus === 200) {
        reason =
          'GUI document answered 200 without a token, but this suite deliberately does not drive a headless ' +
          'Edge against the live user session (no token URL was found, and the interactive GUI is the user\'s).';
      } else {
        reason =
          `GUI app document at ${LIVE_BASE}/ answered HTTP ${rootStatus} (token-protected shell); ` +
          `the plugin API /dsh-mqtt-tools/api/state answered HTTP ${stateResponse.status}${apiOk ? ' with ok:true' : ''}. ` +
          'A token URL is required to load the panel page; none exists in the plugin dir, ' +
          'C:\\Users\\wmd\\.dsh or the DSH process command lines, so headless Edge cannot render the panel.';
      }
    } catch (error) {
      reason = `live GUI unreachable at ${LIVE_BASE} (${error?.message ?? error}); real-browser panel check skipped.`;
    }
    suite.skip('headless Edge renders the panel and clicks 服务端', reason);
  }

  dom.window.close();
});
