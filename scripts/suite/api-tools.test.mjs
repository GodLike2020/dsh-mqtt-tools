/**
 * Live API, agent-tool and security suite for the installed dsh-mqtt-tools plugin.
 *
 * Tests the real instance at LIVE_BASE (default http://127.0.0.1:6871):
 *   - HTTP contract of /dsh-mqtt-tools/api (state, command, SSE) including abuse cases
 *   - SSE hello / state / message frames, reconnect, churn
 *   - password non-disclosure in the API and SSE, plaintext-at-rest risk
 *   - the 9 agent tools as pure modules: schema shape, clamps, base64 round-trip,
 *     mqtt_wait timeouts, broker transitions (isolated engine, not the live broker)
 *
 * Everything the suite creates on the live instance is removed again: clients are
 * disconnected and deleted, broker settings are restored from a snapshot, the log
 * is cleared, and the final state is asserted to be clean.
 *
 * Run alone:  node scripts/suite/api-tools.test.mjs
 */
import { existsSync, readFileSync } from 'node:fs';
import {
  Suite,
  runSuite,
  LIVE_BASE,
  assert,
  eq,
  sleep,
  waitFor,
  snapshotLiveBroker,
  restoreLiveBroker,
  isolatedEngine,
} from './harness.mjs';
import { createTools } from '../../lib/tools.js';

const API = '/dsh-mqtt-tools/api';
const RUN = Math.random().toString(36).slice(2, 10);
const PREFIX = 'suite-api-';
let counter = 0;
const uniq = (label) => `${PREFIX}${RUN}-${label}-${(counter += 1)}`;
const suite = new Suite('api-tools', 'Live HTTP API, agent tools and security', {
  note:
    'SSE listener-leak growth cannot be measured (hub exposes no client count); 20-cycle responsiveness is the proxy. ' +
    'Suite binds to the live loopback instance only; no network probing.',
});

/* ------------------------------------------------------------------ state */

const created = new Set();
const openStreams = new Set();
const markers = [];
let baseline = null;
let baselineStatus = 'stopped';
let stateFile = '';
let brokerPort = 0;
let cleanupReport = null;
let happyTopic = '';

/* ------------------------------------------------------------------ HTTP */

async function http(path, { method = 'GET', body, headers = {}, omitHeaders = [], timeout = 8000 } = {}) {
  const requestHeaders = {};
  if (body !== undefined) requestHeaders['content-type'] = 'application/json';
  if (method === 'POST') requestHeaders['x-dsh-mqtt-tools'] = '1';
  Object.assign(requestHeaders, headers);
  for (const name of omitHeaders) delete requestHeaders[name];
  const response = await fetch(`${LIVE_BASE}${path}`, {
    method,
    headers: requestHeaders,
    body,
    signal: AbortSignal.timeout(timeout),
  });
  const text = await response.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return { status: response.status, headers: Object.fromEntries(response.headers.entries()), text, json };
}

function post(cmd, input = {}, { headers, omitHeaders, timeout } = {}) {
  return http(`${API}/command`, {
    method: 'POST',
    body: JSON.stringify({ cmd, ...input }),
    headers,
    omitHeaders,
    timeout,
  });
}

async function getState() {
  const response = await http(`${API}/state`);
  assert(response.status === 200, `GET /state: expected 200, got ${response.status} (${response.text.slice(0, 200)})`);
  assert(response.json?.ok === true, `GET /state: ok is not true (${response.text.slice(0, 200)})`);
  return response.json;
}

async function liveFind(selector) {
  const { state } = await getState();
  return state.clients.find((client) => client.id === selector || client.name === selector) ?? null;
}

async function saveLiveClient(name, extra = {}) {
  const response = await post('client.save', {
    profile: { name, url: `mqtt://127.0.0.1:${brokerPort}`, clientId: name, protocolVersion: 4, ...extra },
  });
  assert(response.json?.ok === true, `client.save failed: ${response.text.slice(0, 300)}`);
  const profile = response.json.result;
  assert(typeof profile?.id === 'string' && profile.id !== '', `client.save returned no id: ${response.text.slice(0, 200)}`);
  created.add(profile.id);
  return profile;
}

/* ------------------------------------------------------------------ SSE */

function parseSseBlock(block) {
  let event = null;
  const data = [];
  for (const line of block.split('\n')) {
    if (line.startsWith('event:')) event = line.slice(6).trim();
    else if (line.startsWith('data:')) data.push(line.slice(5).trim());
  }
  if (event === null) return null;
  let parsed = null;
  try {
    parsed = data.length > 0 ? JSON.parse(data.join('\n')) : null;
  } catch {
    parsed = null;
  }
  return { event, data: parsed };
}

async function openSse({ timeout = 5000 } = {}) {
  const controller = new AbortController();
  let timer = null;
  const res = await Promise.race([
    fetch(`${LIVE_BASE}${API}/events`, { headers: { accept: 'text/event-stream' }, signal: controller.signal }),
    new Promise((_resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new Error(`SSE connect timeout after ${timeout} ms`));
      }, timeout);
      timer.unref?.();
    }),
  ]).finally(() => clearTimeout(timer));
  const stream = {
    status: res.status,
    contentType: res.headers.get('content-type') ?? '',
    frames: [],
    raw: '',
    error: null,
    closed: false,
  };
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const pump = (async () => {
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value, { stream: true });
        stream.raw += chunk;
        buffer += chunk;
        let index = buffer.indexOf('\n\n');
        while (index >= 0) {
          const block = buffer.slice(0, index);
          buffer = buffer.slice(index + 2);
          const frame = parseSseBlock(block);
          if (frame !== null) stream.frames.push(frame);
          index = buffer.indexOf('\n\n');
        }
      }
    } catch (error) {
      if (error?.name !== 'AbortError') stream.error = String(error?.message ?? error);
    }
  })();
  stream.pump = pump;
  stream.close = async () => {
    if (stream.closed) return;
    stream.closed = true;
    openStreams.delete(stream);
    try {
      controller.abort();
    } catch {
      /* already aborted */
    }
    try {
      await reader.cancel();
    } catch {
      /* already released */
    }
    await pump.catch(() => {});
  };
  openStreams.add(stream);
  return stream;
}

async function waitFrame(stream, predicate, { timeout = 5000, label = 'SSE frame' } = {}) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const found = stream.frames.find(predicate);
    if (found !== undefined) return found;
    if (Date.now() >= deadline) {
      const seen = stream.frames.map((frame) => frame.event).join(', ') || '(none)';
      throw new Error(`timeout waiting for ${label}; frames seen: ${seen}`);
    }
    await sleep(25);
  }
}

/* ------------------------------------------------------------------ cleanup */

async function doCleanup() {
  const report = { errors: [], deleted: 0, disconnected: 0, final: null, stateFile: '', fileText: '' };
  for (const stream of [...openStreams]) {
    try {
      await stream.close();
    } catch (error) {
      report.errors.push(`close SSE: ${error.message}`);
    }
  }
  for (const id of [...created].reverse()) {
    try {
      const disconnect = await post('client.disconnect', { id });
      if (disconnect.json?.ok === true) report.disconnected += 1;
    } catch (error) {
      report.errors.push(`disconnect ${id}: ${error.message}`);
    }
    try {
      const response = await post('client.delete', { id });
      if (response.json?.ok === true) report.deleted += 1;
      else report.errors.push(`delete ${id}: ${response.text.slice(0, 200)}`);
    } catch (error) {
      report.errors.push(`delete ${id}: ${error.message}`);
    }
  }
  try {
    await restoreLiveBroker(baseline);
  } catch (error) {
    report.errors.push(`restore broker: ${error.message}`);
  }
  try {
    const { state } = await getState();
    if (baselineStatus === 'running') {
      // restart so the restored settings are actually applied to the running broker
      await post('broker.stop');
      await post('broker.start');
    } else if (state.broker.status === 'running') {
      await post('broker.stop');
    }
  } catch (error) {
    report.errors.push(`broker status restore: ${error.message}`);
  }
  try {
    const response = await post('log.clear');
    if (response.json?.ok !== true) report.errors.push(`log.clear: ${response.text.slice(0, 200)}`);
  } catch (error) {
    report.errors.push(`log.clear: ${error.message}`);
  }
  await sleep(1000); // store debounce is 300 ms; broker stop settles asynchronously
  try {
    report.final = await getState();
  } catch (error) {
    report.errors.push(`final state: ${error.message}`);
  }
  if (stateFile !== '' && existsSync(stateFile)) {
    report.stateFile = stateFile;
    try {
      report.fileText = readFileSync(stateFile, 'utf8');
    } catch (error) {
      report.errors.push(`read state file: ${error.message}`);
    }
  }
  return report;
}

function finalState() {
  assert(cleanupReport?.final !== null, 'cleanup did not capture a final state');
  return cleanupReport.final;
}

/* ------------------------------------------------------------------ suite body */

await runSuite(suite, async () => {
  const streams = [];
  let lab = null;
  let labTools = null;
  let labClientId = '';
  let happyId = '';
  let sseClientId = '';

  try {
    /* ------------------------------------------------ setup (snapshot first) */
    suite.group('setup');
    await suite.test('snapshot the live broker before any mutation', async () => {
      baseline = await snapshotLiveBroker();
      stateFile = (await getState()).state.stateFile;
      baselineStatus = (await getState()).state.broker.status;
      assert(typeof baseline.host === 'string', 'broker snapshot has no host');
      assert(typeof baseline.port === 'number', 'broker snapshot has no port');
      return `${baselineStatus} on ${baseline.host}:${baseline.port}, file ${stateFile}`;
    });

    await suite.test('remove leftover suite- clients and start from a known state', async () => {
      const { state } = await getState();
      for (const client of state.clients.filter((entry) => String(entry.name).startsWith('suite-') || String(entry.id).startsWith('suite-'))) {
        await post('client.disconnect', { id: client.id });
        await post('client.delete', { id: client.id });
        created.delete(client.id);
      }
      const cleared = await post('log.clear');
      assert(cleared.json?.ok === true, `log.clear failed: ${cleared.text.slice(0, 200)}`);
      const after = await getState();
      assert(after.state.clients.every((client) => !String(client.name).startsWith('suite-')), 'leftover suite- client survived start cleanup');
      brokerPort = 0;
      return 'clean';
    });

    await suite.test('ensure the embedded broker is running for live tests', async () => {
      const { state } = await getState();
      if (state.broker.status !== 'running') {
        const started = await post('broker.start');
        assert(started.json?.ok === true, `broker.start failed: ${started.text.slice(0, 300)}`);
      }
      const running = await waitFor(async () => {
        const current = await getState();
        return current.state.broker.status === 'running' ? current.state.broker : null;
      }, { timeout: 5000, label: 'broker to be running' });
      brokerPort = running.boundPort ?? running.port;
      assert(typeof brokerPort === 'number' && brokerPort > 0, `broker has no usable port: ${JSON.stringify(running)}`);
      return `listening on ${brokerPort}`;
    });

    /* ------------------------------------------------ 1. HTTP contract */
    suite.group('1. HTTP API contract');

    await suite.test('GET /state is JSON with logs beside state and a tool report', async () => {
      const response = await http(`${API}/state`);
      assert(response.status === 200, `status ${response.status}`);
      assert(String(response.headers['content-type'] ?? '').includes('application/json'), `content-type ${response.headers['content-type']}`);
      const body = response.json;
      assert(body?.ok === true, 'ok is not true');
      assert(Array.isArray(body.messages), 'messages is not an array');
      assert(Array.isArray(body.events), 'events is not an array');
      assert(body.state && typeof body.state === 'object', 'state missing');
      assert(Array.isArray(body.state.clients), 'state.clients is not an array');
      assert(body.state.broker && typeof body.state.broker.status === 'string', 'state.broker.status missing');
      assert(typeof body.state.stateFile === 'string' && body.state.stateFile.endsWith('dsh-mqtt-tools.json'), `stateFile ${body.state.stateFile}`);
      return `${body.state.clients.length} client(s), ${body.messages.length} message(s)`;
    });

    await suite.test('GET /state reports exactly the 9 registered tools', async () => {
      const { state } = await getState();
      const names = ['mqtt_list', 'mqtt_connect', 'mqtt_disconnect', 'mqtt_subscribe', 'mqtt_unsubscribe', 'mqtt_publish', 'mqtt_messages', 'mqtt_wait', 'mqtt_broker'];
      eq(state.tools.registered, 9, 'tools.registered');
      eq([...state.tools.names].sort(), [...names].sort(), 'tools.names');
      eq(state.tools.error, null, 'tools.error');
      return state.tools.names.join(',');
    });

    await suite.test('POST /command without the x-dsh-mqtt-tools header is refused as JSON', async () => {
      const response = await post('log.clear', {}, { omitHeaders: ['x-dsh-mqtt-tools'] });
      eq(response.status, 403, 'status');
      assert(String(response.headers['content-type'] ?? '').includes('application/json'), 'non-JSON refusal');
      eq(response.json?.ok, false, 'ok');
      assert(String(response.json?.error).includes('x-dsh-mqtt-tools'), `error ${response.json?.error}`);
      return response.json.error;
    });

    await suite.test('POST /command with a foreign Origin is refused', async () => {
      const response = await post('log.clear', {}, { headers: { origin: 'http://evil.example' } });
      eq(response.status, 403, 'status');
      eq(response.json?.ok, false, 'ok');
      eq(response.json?.error, 'unexpected origin', 'error');
      return '403 unexpected origin';
    });

    await suite.test('GET /state with a foreign Origin is refused', async () => {
      const response = await http(`${API}/state`, { headers: { origin: 'http://evil.example' } });
      eq(response.status, 403, 'status');
      eq(response.json?.error, 'unexpected origin', 'error');
      return '403 unexpected origin';
    });

    await suite.test('missing Origin is allowed: the header check is CSRF, not authentication', async () => {
      const response = await post('log.clear');
      eq(response.status, 200, 'status');
      eq(response.json?.ok, true, 'ok');
      return 'non-browser caller allowed';
    });

    await suite.test('text/plain content-type is accepted (no 415 enforcement)', async () => {
      const response = await http(`${API}/command`, {
        method: 'POST',
        body: JSON.stringify({ cmd: 'log.clear' }),
        headers: { 'content-type': 'text/plain' },
      });
      eq(response.status, 200, 'status');
      eq(response.json?.ok, true, 'ok');
      // CSRF is still blocked: the x-dsh-mqtt-tools header forces a CORS preflight.
      return '200; x-dsh-mqtt-tools header still gates simple cross-origin requests';
    });

    await suite.test('unknown cmd is answered in-band with ok:false and a state echo', async () => {
      const response = await post('nope');
      eq(response.status, 200, 'status');
      eq(response.json?.ok, false, 'ok');
      assert(String(response.json?.error).includes('unknown command: nope'), `error ${response.json?.error}`);
      assert(response.json.state && typeof response.json.state === 'object', 'no state echo');
      return response.json.error;
    });

    await suite.test('empty / null / array / scalar bodies are rejected as missing command', async () => {
      for (const body of ['', 'null', '[1,2]', '42']) {
        const response = await http(`${API}/command`, { method: 'POST', body });
        eq(response.status, 400, `status for body ${JSON.stringify(body)}`);
        eq(response.json?.ok, false, `ok for body ${JSON.stringify(body)}`);
        eq(response.json?.error, 'missing command', `error for body ${JSON.stringify(body)}`);
      }
      return '4 bodies rejected';
    });

    await suite.test('malformed JSON answers a JSON error and the server stays responsive', async () => {
      const response = await http(`${API}/command`, { method: 'POST', body: '{"cmd":' });
      assert(String(response.headers['content-type'] ?? '').includes('application/json'), 'non-JSON error');
      eq(response.json?.ok, false, 'ok');
      assert(String(response.json?.error).includes('not valid JSON'), `error ${response.json?.error}`);
      const state = await getState();
      eq(state.state.tools.registered, 9, 'tools after malformed body');
      return `status ${response.status}, JSON error; /state still ok`;
    });

    await suite.test('malformed JSON is a client error, not 500', async () => {
      const response = await http(`${API}/command`, { method: 'POST', body: '{"cmd":' });
      assert(response.status >= 400 && response.status < 500, `expected 4xx for malformed JSON, got ${response.status} (${response.json?.error})`);
      return String(response.status);
    });

    await suite.test('a body over 2 MiB answers a JSON error without wedging the server', async () => {
      const body = JSON.stringify({ cmd: 'log.clear', pad: 'x'.repeat(2 * 1024 * 1024 + 1024) });
      const response = await http(`${API}/command`, { method: 'POST', body, timeout: 15000 });
      assert(String(response.headers['content-type'] ?? '').includes('application/json'), 'non-JSON error');
      eq(response.json?.ok, false, 'ok');
      assert(String(response.json?.error).includes('too large'), `error ${response.json?.error}`);
      const started = Date.now();
      await getState();
      assert(Date.now() - started < 2000, 'server is slow after oversized body');
      return `status ${response.status}, JSON error; responsive`;
    });

    await suite.test('a body over 2 MiB is rejected as 413', async () => {
      const body = JSON.stringify({ cmd: 'log.clear', pad: 'x'.repeat(2 * 1024 * 1024 + 1024) });
      const response = await http(`${API}/command`, { method: 'POST', body, timeout: 15000 });
      eq(response.status, 413, 'status');
      return '413';
    });

    await suite.test('a body just under 2 MiB is still accepted', async () => {
      const pad = 'x'.repeat(2 * 1024 * 1024 - 256);
      const response = await http(`${API}/command`, { method: 'POST', body: JSON.stringify({ cmd: 'log.clear', pad }), timeout: 15000 });
      eq(response.status, 200, 'status');
      eq(response.json?.ok, true, 'ok');
      return `${JSON.stringify({ cmd: 'log.clear', pad }).length} bytes accepted`;
    });

    await suite.test('method misuse is refused as JSON: GET /command, DELETE and POST /state', async () => {
      const getCommand = await http(`${API}/command`);
      eq(getCommand.status, 405, 'GET /command status');
      eq(getCommand.json?.error, 'POST only', 'GET /command error');
      const deleteState = await http(`${API}/state`, { method: 'DELETE' });
      eq(deleteState.status, 405, 'DELETE /state status');
      eq(deleteState.json?.error, 'GET only', 'DELETE /state error');
      const postState = await http(`${API}/state`, { method: 'POST', body: '{}' });
      eq(postState.status, 405, 'POST /state status');
      return '405 JSON everywhere';
    });

    await suite.test('cross-origin preflight is not granted (no CORS headers)', async () => {
      const response = await http(`${API}/command`, {
        method: 'OPTIONS',
        headers: {
          origin: 'http://evil.example',
          'access-control-request-method': 'POST',
          'access-control-request-headers': 'content-type,x-dsh-mqtt-tools',
        },
      });
      assert(response.headers['access-control-allow-origin'] === undefined, 'ACAO present on preflight');
      assert(!(response.status >= 200 && response.status < 300), `preflight unexpectedly allowed (${response.status})`);
      return `status ${response.status}, no ACAO`;
    });

    await suite.test('GET /state still answers quickly after the abuse cases', async () => {
      const started = Date.now();
      const { state } = await getState();
      eq(state.tools.registered, 9, 'tools still registered');
      const elapsed = Date.now() - started;
      assert(elapsed < 2000, `GET /state took ${elapsed} ms`);
      return `${elapsed} ms`;
    });

    /* ------------------------------------------------ broker transitions (live) */
    suite.group('1b. Live broker command transitions');

    await suite.test('broker.stop / broker.publish refusal / broker.start / broker.save', async () => {
      const stopped = await post('broker.stop');
      eq(stopped.json?.ok, true, 'broker.stop ok');
      const afterStop = await getState();
      eq(afterStop.state.broker.status, 'stopped', 'broker status after stop');
      const refused = await post('broker.publish', { topic: `${PREFIX}${RUN}/broker/off`, payload: 'x' });
      eq(refused.status, 200, 'refusal status');
      eq(refused.json?.ok, false, 'refusal ok');
      assert(String(refused.json?.error).includes('start the broker'), `error ${refused.json?.error}`);
      const started = await post('broker.start');
      eq(started.json?.ok, true, 'broker.start ok');
      const running = await waitFor(async () => {
        const current = await getState();
        return current.state.broker.status === 'running' ? current.state.broker : null;
      }, { timeout: 5000, label: 'broker running after restart' });
      brokerPort = running.boundPort ?? running.port;
      const saved = await post('broker.save', {
        broker: {
          host: baseline.host,
          port: baseline.port,
          autoStart: baseline.autoStart,
          allowAnonymous: baseline.allowAnonymous,
          protocols: baseline.protocols,
        },
      });
      eq(saved.json?.ok, true, 'broker.save ok');
      const state = (await getState()).state;
      eq(state.broker.port, baseline.port, 'saved port');
      eq(state.broker.allowAnonymous, baseline.allowAnonymous, 'saved allowAnonymous');
      return `stopped -> refused -> running :${brokerPort} -> saved`;
    });

    /* ------------------------------------------------ command happy path */
    suite.group('1c. Command happy path');

    await suite.test('client.save creates a throwaway connection', async () => {
      const name = uniq('happy');
      const profile = await saveLiveClient(name);
      happyId = profile.id;
      assert(profile.hasPassword === false, 'new profile reports a password');
      const stored = await liveFind(profile.id);
      assert(stored !== null, 'saved client is not in GET /state');
      eq(stored.name, name, 'stored name');
      eq(stored.status, 'idle', 'stored status');
      return `${name} (${profile.id.slice(0, 8)}…)`;
    });

    await suite.test('client.connect reaches connected state', async () => {
      const response = await post('client.connect', { id: happyId });
      eq(response.json?.ok, true, 'connect ok');
      const view = await waitFor(async () => {
        const client = await liveFind(happyId);
        return client?.status === 'connected' ? client : null;
      }, { timeout: 8000, label: 'happy client connected' });
      eq(view.status, 'connected', 'status');
      return 'connected';
    });

    await suite.test('client.subscribe is remembered on the profile', async () => {
      happyTopic = `${PREFIX}${RUN}/happy/echo`;
      const response = await post('client.subscribe', { id: happyId, topic: happyTopic, qos: 1 });
      eq(response.json?.ok, true, 'subscribe ok');
      const view = await waitFor(async () => {
        const client = await liveFind(happyId);
        return client?.subscriptions?.some((sub) => sub.topic === happyTopic) ? client : null;
      }, { timeout: 5000, label: 'subscription stored' });
      const subscription = view.subscriptions.find((sub) => sub.topic === happyTopic);
      eq(subscription.qos, 1, 'granted qos remembered');
      return `${happyTopic}@1`;
    });

    await suite.test('client.publish succeeds and its record appears in GET /state messages', async () => {
      const marker = `payload-${RUN}-${Date.now()}`;
      const response = await post('client.publish', { id: happyId, topic: happyTopic, payload: marker, qos: 1 });
      eq(response.status, 200, 'status');
      eq(response.json?.ok, true, 'publish ok');
      eq(response.json.result.topic, happyTopic, 'published topic');
      eq(response.json.result.bytes, Buffer.byteLength(marker), 'published bytes');
      const { messages } = await waitFor(async () => {
        const current = await getState();
        return current.messages.some((entry) => entry.topic === happyTopic && entry.direction === 'out' && entry.kind === 'client')
          ? current
          : null;
      }, { timeout: 5000, label: 'outbound message in GET /state' });
      const out = messages.find((entry) => entry.topic === happyTopic && entry.direction === 'out' && entry.kind === 'client');
      eq(out.encoding, 'utf8', 'outbound encoding');
      assert(out.payload === marker, `outbound payload ${JSON.stringify(out.payload)}`);
      return `outbound record has payload and ${out.bytes} byte(s)`;
    });

    await suite.test('client.disconnect then client.delete removes the connection', async () => {
      const disconnected = await post('client.disconnect', { id: happyId });
      eq(disconnected.json?.ok, true, 'disconnect ok');
      await waitFor(async () => (await liveFind(happyId))?.status === 'idle', { timeout: 5000, label: 'client idle' });
      const deleted = await post('client.delete', { id: happyId });
      eq(deleted.json?.ok, true, 'delete ok');
      await waitFor(async () => (await liveFind(happyId)) === null, { timeout: 5000, label: 'client gone' });
      created.delete(happyId);
      return 'removed';
    });

    /* ------------------------------------------------ 2. SSE */
    suite.group('2. Server-sent events');

    await suite.test('GET /events opens an SSE stream with a hello frame', async () => {
      const stream = await openSse();
      streams.push(stream);
      eq(stream.status, 200, 'status');
      assert(stream.contentType.startsWith('text/event-stream'), `content-type ${stream.contentType}`);
      const hello = await waitFrame(stream, (frame) => frame.event === 'hello', { label: 'hello frame' });
      assert(hello.data?.state && typeof hello.data.state === 'object', 'hello has no state');
      assert(Array.isArray(hello.data.messages), 'hello has no messages array');
      assert(Array.isArray(hello.data.events), 'hello has no events array');
      eq(hello.data.state.tools.registered, 9, 'hello state tools');
      assert(stream.raw.includes('retry: 2000'), 'SSE retry hint missing');
      return `hello + retry hint; ${hello.data.messages.length} message(s)`;
    });

    await suite.test('a command triggers a state frame', async () => {
      const stream = streams[0];
      const name = uniq('sse-state');
      const saved = await post('client.save', {
        profile: { name, url: `mqtt://127.0.0.1:${brokerPort}`, clientId: name, protocolVersion: 4 },
      });
      assert(saved.json?.ok === true, `client.save failed: ${saved.text.slice(0, 300)}`);
      created.add(saved.json.result.id);
      const frame = await waitFrame(stream, (candidate) => candidate.event === 'state'
        && Array.isArray(candidate.data?.clients)
        && candidate.data.clients.some((client) => client.name === name), { label: 'state frame after client.save' });
      eq(frame.data.tools.registered, 9, 'state frame tool report');
      return `state frame carries the new client (${name})`;
    });

    await suite.test('a client publish produces an inbound message frame', async () => {
      const stream = streams[0];
      const name = uniq('sse-client');
      const profile = await saveLiveClient(name);
      sseClientId = profile.id;
      const topic = `${PREFIX}${RUN}/sse/echo`;
      await post('client.connect', { id: profile.id });
      await waitFor(async () => (await liveFind(profile.id))?.status === 'connected', { timeout: 8000, label: 'sse client connected' });
      const subscribed = await post('client.subscribe', { id: profile.id, topic, qos: 1 });
      eq(subscribed.json?.ok, true, 'subscribe ok');
      const published = await post('client.publish', { id: profile.id, topic, payload: `echo-${RUN}`, qos: 1 });
      eq(published.json?.ok, true, 'publish ok');
      const frame = await waitFrame(stream, (candidate) => candidate.event === 'message'
        && candidate.data?.kind === 'client'
        && candidate.data?.direction === 'in'
        && candidate.data?.topic === topic, { label: 'inbound message frame', timeout: 5000 });
      eq(frame.data.encoding, 'utf8', 'inbound encoding');
      return `inbound echo frame on ${topic}`;
    });

    await suite.test('a client publish produces an outbound message frame', async () => {
      const stream = streams[0];
      assert(sseClientId !== '', 'sse client missing from earlier test');
      const topic = `${PREFIX}${RUN}/sse/out`;
      const published = await post('client.publish', { id: sseClientId, topic, payload: 'outbound-check', qos: 0 });
      eq(published.json?.ok, true, 'publish ok');
      const frame = await waitFrame(stream, (candidate) => candidate.event === 'message'
        && candidate.data?.kind === 'client'
        && candidate.data?.direction === 'out'
        && candidate.data?.topic === topic, { label: 'outbound message frame', timeout: 2500 });
      eq(frame.data.payload, 'outbound-check', 'outbound payload');
      return 'outbound frame present';
    });

    await suite.test('an SSE reconnect delivers a fresh hello with current state', async () => {
      const first = streams[0];
      await first.close();
      const second = await openSse();
      streams.push(second);
      const hello = await waitFrame(second, (frame) => frame.event === 'hello', { label: 'hello after reconnect' });
      assert(Array.isArray(hello.data.state.clients), 'reconnected hello has no clients');
      eq(second.status, 200, 'status');
      return `${hello.data.state.clients.length} client(s) in reconnected hello`;
    });

    await suite.test('20 SSE connect/disconnect cycles leave the instance responsive', async () => {
      const failures = [];
      for (let index = 0; index < 20; index += 1) {
        try {
          const stream = await openSse({ timeout: 5000 });
          await waitFrame(stream, (frame) => frame.event === 'hello', { label: 'hello in cycle', timeout: 3000 });
          await stream.close();
        } catch (error) {
          failures.push(error.message);
        }
      }
      eq(failures.length, 0, `cycle failures: ${failures.slice(0, 3).join('; ')}`);
      const started = Date.now();
      await getState();
      const elapsed = Date.now() - started;
      assert(elapsed < 2000, `GET /state took ${elapsed} ms after churn`);
      return `20/20 hellos; /state ${elapsed} ms`;
    });

    suite.skip('SSE heartbeat ping at 20 s', 'would add 21 s to the suite; heartbeat exists in lib/hub.js but is not worth the wall time here');
    suite.skip('SSE listener-leak count', 'Hub exposes no client count to the API; 20-cycle responsiveness is the available proxy');

    /* ------------------------------------------------ 3. security / leaks */
    suite.group('3. Security and password leaks');

    const passwordMarker = `Sentinel-${RUN}-${Date.now()}`;
    const brokerMarker = `BrokerSentinel-${RUN}-${Date.now()}`;
    markers.push(passwordMarker, brokerMarker);
    let secretId = '';

    await suite.test('a client password is never echoed by GET /state', async () => {
      const name = uniq('secret');
      const profile = await saveLiveClient(name, { username: `${PREFIX}user`, password: passwordMarker });
      secretId = profile.id;
      eq(profile.hasPassword, true, 'hasPassword');
      assert(!('password' in profile), 'client view contains a password field');
      const raw = (await http(`${API}/state`)).text;
      assert(!raw.includes(passwordMarker), 'GET /state leaks the client password marker');
      const stored = await liveFind(profile.id);
      eq(stored.hasPassword, true, 'stored hasPassword');
      assert(!('password' in stored), 'stored client view contains a password field');
      assert(stored.username.length > 0, 'username not echoed');
      return 'hasPassword:true, marker absent, no password field';
    });

    await suite.test('a broker user password is never echoed by GET /state', async () => {
      const users = [...(baseline.users ?? []).map((user) => ({ username: user.username })), { username: uniq('user'), password: brokerMarker }];
      const response = await post('broker.save', {
        broker: {
          host: baseline.host,
          port: baseline.port,
          autoStart: baseline.autoStart,
          allowAnonymous: baseline.allowAnonymous,
          protocols: baseline.protocols,
          users,
        },
      });
      eq(response.json?.ok, true, `broker.save ok (${response.text.slice(0, 200)})`);
      const { state } = await getState();
      const raw = (await http(`${API}/state`)).text;
      assert(!raw.includes(brokerMarker), 'GET /state leaks the broker user password marker');
      const savedUser = state.broker.users.find((user) => String(user.username).startsWith(`${PREFIX}${RUN}-user`));
      assert(savedUser !== undefined, `broker user missing from view: ${JSON.stringify(state.broker.users)}`);
      eq(savedUser.hasPassword, true, 'broker user hasPassword');
      assert(!('password' in savedUser), 'broker user view contains a password field');
      return 'hasPassword:true, marker absent, no password field';
    });

    await suite.test('SSE frames never contain either password marker', async () => {
      const stream = await openSse();
      streams.push(stream);
      await waitFrame(stream, (frame) => frame.event === 'hello', { label: 'hello' });
      // trigger a fresh state frame that includes the secret client and broker users
      await saveLiveClient(uniq('sse-sec'));
      await waitFrame(stream, (frame) => frame.event === 'state', { label: 'state frame' });
      assert(!stream.raw.includes(passwordMarker), 'SSE leaks the client password marker');
      assert(!stream.raw.includes(brokerMarker), 'SSE leaks the broker user password marker');
      assert(!stream.raw.includes('"password"'), 'SSE frames contain a password key');
      return `hello+state frames checked (${stream.raw.length} bytes)`;
    });

    await suite.test('the state file stores both passwords in plaintext (reported risk)', async () => {
      await sleep(900); // store debounce is 300 ms
      assert(existsSync(stateFile), `state file missing: ${stateFile}`);
      const text = readFileSync(stateFile, 'utf8');
      assert(text.includes(passwordMarker), `expected client password in plaintext in ${stateFile}`);
      assert(text.includes(brokerMarker), `expected broker user password in plaintext in ${stateFile}`);
      return `${stateFile} contains both markers in plaintext`;
    });

    await suite.test('file access boundary: the API has no authentication of its own', async () => {
      const state = await http(`${API}/state`);
      assert(state.status === 200, 'unauthenticated GET /state was refused');
      assert(state.headers['set-cookie'] === undefined, 'API sets a cookie');
      const command = await post('log.clear');
      eq(command.json?.ok, true, 'unauthenticated command refused');
      // Only a local process (or same-origin page) can reach the loopback listener,
      // and it can drive the whole engine without any token.
      return 'no auth token/cookie: any local caller can drive the panel; cross-origin browsers are blocked by 403/preflight';
    });

    await suite.test('access boundary: cross-origin browser access is blocked, missing Origin is not', async () => {
      const foreign = await post('log.clear', {}, { headers: { origin: 'http://evil.example' } });
      eq(foreign.status, 403, 'foreign origin status');
      const noOrigin = await post('log.clear');
      eq(noOrigin.status, 200, 'missing origin status');
      return 'foreign Origin rejected; non-browser caller accepted (CSRF header != auth)';
    });

    suite.skip('plugin reachable from non-loopback interfaces', 'determined outside the suite: the DSH web server listens on 127.0.0.1:6871 only; no network scanning in this suite');

    /* ------------------------------------------------ 4. agent tools (isolated) */
    suite.group('4. Agent tools (isolated engine)');
    lab = await isolatedEngine({ autoStart: true });
    labTools = Object.fromEntries(createTools(lab).map((tool) => [tool.name, tool]));
    const labBase = `${PREFIX}${RUN}/lab`;
    let labConnected = false;
    try {
      await suite.test('createTools registers the same 9 tools as the live report', async () => {
        eq(Object.keys(labTools).length, 9, 'tool count');
        const live = (await getState()).state.tools.names;
        eq(Object.keys(labTools).sort(), [...live].sort(), 'tool names vs live report');
        return Object.keys(labTools).join(',');
      });

      await suite.test('tool schemas are closed objects with lifted required arrays and enums', async () => {
        for (const [name, tool] of Object.entries(labTools)) {
          eq(tool.parameters.type, 'object', `${name}.parameters.type`);
          eq(tool.parameters.additionalProperties, false, `${name}.parameters.additionalProperties`);
          for (const required of tool.parameters.required ?? []) {
            assert(Object.prototype.hasOwnProperty.call(tool.parameters.properties ?? {}, required), `${name}: required "${required}" is not a property`);
          }
          for (const [property, schema] of Object.entries(tool.parameters.properties ?? {})) {
            if (schema.enum !== undefined) {
              assert(Array.isArray(schema.enum) && schema.enum.length > 0, `${name}.${property}: empty enum`);
              eq(schema.type, 'string', `${name}.${property}: non-string enum type`);
            }
          }
        }
        eq(labTools.mqtt_publish.parameters.required, ['topic', 'payload'], 'mqtt_publish.required');
        eq(labTools.mqtt_broker.parameters.required, ['action'], 'mqtt_broker.required');
        eq(labTools.mqtt_publish.parameters.properties.via.enum, ['client', 'broker'], 'mqtt_publish.via enum');
        eq(labTools.mqtt_messages.parameters.properties.direction.enum, ['any', 'in', 'out'], 'mqtt_messages.direction enum');
        eq(labTools.mqtt_list.parameters.required, undefined, 'mqtt_list has no required members');
        return '9 schemas checked';
      });

      await suite.test('mqtt_connect creates and connects a profile against the isolated broker', async () => {
        const name = uniq('lab');
        const result = await labTools.mqtt_connect.execute({ url: lab.url, name, timeoutMs: 5000 });
        eq(result?.ok, true, `mqtt_connect: ${JSON.stringify(result)}`);
        eq(result.status, 'connected', 'connect status');
        labClientId = result.connection.id;
        labConnected = true;
        return `${name} connected to ${lab.url}`;
      });

      await suite.test('extra unknown properties are ignored by execute (schema closed, runtime not enforcing)', async () => {
        const result = await labTools.mqtt_list.execute({ bogus: 1, verbose: true });
        eq(result?.ok, true, 'mqtt_list ok with unknown property');
        eq(Array.isArray(result.connections), true, 'connections array');
        return 'unknown property ignored';
      });

      await suite.test('mqtt_publish via enum typo is refused instead of silently falling back', async () => {
        const result = await labTools.mqtt_publish.execute({ via: 'nope', topic: `${labBase}/fallback`, payload: 'x' });
        eq(result?.ok, false, `publish must refuse via:"nope": ${JSON.stringify(result)}`);
        eq(typeof result.error === 'string' && result.error.includes('unknown via'), true, 'error names the bad argument');
        return 'via:"nope" refused with a clear error';
      });

      await suite.test('qos outside 0-2 is clamped instead of refused', async () => {
        const result = await labTools.mqtt_publish.execute({ connection: labClientId, topic: `${labBase}/qos`, payload: 'x', qos: 9 });
        eq(result?.ok, true, 'publish ok');
        eq(result.qos, 2, 'clamped qos');
        return 'qos 9 -> 2';
      });

      await suite.test('missing required topic and payload are handled at execute, not by schema', async () => {
        const noTopic = await labTools.mqtt_publish.execute({});
        eq(noTopic?.ok, false, 'publish {} ok');
        assert(String(noTopic.error).includes('topic is required'), `error ${noTopic.error}`);
        const noPayload = await labTools.mqtt_publish.execute({ connection: labClientId, topic: `${labBase}/empty` });
        eq(noPayload?.ok, true, 'publish without payload');
        eq(noPayload.bytes, 0, 'empty payload bytes');
        return 'no topic -> error; no payload -> empty message (schema required not enforced)';
      });

      await suite.test('mqtt_subscribe then base64 payload round-trips through publish/messages', async () => {
        const subscribed = await labTools.mqtt_subscribe.execute({ connection: labClientId, topic: `${labBase}/#`, qos: 1 });
        eq(subscribed?.ok, true, `subscribe: ${JSON.stringify(subscribed)}`);
        const bytes = Buffer.from([0xff, 0x00, 0xfe, 0x01, 0x80, 0x7f]);
        const encoded = bytes.toString('base64');
        const topic = `${labBase}/b64`;
        const published = await labTools.mqtt_publish.execute({ connection: labClientId, topic, payload: encoded, encoding: 'base64', qos: 1 });
        eq(published?.ok, true, `publish base64: ${JSON.stringify(published)}`);
        eq(published.bytes, bytes.byteLength, 'published byte count');
        const read = await labTools.mqtt_wait.execute({ connection: labClientId, topic, direction: 'in', timeoutMs: 5000, lookbackMs: 5000, count: 1 });
        eq(read?.ok, true, `wait: ${JSON.stringify(read)}`);
        const entry = read.entries.find((row) => row.topic === topic && row.direction === 'in' && row.kind === 'client');
        assert(entry !== undefined, `no inbound b64 entry: ${JSON.stringify(read.entries)}`);
        eq(entry.encoding, 'base64', 'entry encoding');
        eq(entry.payload, encoded, 'entry payload');
        eq(entry.bytes, bytes.byteLength, 'entry byte count');
        return `${bytes.byteLength} binary bytes round-tripped as base64`;
      });

      await suite.test('utf8 payloads round-trip through publish/messages', async () => {
        const topic = `${labBase}/utf8`;
        const text = `hello-✓-${RUN}`;
        const published = await labTools.mqtt_publish.execute({ connection: labClientId, topic, payload: text, qos: 0 });
        eq(published?.ok, true, 'publish ok');
        const read = await labTools.mqtt_wait.execute({ connection: labClientId, topic, direction: 'in', timeoutMs: 5000, lookbackMs: 5000 });
        eq(read?.ok, true, `wait: ${JSON.stringify(read)}`);
        const entry = read.entries.find((row) => row.topic === topic);
        assert(entry !== undefined, 'utf8 entry missing');
        eq(entry.encoding, 'utf8', 'encoding');
        eq(entry.payload, text, 'payload');
        return 'utf8 payload intact';
      });

      await suite.test('mqtt_messages clamps limit 99999 to at most 200 rows', async () => {
        for (let index = 0; index < 260; index += 1) {
          lab.messages.push({ kind: 'client', direction: 'in', name: 'lab', clientId: labClientId, topic: `${labBase}/clamp`, payload: 'x', encoding: 'utf8', bytes: 1, qos: 0, retain: false });
        }
        const read = await labTools.mqtt_messages.execute({ topic: `${labBase}/clamp`, limit: 99999 });
        eq(read?.ok, true, 'messages ok');
        eq(read.count, 200, 'clamped count');
        eq(read.entries.length, 200, 'entries length');
        const lowest = await labTools.mqtt_messages.execute({ topic: `${labBase}/clamp`, limit: 0 });
        eq(lowest.count, 1, 'limit 0 -> 1');
        return 'limit 99999 -> 200; limit 0 -> 1';
      });

      await suite.test('mqtt_wait times out cleanly without matching messages', async () => {
        const result = await labTools.mqtt_wait.execute({ topic: `${labBase}/never-${Date.now()}`, lookbackMs: 0, timeoutMs: 1000 });
        eq(result?.ok, false, 'wait ok');
        eq(result.timedOut, true, 'timedOut');
        eq(result.count, 0, 'count');
        assert(result.waitedMs >= 900, `waitedMs ${result.waitedMs}`);
        eq(result.error, null, 'error');
        return `timedOut after ${result.waitedMs} ms`;
      });

      await suite.test('mqtt_broker status/stop/start/save transitions and closed-tool publish refusal', async () => {
        const status = await labTools.mqtt_broker.execute({ action: 'status' });
        eq(status?.ok, true, 'status ok');
        eq(status.broker.status, 'running', 'initial broker status');
        const stopped = await labTools.mqtt_broker.execute({ action: 'stop' });
        eq(stopped?.ok, true, 'stop ok');
        eq((await labTools.mqtt_broker.execute({ action: 'status' })).broker.status, 'stopped', 'status after stop');
        const refused = await labTools.mqtt_publish.execute({ via: 'broker', topic: `${labBase}/off`, payload: 'x' });
        eq(refused?.ok, false, 'publish while stopped');
        assert(String(refused.error).includes('start the broker'), `error ${refused.error}`);
        const started = await labTools.mqtt_broker.execute({ action: 'start' });
        eq(started?.ok, true, 'start ok');
        eq((await labTools.mqtt_broker.execute({ action: 'status' })).broker.status, 'running', 'status after start');
        const saved = await labTools.mqtt_broker.execute({ action: 'save', autoStart: true, allowAnonymous: true });
        eq(saved?.ok, true, 'save ok');
        await waitFor(() => lab.broker.view().autoStart === true, { timeout: 3000, label: 'autoStart applied' });
        const invalid = await labTools.mqtt_broker.execute({ action: 'nope' });
        eq(invalid?.ok, false, 'invalid action');
        assert(String(invalid.error).includes('action must be one of'), `error ${invalid.error}`);
        return 'stopped -> refused -> running -> saved; invalid action refused';
      });

      await suite.test('tool outputs survive a lossless-JSON trip (no undefined/NaN)', async () => {
        const results = [
          await labTools.mqtt_list.execute({ verbose: true }),
          await labTools.mqtt_messages.execute({ limit: 5 }),
          await labTools.mqtt_broker.execute({ action: 'status' }),
        ];
        for (const result of results) {
          const serialized = JSON.stringify(result);
          assert(serialized !== undefined, 'result is not serializable');
          eq(JSON.parse(serialized), result, 'result changed under JSON round-trip');
        }
        return '3 outputs lossless';
      });

      await suite.test('mqtt_list verbose exposes hasPassword but never the password value', async () => {
        await labTools.mqtt_broker.execute({ action: 'save', users: [{ username: 'lab-user', password: `lab-${brokerMarker}` }] });
        await waitFor(() => (lab.broker.view().users ?? []).some((user) => user.username === 'lab-user'), { timeout: 3000, label: 'lab user saved' });
        const listed = await labTools.mqtt_list.execute({ verbose: true });
        const serialized = JSON.stringify(listed);
        assert(!serialized.includes(`lab-${brokerMarker}`), 'mqtt_list leaked a password value');
        const user = listed.users.find((candidate) => candidate.username === 'lab-user');
        assert(user !== undefined, 'lab user missing from verbose list');
        eq(user.hasPassword, true, 'hasPassword');
        return 'verbose list has hasPassword:true and no password value';
      });
    } finally {
      await lab.dispose();
      lab = null;
    }

    /* ------------------------------------------------ 5. cleanup and final counts */
    suite.group('5. Cleanup and final counts');
    cleanupReport = await doCleanup();
  } finally {
    for (const stream of [...openStreams]) {
      try {
        await stream.close();
      } catch {
        /* ignore */
      }
    }
    if (lab !== null) {
      try {
        await lab.dispose();
      } catch {
        /* ignore */
      }
    }
    if (cleanupReport === null) {
      cleanupReport = await doCleanup();
    }
  }

  await suite.test('cleanup removed every suite- client and reported no errors', () => {
    const final = finalState();
    const leftovers = final.state.clients.filter((client) => String(client.name).startsWith('suite-') || String(client.id).startsWith('suite-'));
    eq(leftovers.map((client) => client.name), [], 'leftover suite- connections');
    eq(cleanupReport.errors, [], 'cleanup errors');
    return `${final.state.clients.length} client(s) remain: ${final.state.clients.map((client) => client.name).join(', ') || 'none'}`;
  });

  await suite.test('broker settings and status match the pre-suite snapshot', () => {
    const broker = finalState().state.broker;
    eq(broker.host, baseline.host, 'broker.host');
    eq(broker.port, baseline.port, 'broker.port');
    eq(Boolean(broker.autoStart), Boolean(baseline.autoStart), 'broker.autoStart');
    eq(Boolean(broker.allowAnonymous), Boolean(baseline.allowAnonymous), 'broker.allowAnonymous');
    eq(broker.status, baselineStatus, 'broker.status');
    const usernames = (broker.users ?? []).map((user) => user.username);
    eq(usernames, (baseline.users ?? []).map((user) => user.username), 'broker.users');
    if (baselineStatus === 'stopped') assert(broker.boundPort === null, `stopped broker bound to ${broker.boundPort}`);
    return `${broker.status} on ${broker.host}:${broker.port}, users ${usernames.length}`;
  });

  await suite.test('the live log is empty after cleanup', () => {
    const final = finalState();
    eq(final.messages.length, 0, 'live messages after cleanup');
    eq(final.events.length, 0, 'live events after cleanup');
    return 'messages and events cleared';
  });

  await suite.test('the state file no longer contains the test password markers', () => {
    assert(cleanupReport.fileText !== '', `state file was not read (${cleanupReport.errors.join('; ') || 'no errors'})`);
    for (const marker of markers) {
      assert(!cleanupReport.fileText.includes(marker), `state file still contains ${marker}`);
    }
    return `${cleanupReport.stateFile} free of markers`;
  });

  await suite.test('the live instance is still healthy after the whole suite', async () => {
    const started = Date.now();
    const { state } = await getState();
    eq(state.tools.registered, 9, 'tools after suite');
    const elapsed = Date.now() - started;
    assert(elapsed < 2000, `GET /state took ${elapsed} ms`);
    return `GET /state ${elapsed} ms, 9 tools registered`;
  });
});
