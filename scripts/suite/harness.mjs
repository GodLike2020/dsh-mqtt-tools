// Shared harness for the dsh-mqtt-tools test suites.
//
// Rules every suite follows:
//   * an isolated broker/client pair gets its own temporary DSH_HOME and its own
//     free port, so suites never touch the user's real state file or the live GUI;
//   * everything is created with the plugin's own classes (`lib/broker.js`,
//     `lib/clients.js`, `lib/store.js`) so the tests exercise shipping code;
//   * no green-by-luck: every axis a test claims is asserted explicitly, and a
//     failing suite exits non-zero and prints `SUITE-RESULT {json}` on its last line.
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import net from 'node:net';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import mqtt from 'mqtt';
import mqttPacket from 'mqtt-packet';

export const HERE = dirname(fileURLToPath(import.meta.url));
export const PLUGIN_DIR = dirname(dirname(HERE));
// The live GUI moves ports between DSH restarts; override with DSH_BASE when it does.
export const LIVE_BASE = process.env.DSH_BASE ?? 'http://127.0.0.1:6871';

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

export async function loadPluginLib(name) {
  return import(pathToFileURL(join(PLUGIN_DIR, 'lib', `${name}.js`)).href);
}

/**
 * A fully isolated engine: temporary DSH_HOME, real Store/RingLog/ClientManager/
 * BrokerManager, random port, anonymous access, all three protocol versions.
 * `dispose()` stops everything and removes the temporary directory.
 */
export async function isolatedEngine(options = {}) {
  const home = mkdtempSync(join(tmpdir(), 'dsh-mqtt-tools-suite-'));
  const previousHome = process.env.DSH_HOME;
  process.env.DSH_HOME = home;
  const { Store } = await loadPluginLib('store');
  const { RingLog } = await loadPluginLib('log');
  const { ClientManager } = await loadPluginLib('clients');
  const { BrokerManager } = await loadPluginLib('broker');
  const store = new Store();
  await store.ready;
  const messages = new RingLog(600);
  const events = new RingLog(200);
  const onChange = options.onChange ?? (() => {});
  const clients = new ClientManager({ store, messages, onChange });
  const broker = new BrokerManager({ store, messages, events, onChange });
  const port = options.port ?? (await freePort());
  const settings = broker.settings();
  await broker.saveSettings({
    host: '127.0.0.1',
    port,
    autoStart: false,
    allowAnonymous: options.allowAnonymous ?? true,
    protocols: options.protocols ?? ['3.1', '3.1.1', '5.0'],
    users: options.users ?? (Array.isArray(settings?.users) ? settings.users : []),
  });
  if (options.autoStart !== false) await broker.start();
  return {
    home,
    store,
    messages,
    events,
    clients,
    broker,
    port,
    url: `mqtt://127.0.0.1:${port}`,
    async dispose() {
      await Promise.allSettled([
        Promise.resolve().then(() => broker.stop()),
        Promise.resolve().then(() => broker.dispose?.()),
        Promise.resolve().then(() => clients.dispose?.()),
      ]);
      await Promise.resolve()
        .then(() => store.dispose?.())
        .catch(() => {});
      if (previousHome === undefined) delete process.env.DSH_HOME;
      else process.env.DSH_HOME = previousHome;
      rmSync(home, { recursive: true, force: true });
    },
  };
}

export function connectMqtt(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(url, { connectTimeout: 5000, reconnectPeriod: 0, ...opts });
    const timer = setTimeout(() => {
      client.end(true);
      reject(new Error(`MQTT connect timeout: ${url}`));
    }, 9000);
    const onError = (error) => {
      clearTimeout(timer);
      client.removeListener('connect', onConnect);
      client.end(true);
      reject(error);
    };
    const onConnect = () => {
      clearTimeout(timer);
      client.removeListener('error', onError);
      resolve(client);
    };
    client.once('error', onError);
    client.once('connect', onConnect);
  });
}

/** Resolves with the client's error (never throws) — for expected refusals. */
export function connectMqttError(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(url, { connectTimeout: 5000, reconnectPeriod: 0, ...opts });
    const timer = setTimeout(() => {
      client.end(true);
      reject(new Error(`MQTT connect timeout waiting for an error: ${url}`));
    }, 9000);
    client.once('error', (error) => {
      clearTimeout(timer);
      client.end(true);
      resolve(error);
    });
    client.once('connect', () => {
      clearTimeout(timer);
      client.end(true);
      reject(new Error(`expected a refused connection, but ${url} accepted it`));
    });
  });
}

export function endMqtt(client) {
  return new Promise((resolve) => {
    if (!client) return resolve();
    try {
      client.end(true, {}, () => resolve());
    } catch {
      resolve();
    }
  });
}

export function subscribeAsync(client, topic, qos = 0) {
  return new Promise((resolve, reject) => {
    client.subscribe(topic, { qos }, (error, granted) => (error ? reject(error) : resolve(granted)));
  });
}

export function publishAsync(client, topic, payload, options = {}) {
  return new Promise((resolve, reject) => {
    client.publish(topic, payload, options, (error) => (error ? reject(error) : resolve()));
  });
}

/** Records every delivered message so tests never race a one-shot listener. */
export function recorder(client) {
  const messages = [];
  const waiters = new Set();
  client.on('message', (topic, payload, packet) => {
    const entry = { topic, payload, packet };
    messages.push(entry);
    for (const waiter of [...waiters]) waiter(entry);
  });
  return {
    messages,
    clear() {
      messages.length = 0;
    },
    waitFor(predicate, { timeout = 5000, label = 'message' } = {}) {
      const found = messages.find(predicate);
      if (found) return Promise.resolve(found);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          waiters.delete(check);
          reject(new Error(`timeout waiting for ${label} (saw ${messages.length}: ${messages.map((m) => m.topic).join(', ')})`));
        }, timeout);
        const check = (entry) => {
          if (!predicate(entry)) return;
          clearTimeout(timer);
          waiters.delete(check);
          resolve(entry);
        };
        waiters.add(check);
      });
    },
  };
}

export function openSocket(port, host = '127.0.0.1') {
  return new Promise((resolve, reject) => {
    const socket = net.connect({ port, host });
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error('socket connect timeout'));
    }, 5000);
    socket.once('connect', () => {
      clearTimeout(timer);
      resolve(socket);
    });
    socket.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

/** Sequential packet reader over a raw socket — for byte-level protocol tests. */
export function packetReader(socket, protocolVersion = 4) {
  const queue = [];
  const waiters = [];
  let lastError = null;
  const parser = mqttPacket.parser({ protocolVersion });
  parser.on('packet', (packet) => {
    const waiter = waiters.shift();
    if (waiter) waiter({ packet });
    else queue.push(packet);
  });
  parser.on('error', (error) => {
    lastError = error;
    for (const waiter of waiters.splice(0)) waiter({ error });
  });
  socket.on('data', (chunk) => {
    try {
      parser.parse(chunk);
    } catch (error) {
      lastError = error;
    }
  });
  return {
    queue,
    get error() {
      return lastError;
    },
    next(timeout = 4000, label = 'packet') {
      if (queue.length) return Promise.resolve(queue.shift());
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error(`timeout waiting for ${label}${lastError ? ` (${lastError.message})` : ''}`));
        }, timeout);
        waiters.push((result) => {
          clearTimeout(timer);
          if (result.error) reject(result.error);
          else resolve(result.packet);
        });
      });
    },
  };
}

export function generatePacket(packet, protocolVersion = 4) {
  return mqttPacket.generate(packet, { protocolVersion });
}

export function assert(condition, message) {
  if (!condition) throw new Error(message);
}

export function eq(actual, expected, label = 'value') {
  const got = JSON.stringify(actual);
  const want = JSON.stringify(expected);
  if (got !== want) throw new Error(`${label}: expected ${want}, got ${got}`);
  return got;
}

export async function waitFor(predicate, { timeout = 5000, interval = 25, label = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  let lastError = null;
  while (Date.now() <= deadline) {
    try {
      const value = await predicate();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await sleep(interval);
  }
  throw new Error(`timeout after ${timeout} ms waiting for ${label}${lastError ? ` (last error: ${lastError.message})` : ''}`);
}

export class Suite {
  constructor(id, title, options = {}) {
    this.id = id;
    this.title = title;
    this.note = options.note ?? '';
    this.passed = 0;
    this.failed = 0;
    this.skipped = 0;
    this.failures = [];
    this.skips = [];
    this.startedAt = Date.now();
  }

  group(title) {
    console.log(`\n-- ${title}`);
  }

  async test(name, fn) {
    try {
      const detail = await fn();
      this.passed += 1;
      console.log(`  ok   ${name}${detail ? ` [${detail}]` : ''}`);
    } catch (error) {
      this.failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      this.failures.push({ name, message });
      console.log(`  FAIL ${name}: ${message}`);
    }
  }

  skip(name, reason) {
    this.skipped += 1;
    this.skips.push({ name, reason });
    console.log(`  skip ${name}: ${reason}`);
  }

  result() {
    return {
      suite: this.id,
      title: this.title,
      note: this.note,
      passed: this.passed,
      failed: this.failed,
      skipped: this.skipped,
      failures: this.failures,
      skips: this.skips,
      ms: Date.now() - this.startedAt,
    };
  }

  finish() {
    const result = this.result();
    console.log(`\n${this.title}: ${result.passed} passed, ${result.failed} failed, ${result.skipped} skipped in ${(result.ms / 1000).toFixed(1)} s`);
    console.log(`SUITE-RESULT ${JSON.stringify(result)}`);
    if (result.failed > 0) process.exitCode = 1;
    return result;
  }
}

/**
 * Run an async body with a suite-level guard: an unexpected throw inside one test
 * must never take down the rest of the suite, and every suite must dispose its
 * engine even when it fails.
 */
export async function runSuite(suite, body) {
  try {
    await body();
  } catch (error) {
    suite.failed += 1;
    suite.failures.push({ name: '(suite harness)', message: error instanceof Error ? error.stack ?? error.message : String(error) });
    console.log(`  FAIL (suite harness): ${error instanceof Error ? error.message : error}`);
  }
  return suite.finish();
}

// ---------------------------------------------------------------------------
// Live GUI helpers (only for suites that specifically test the installed plugin)
// ---------------------------------------------------------------------------

export async function apiJson(path, { method = 'GET', body, headers = {}, base = LIVE_BASE } = {}) {
  const response = await fetch(`${base}${path}`, {
    method,
    headers: {
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      ...(method === 'POST' ? { 'x-dsh-mqtt-tools': '1' } : {}),
      ...headers,
    },
    body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
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

export async function liveState(base = LIVE_BASE) {
  const response = await apiJson('/dsh-mqtt-tools/api/state', { base });
  if (!response.json?.ok) throw new Error(`live state unavailable (HTTP ${response.status})`);
  return response.json;
}

export async function liveCommand(cmd, input = {}, base = LIVE_BASE) {
  const response = await apiJson('/dsh-mqtt-tools/api/command', { method: 'POST', body: { cmd, ...input }, base });
  return response.json;
}

/** Snapshot of broker settings that can be fed back to broker.save (passwords are not restorable). */
export async function snapshotLiveBroker(base = LIVE_BASE) {
  const { state } = await liveState(base);
  const { host, port, autoStart, allowAnonymous, protocols, users } = state.broker;
  return { host, port, autoStart, allowAnonymous, protocols, users };
}

export async function restoreLiveBroker(snapshot, base = LIVE_BASE) {
  const result = await liveCommand('broker.save', { broker: snapshot }, base);
  if (!result?.ok) throw new Error(`broker restore failed: ${result?.error ?? 'unknown'}`);
}
