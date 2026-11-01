// Performance and footprint benchmark for dsh-mqtt-tools.
//
// Every phase runs in its own child process (node --expose-gc bench.mjs --phase=...)
// so heap deltas are not polluted by whatever the previous phase left behind, and
// so the cold module-load numbers are genuinely cold. The orchestrator (no flags)
// spawns the phases, collects their JSON results, and prints one report.
//
// Safety: the plugin is never mounted against the real DSH_HOME (every phase that
// mounts it points DSH_HOME at a fresh temp directory) and the broker always binds
// an ephemeral 127.0.0.1 port. Nothing here touches the live GUI.
//
// Only this file is written into the repository; temp homes are removed at the end
// of each phase.
import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import { dirname, join, relative } from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';
import zlib from 'node:zlib';

const HERE = dirname(fileURLToPath(import.meta.url));
const PLUGIN_DIR = dirname(dirname(HERE));
const BENCH_FILE = fileURLToPath(import.meta.url);
const INDEX_URL = pathToFileURL(join(PLUGIN_DIR, 'index.js')).href;
const HARNESS_URL = pathToFileURL(join(HERE, 'harness.mjs')).href;
const MB = 1024 * 1024;

const DEP_SPECS = [
  { label: '64 B', size: 64, n: 2000 },
  { label: '1 KiB', size: 1024, n: 1000 },
  { label: '64 KiB', size: 64 * 1024, n: 300 },
  { label: '512 KiB', size: 512 * 1024, n: 60 },
];

/* ------------------------------------------------------------------ utilities */

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function settle(rounds = 3) {
  for (let index = 0; index < rounds; index += 1) {
    if (typeof global.gc === 'function') global.gc();
    await sleep(15);
  }
}

const mem = () => process.memoryUsage();
const used = () => process.memoryUsage().heapUsed;
const mb = (bytes) => Number((bytes / MB).toFixed(2));

// V8 stores runs of one repeated character very cheaply (`'x'.repeat(8192)` x 600
// measures ~0.2 MB instead of the expected 4.9 MB), so payloads meant to exercise
// memory must carry unique content. Random bytes as latin1 produce a distinct
// one-byte JS string of exactly `size` chars.
function uniqueText(size) {
  return randomBytes(size).toString('latin1');
}
const round = (value, digits = 2) => (Number.isFinite(value) ? Number(value.toFixed(digits)) : null);

function resources() {
  try {
    return process.getActiveResourcesInfo();
  } catch {
    return [];
  }
}

function resourceSummary(list) {
  const counts = {};
  for (const type of list) counts[type] = (counts[type] ?? 0) + 1;
  return { total: list.length, counts, list: [...list] };
}

function percentile(values, p) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return round(sorted[index], 3);
}

function timer(ms, label) {
  const handle = setTimeout(() => {
    /* replaced below */
  }, ms);
  handle.unref?.();
  clearTimeout(handle);
  return new Promise((_, reject) => {
    const timeout = setTimeout(() => reject(new Error(`timeout: ${label}`)), ms);
    timeout.unref?.();
  });
}

function waitUntil(predicate, { timeout = 30000, interval = 10, label = 'condition' } = {}) {
  return new Promise((resolve, reject) => {
    const startedAt = Date.now();
    const tick = async () => {
      try {
        const value = await predicate();
        if (value) return resolve(value);
      } catch {
        /* keep polling */
      }
      if (Date.now() - startedAt > timeout) return reject(new Error(`timeout waiting for ${label}`));
      const handle = setTimeout(tick, interval);
      handle.unref?.();
    };
    tick();
  });
}

async function freePort() {
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

/** true when something accepts a TCP connection on the port. */
function tcpProbe(port, timeout = 400) {
  return new Promise((resolve) => {
    const socket = net.connect({ port, host: '127.0.0.1' });
    let settled = false;
    const finish = (value) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(value);
    };
    socket.setTimeout(timeout);
    socket.once('connect', () => finish(true));
    socket.once('error', () => finish(false));
    socket.once('timeout', () => finish(false));
  });
}

function packageVersion(name) {
  try {
    const file = name.startsWith('.') || name.includes('/') ? join(PLUGIN_DIR, name, 'package.json') : join(PLUGIN_DIR, 'node_modules', name, 'package.json');
    return JSON.parse(readFileSync(file, 'utf8')).version;
  } catch {
    return null;
  }
}

function dirSize(dir, options = {}) {
  let total = 0;
  let files = 0;
  let entries;
  try {
    entries = readdirSync(dir, { withFileTypes: true });
  } catch {
    return { bytes: 0, files: 0 };
  }
  for (const entry of entries) {
    if (options.skip?.includes(entry.name)) continue;
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      const inner = dirSize(full, options);
      total += inner.bytes;
      files += inner.files;
    } else if (entry.isFile()) {
      try {
        total += statSync(full).size;
        files += 1;
      } catch {
        /* raced with cleanup */
      }
    }
  }
  return { bytes: total, files };
}

/* --------------------------------------------------- full-plugin mount helpers */

/**
 * Minimal cordis stand-in: it records the cleanup functions, the routes and the
 * agent tools index.js registers, which is all `apply(ctx)` needs.
 */
function fakeCtx() {
  const disposers = [];
  const registeredTools = [];
  const routes = [];
  return {
    disposers,
    registeredTools,
    routes,
    effect(fn) {
      const dispose = fn();
      if (typeof dispose === 'function') disposers.push(dispose);
    },
    webServer: {
      getOrigin: () => null,
      register(route) {
        routes.push(route);
        return () => {};
      },
    },
    tools: {
      register(tool) {
        registeredTools.push(tool.name);
        return () => {};
      },
    },
  };
}

/** Ref'd Timeout handles with their delays, to name what an engine leaves behind. */
function activeTimeouts() {
  try {
    return process
      ._getActiveHandles()
      .filter((handle) => handle?.constructor?.name === 'Timeout')
      .map((handle) => handle._idleTimeout ?? null);
  } catch {
    return [];
  }
}

/** Ref'd timers per getActiveResourcesInfo (what keeps the loop alive). */
function refdTimeoutCount(list = resources()) {
  return list.filter((type) => type === 'Timeout').length;
}

/** Process-level listeners, to check an agent plugin does not leave any behind. */
function listenerSnapshot() {
  const out = {};
  for (const name of process.eventNames()) {
    const count = process.listenerCount(name);
    if (count > 0) out[String(name)] = count;
  }
  return out;
}

async function prepareTempHome(state = null) {
  const home = mkdtempSync(join(os.tmpdir(), 'dsh-mqtt-bench-'));
  process.env.DSH_HOME = home;
  if (state !== null) writeFileSync(join(home, 'dsh-mqtt-tools.json'), JSON.stringify(state));
  return home;
}

function cleanupHome(home) {
  try {
    rmSync(home, { recursive: true, force: true });
  } catch {
    /* temp dir cleanup is best effort */
  }
}

function defaultState(port) {
  return {
    version: 1,
    clients: [],
    broker: {
      host: '127.0.0.1',
      port,
      autoStart: true,
      allowAnonymous: true,
      protocols: ['3.1', '3.1.1', '5.0'],
      users: [],
    },
    log: { showSystem: false },
  };
}

/* ------------------------------------------------------------------- phases */

/** (a) cold module load: index.js graph, then each lazy dependency. */
async function phaseLoad() {
  await settle(2);
  const base = mem();
  const startedAt = performance.now();
  const mod = await import(INDEX_URL);
  const importMs = performance.now() - startedAt;
  await settle(2);
  const afterIndex = mem();

  const loads = {};
  for (const dep of ['mqtt', 'aedes', 'mqtt-packet']) {
    const t0 = performance.now();
    await import(dep);
    loads[dep] = { ms: round(performance.now() - t0, 1) };
    await settle(1);
    loads[dep].heapUsedAfter = used();
  }

  return {
    node: process.version,
    v8: process.versions.v8,
    gcExposed: typeof global.gc === 'function',
    imports: {
      indexMs: round(importMs, 1),
      mqttMs: loads.mqtt.ms,
      aedesMs: loads.aedes.ms,
      mqttPacketMs: loads['mqtt-packet'].ms,
    },
    heaps: {
      base: base.heapUsed,
      afterIndex: afterIndex.heapUsed,
      afterMqtt: loads.mqtt.heapUsedAfter,
      afterAedes: loads.aedes.heapUsedAfter,
      afterMqttPacket: loads['mqtt-packet'].heapUsedAfter,
    },
    heapDeltas: {
      index: afterIndex.heapUsed - base.heapUsed,
      mqtt: loads.mqtt.heapUsedAfter - afterIndex.heapUsed,
      aedes: loads.aedes.heapUsedAfter - loads.mqtt.heapUsedAfter,
      mqttPacket: loads['mqtt-packet'].heapUsedAfter - loads.aedes.heapUsedAfter,
    },
    exportsApply: typeof mod.apply,
    versions: {
      mqtt: packageVersion('mqtt'),
      aedes: packageVersion('aedes'),
      'mqtt-packet': packageVersion('mqtt-packet'),
    },
  };
}

/** First-use cost of one dependency, measured in a process that has never loaded it. */
async function phaseDep(dep) {
  await settle(2);
  const base = mem();
  const t0 = performance.now();
  const mod = await import(dep);
  const ms = performance.now() - t0;
  await settle(2);
  const after = mem();
  return {
    dep,
    version: packageVersion(dep) ?? dep,
    firstImportMs: round(ms, 1),
    heapDelta: after.heapUsed - base.heapUsed,
    rssDelta: after.rss - base.rss,
    exportsSample: Object.keys(mod).slice(0, 6),
  };
}

/** (a) heap cost of mounting the whole plugin via apply(ctx). */
async function phaseMount() {
  const home = await prepareTempHome();
  const mod = await import(INDEX_URL);
  await settle(3);
  const base = mem();
  const listenersBefore = listenerSnapshot();
  const ctx = fakeCtx();
  const startedAt = performance.now();
  mod.apply(ctx);
  const applyMs = performance.now() - startedAt;
  // clients.warm() and broker.warm() import mqtt/aedes in the background; joining
  // those imports here makes "mounted" mean "everything the plugin will load".
  await Promise.all([import('mqtt').catch(() => {}), import('aedes').catch(() => {})]);
  await sleep(250);
  await settle(3);
  const mounted = mem();
  const resourcesAfterMount = resourceSummary(resources());
  const disposeStarted = performance.now();
  for (const dispose of [...ctx.disposers].reverse()) {
    try {
      dispose();
    } catch {
      /* collected by the caller */
    }
  }
  const disposeMs = performance.now() - disposeStarted;
  await sleep(300);
  await settle(3);
  const disposed = mem();
  const resourcesAfterDispose = resourceSummary(resources());
  cleanupHome(home);
  return {
    applyMs: round(applyMs, 2),
    toolsRegistered: ctx.registeredTools.length,
    toolNames: ctx.registeredTools,
    routesRegistered: ctx.routes.length,
    heapDeltaMountBytes: mounted.heapUsed - base.heapUsed,
    heapDeltaMountMb: mb(mounted.heapUsed - base.heapUsed),
    heapAfterDisposeMb: mb(disposed.heapUsed - base.heapUsed),
    heapReturnedBytes: mounted.heapUsed - disposed.heapUsed,
    disposeMs: round(disposeMs, 2),
    resourcesAfterMount,
    resourcesAfterDispose,
    timersAfterMount: activeTimeouts(),
    timersAfterDispose: activeTimeouts(),
    refdTimeoutsAfterMount: refdTimeoutCount(resourcesAfterMount.list),
    refdTimeoutsAfterDispose: refdTimeoutCount(resourcesAfterDispose.list),
    listenersBeforeMount: listenersBefore,
    listenersAfterMount: listenerSnapshot(),
    listenersAfterDispose: listenerSnapshot(),
    // The orchestrator treats a child that does not exit on its own as "left a
    // ref'd handle behind"; this object is written before that can happen.
    naturalExitCheckedByOrchestrator: true,
  };
}

/** (i) full plugin with a running broker: idle handles, dispose latency, port release. */
async function phaseLifecycle() {
  const port = await freePort();
  const home = await prepareTempHome(defaultState(port));
  const mod = await import(INDEX_URL);
  await settle(3);
  const base = mem();
  const ctx = fakeCtx();
  const applyStarted = performance.now();
  mod.apply(ctx);
  const applyMs = performance.now() - applyStarted;

  let listenMs = null;
  try {
    await waitUntil(() => tcpProbe(port), { timeout: 15000, interval: 15, label: `broker on ${port}` });
    listenMs = performance.now() - applyStarted;
  } catch {
    /* reported below */
  }
  await sleep(200);
  await settle(3);
  const running = mem();
  const resourcesAfterStart = resourceSummary(resources());

  const disposeStarted = performance.now();
  for (const dispose of [...ctx.disposers].reverse()) {
    try {
      dispose();
    } catch {
      /* disposal continues */
    }
  }
  const disposeSyncMs = performance.now() - disposeStarted;

  let portClosedMs = null;
  const closeStarted = performance.now();
  try {
    await waitUntil(async () => !(await tcpProbe(port, 150)), { timeout: 8000, interval: 10, label: 'port release' });
    portClosedMs = performance.now() - closeStarted;
  } catch {
    portClosedMs = null;
  }
  await sleep(300);
  await settle(3);
  const disposed = mem();
  const resourcesAfterDispose = resourceSummary(resources());
  cleanupHome(home);
  return {
    applyMs: round(applyMs, 2),
    brokerListenMs: round(listenMs, 2),
    heapDeltaRunningBytes: running.heapUsed - base.heapUsed,
    heapDeltaRunningMb: mb(running.heapUsed - base.heapUsed),
    disposeCallMs: round(disposeSyncMs, 2),
    portReleasedMs: round(portClosedMs, 2),
    heapAfterDisposeMb: mb(disposed.heapUsed - base.heapUsed),
    resourcesAfterStart,
    resourcesAfterDispose,
    timersAfterStart: activeTimeouts(),
    timersAfterDispose: activeTimeouts(),
    refdTimeoutsAfterStart: refdTimeoutCount(resourcesAfterStart.list),
    refdTimeoutsAfterDispose: refdTimeoutCount(resourcesAfterDispose.list),
    listenersAfterStart: listenerSnapshot(),
    listenersAfterDispose: listenerSnapshot(),
    naturalExitCheckedByOrchestrator: true,
  };
}

/** (c) heap cost of 1/10/50 connected mqtt.js clients against the embedded broker. */
async function phaseConn(n) {
  const harness = await import(HARNESS_URL);
  const engine = await harness.isolatedEngine();
  const clients = [];
  try {
    // Warm the connect path (and let V8 finish JIT-ing it) against a throwaway
    // client, so the delta below is per-connection state rather than first-use
    // code/JIT allocation, which is several MB on the first TCP connect.
    const warm = await harness.connectMqtt(engine.url, { clientId: 'conn-warm' });
    await harness.subscribeAsync(warm, 'conn/#', 0);
    await harness.endMqtt(warm);
    await sleep(100);
    await settle(4);
    const base = mem();
    const connectStarted = performance.now();
    for (let index = 0; index < n; index += 1) {
      clients.push(await harness.connectMqtt(engine.url, { clientId: `conn-${n}-${index}` }));
    }
    const connectMs = performance.now() - connectStarted;
    await harness.waitFor(() => engine.broker.view().clientCount === n, { timeout: 15000, interval: 15, label: `${n} broker clients` });
    await sleep(150);
    await settle(3);
    const after = mem();
    return {
      n,
      connectMs: round(connectMs, 1),
      connectMsPerClient: round(connectMs / n, 2),
      heapDeltaBytes: after.heapUsed - base.heapUsed,
      heapDeltaMb: mb(after.heapUsed - base.heapUsed),
      bytesPerConnection: Math.round((after.heapUsed - base.heapUsed) / n),
      rssDeltaMb: mb(after.rss - base.rss),
      brokerClientCount: engine.broker.view().clientCount,
      resources: resourceSummary(resources()),
    };
  } finally {
    for (const client of clients) await harness.endMqtt(client);
    await engine.dispose();
  }
}

// Shared message-delivery recorder: counts per client and pools latencies.
function deliveryTracker(clients, total, label) {
  const latencies = [];
  const publishAt = new Float64Array(total);
  const perClient = new Array(clients.length).fill(0);
  let delivered = 0;
  let resolveAll;
  let rejectAll;
  const all = new Promise((resolve, reject) => {
    resolveAll = resolve;
    rejectAll = reject;
  });
  const timeout = setTimeout(() => rejectAll(new Error(`timeout: ${label}`)), 180000);
  timeout.unref?.();
  clients.forEach((client, clientIndex) => {
    client.on('message', (topic) => {
      const index = Number(String(topic).replace(/^.*\//, ''));
      const latency = performance.now() - publishAt[index];
      if (Number.isFinite(latency)) latencies.push(latency);
      perClient[clientIndex] += 1;
      delivered += 1;
      if (delivered >= total * clients.length) {
        clearTimeout(timeout);
        resolveAll();
      }
    });
  });
  return { publishAt, latencies, perClient, all, delivered: () => delivered };
}

/** (b) broker.publish throughput + end-to-end in-process latency for N=1/10/50. */
async function phaseThroughput() {
  const harness = await import(HARNESS_URL);
  const out = {};
  for (const n of [1, 10, 50]) {
    const engine = await harness.isolatedEngine();
    const clients = [];
    try {
      for (let index = 0; index < n; index += 1) {
        const client = await harness.connectMqtt(engine.url, { clientId: `thr-${n}-${index}` });
        clients.push(client);
        await harness.subscribeAsync(client, 'bench/#', 0);
      }
      const total = 2000;
      // Warm the publish/fan-out path first: first-use JIT and lazy aedes internals
      // otherwise show up as several MB of heap, swamping the actual log growth.
      const warmPayload = 'w'.repeat(64);
      for (let index = 0; index < 200; index += 1) {
        await engine.broker.publish({ topic: `bench/warm/${index}`, payload: warmPayload });
      }
      await sleep(150);
      await settle(3);
      const tracker = deliveryTracker(clients, total, `${n} clients x ${total}`);
      const base = mem();
      const startedAt = performance.now();
      for (let index = 0; index < total; index += 1) {
        tracker.publishAt[index] = performance.now();
        await engine.broker.publish({ topic: `bench/${index}`, payload: 'x'.repeat(64) });
      }
      await tracker.all;
      const elapsed = performance.now() - startedAt;
      await settle(2);
      const after = mem();
      out[`n${n}`] = {
        clients: n,
        published: total,
        deliveries: tracker.delivered(),
        elapsedMs: round(elapsed, 1),
        msgsPerSec: Math.round(total / (elapsed / 1000)),
        deliveriesPerSec: Math.round((total * n) / (elapsed / 1000)),
        p50Ms: percentile(tracker.latencies, 50),
        p99Ms: percentile(tracker.latencies, 99),
        maxMs: percentile(tracker.latencies, 100),
        heapDeltaMb: mb(after.heapUsed - base.heapUsed),
        ringEntries: engine.messages.tail(10000).length,
        minDeliveredPerClient: Math.min(...tracker.perClient),
      };
    } finally {
      for (const client of clients) await harness.endMqtt(client);
      await engine.dispose();
    }
  }
  return out;
}

/** (d) payload size sweep: throughput, latency and heap impact. */
async function phasePayload() {
  const harness = await import(HARNESS_URL);
  const out = {};
  for (const spec of DEP_SPECS) {
    const engine = await harness.isolatedEngine();
    const clients = [];
    try {
      const client = await harness.connectMqtt(engine.url, { clientId: `pay-${spec.size}` });
      clients.push(client);
      await harness.subscribeAsync(client, 'pay/#', 0);
      const tracker = deliveryTracker(clients, spec.n, `payload ${spec.label}`);
      // One buffer reused for every publish with a message stamp in its header:
      // random (incompressible) content so retained/logged copies cost real memory,
      // and no 512 KiB x n payload-spray skewing the heap delta.
      const template = randomBytes(spec.size);
      await settle(2);
      const base = mem();
      const startedAt = performance.now();
      for (let index = 0; index < spec.n; index += 1) {
        template.writeUInt32BE(index % 4294967296, 0);
        tracker.publishAt[index] = performance.now();
        await engine.broker.publish({ topic: `pay/${index}`, payload: template });
      }
      await tracker.all;
      const elapsed = performance.now() - startedAt;
      const peak = mem();
      await settle(3);
      const after = mem();
      const ring = engine.messages.tail(10000);
      const ringPayloadChars = ring.reduce((sum, entry) => sum + String(entry.payload ?? '').length, 0);
      out[spec.label] = {
        sizeBytes: spec.size,
        messages: spec.n,
        totalMib: round((spec.size * spec.n) / MB, 2),
        elapsedMs: round(elapsed, 1),
        msgsPerSec: Math.round(spec.n / (elapsed / 1000)),
        mibPerSec: round((spec.size * spec.n) / MB / (elapsed / 1000), 1),
        p50Ms: percentile(tracker.latencies, 50),
        p99Ms: percentile(tracker.latencies, 99),
        peakHeapDeltaMb: mb(peak.heapUsed - base.heapUsed),
        peakExternalDeltaMb: mb(peak.external - base.external),
        heapDeltaAfterGcMb: mb(after.heapUsed - base.heapUsed),
        externalAfterGcMb: mb(after.external - base.external),
        rssDeltaAfterGcMb: mb(after.rss - base.rss),
        ringEntries: ring.length,
        ringPayloadChars,
        lastRingPayloadChars: ring.length === 0 ? 0 : String(ring[ring.length - 1].payload ?? '').length,
        logTruncated: ring.length > 0 && /…$/.test(String(ring[ring.length - 1].payload ?? '')),
      };
    } finally {
      for (const client of clients) await harness.endMqtt(client);
      await engine.dispose();
    }
  }
  return out;
}

/** (e) retained-message memory: 100 and 1000 retained x 1 KiB. */
async function phaseRetained() {
  const harness = await import(HARNESS_URL);
  const out = {};
  for (const count of [100, 1000]) {
    const engine = await harness.isolatedEngine();
    try {
      // Unique random 1 KiB buffers: aedes keeps a reference to each retained
      // payload, and Buffer storage lands in `external`, not `heapUsed`. The
      // buffers are allocated *before* the baseline so the delta is what the
      // broker retained, not the payloads the benchmark itself still holds.
      const payloads = Array.from({ length: count }, () => randomBytes(1024));
      await sleep(50);
      await settle(3);
      const base = mem();
      const startedAt = performance.now();
      for (let index = 0; index < count; index += 1) {
        await engine.broker.publish({ topic: `ret/${index}`, payload: payloads[index], retain: true });
      }
      const elapsed = performance.now() - startedAt;
      await sleep(100);
      // The RingLog also holds the last 600 publishes; clear it so the delta is
      // the retained set (aedes' retained map), not the message log.
      engine.messages.clear();
      await settle(4);
      const after = mem();
      const stats = engine.broker.view().stats;
      const heapDelta = after.heapUsed - base.heapUsed;
      const externalDelta = after.external - base.external;
      out[count] = {
        count,
        elapsedMs: round(elapsed, 1),
        publishMsEach: round(elapsed / count, 2),
        retainedPayloadMib: round((count * 1024) / MB, 2),
        heapDeltaBytes: heapDelta,
        heapDeltaMb: mb(heapDelta),
        externalDeltaMb: mb(externalDelta),
        arrayBuffersDeltaMb: mb(after.arrayBuffers - base.arrayBuffers),
        bytesPerRetained: Math.round((heapDelta + Math.max(0, externalDelta)) / count),
        published: stats.published,
        ringEntries: engine.messages.tail(10000).length,
      };
    } finally {
      await engine.dispose();
    }
  }
  return out;
}

/** (f) RingLog: 10k pushes time, and memory held by a full 600-entry log of 8 KiB payloads. */
async function phaseLog() {
  const { RingLog, describePayload } = await import(pathToFileURL(join(PLUGIN_DIR, 'lib', 'log.js')).href);
  await settle(3);
  const base = used();
  const log = new RingLog(600);
  const pushes = 10000;
  const smallPayload = uniqueText(256);
  // Timing loop: payload object reused so the number is the cost of push() itself.
  const startedAt = performance.now();
  for (let index = 0; index < pushes; index += 1) {
    log.push({
      kind: 'broker',
      direction: 'in',
      clientId: 'bench-client',
      name: 'bench-client',
      topic: `bench/log/${index}`,
      qos: 0,
      retain: false,
      payload: smallPayload,
      encoding: 'utf8',
      bytes: 256,
    });
  }
  const pushesMs = performance.now() - startedAt;
  await settle(4);
  const heldSmall = used() - base;

  // Realistic full ring: 600 distinct 8 KiB text payloads (index.js caps the ring
  // at 600 entries and truncates each payload at 8192 chars).
  const pool = Array.from({ length: 600 }, () => uniqueText(8192));
  for (let index = 0; index < pool.length; index += 1) {
    log.push({
      kind: 'broker',
      direction: 'in',
      clientId: 'bench-client',
      name: 'bench-client',
      topic: `bench/log/big/${index}`,
      qos: 0,
      retain: false,
      payload: pool[index],
      encoding: 'utf8',
      bytes: 8192,
    });
  }
  await settle(4);
  const held = used() - base;

  // A second 10k burst must not grow the log (the ring evicts, it does not grow).
  const secondStarted = performance.now();
  for (let index = 0; index < pushes; index += 1) {
    log.push({
      topic: `bench/log/again/${index}`,
      payload: pool[index % pool.length],
      encoding: 'utf8',
      bytes: 8192,
    });
  }
  const secondMs = performance.now() - secondStarted;
  await settle(4);
  const heldAfterSecond = used() - base;

  // The plugin does not push raw payloads: broker.attach()/clients.receive()
  // store describePayload(payload), whose 8192-char truncation is only visual.
  // V8 sliced strings keep the whole parent, so a ring entry that *shows* 8 KiB
  // can pin the full payload. Measure that with plugin-realistic entries.
  log.clear();
  await settle(4);
  const truncBase = used();
  const truncEntries = 600;
  const truncPayloadSize = 64 * 1024;
  for (let index = 0; index < truncEntries; index += 1) {
    log.push({
      kind: 'broker',
      direction: 'in',
      topic: `bench/trunc/${index}`,
      qos: 0,
      ...describePayload(randomBytes(truncPayloadSize)),
    });
  }
  await settle(4);
  const truncHeld = used() - truncBase;
  const truncSample = log.items[log.items.length - 1];
  const truncPerEntry = Math.round(truncHeld / truncEntries);
  // Worst case: index.js's ring of 600 filled with 512 KiB binary messages.
  const worstCase600x512KiBmb = mb((600 * 699204));

  return {
    pushes,
    pushesMs: round(pushesMs, 1),
    perPushUs: round((pushesMs * 1000) / pushes, 2),
    pushesPerSec: Math.round(pushes / (pushesMs / 1000)),
    ringLimit: log.limit,
    ringItems: log.items.length,
    heldSmallBytes: heldSmall,
    heldBytes: held,
    heldMb: mb(held),
    perEntryBytes: Math.round(held / pool.length),
    secondBurstMs: round(secondMs, 1),
    secondBurstHasPayloadGeneration: true,
    heldAfterSecondBurstMb: mb(heldAfterSecond),
    bounded: log.items.length === log.limit,
    truncation: {
      entries: truncEntries,
      payloadSize: truncPayloadSize,
      visibleChars: String(truncSample?.payload ?? '').length,
      heldBytes: truncHeld,
      heldMb: mb(truncHeld),
      perEntryBytes: truncPerEntry,
      // Full base64/utf8 parent the slice keeps alive vs. the 8192-char text shown.
      pinnedRatio: round(truncPerEntry / 8192, 2),
      worstCase600x512KiBmb,
    },
  };
}

/** (g) SSE fan-out through a real HTTP server: 5 streams x 2000 frames. */
async function phaseSse() {
  const { Hub } = await import(pathToFileURL(join(PLUGIN_DIR, 'lib', 'hub.js')).href);
  const hub = new Hub();
  const server = http.createServer((req, res) => hub.attach(req, res, { hello: true, state: {} }));
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const port = server.address().port;
  const url = `http://127.0.0.1:${port}/events`;
  const streams = 5;
  const frames = 2000;
  const controllers = [];
  const workers = [];
  const counts = new Array(streams).fill(0);
  let helloCount = 0;
  let helloResolve;
  const allHello = new Promise((resolve) => {
    helloResolve = resolve;
  });
  const streamDone = [];
  const allFrames = Promise.all(
    Array.from({ length: streams }, (_, index) => {
      const done = new Promise((resolve, reject) => {
        streamDone[index] = { resolve, reject };
      });
      controllers.push(new AbortController());
      workers.push(
        (async () => {
          const response = await fetch(url, { signal: controllers[index].signal });
          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';
          let bytes = 0;
          try {
            for (;;) {
              const { done: finished, value } = await reader.read();
              if (finished) break;
              bytes += value.byteLength;
              buffer += decoder.decode(value, { stream: true });
              for (;;) {
                const boundary = buffer.indexOf('\n\n');
                if (boundary < 0) break;
                const frame = buffer.slice(0, boundary);
                buffer = buffer.slice(boundary + 2);
                if (frame.startsWith('event: hello')) {
                  helloCount += 1;
                  if (helloCount === streams) helloResolve();
                } else if (frame.startsWith('event: message')) {
                  counts[index] += 1;
                  if (counts[index] === frames) streamDone[index].resolve(bytes);
                }
              }
            }
          } catch (error) {
            if (error?.name !== 'AbortError') streamDone[index].reject(error);
          }
        })(),
      );
      return done;
    }),
  );
  const receivedBytes = await (async () => {
    await allHello;
    await settle(3);
    const base = mem();
    const frame = {
      kind: 'message',
      direction: 'in',
      clientId: 'bench',
      name: 'bench',
      topic: 'fan/0',
      qos: 0,
      retain: false,
      encoding: 'utf8',
      bytes: 200,
      payload: 'z'.repeat(200),
      seq: 0,
      ts: Date.now(),
    };
    const startedAt = performance.now();
    for (let index = 0; index < frames; index += 1) {
      frame.seq = index;
      frame.ts = Date.now();
      hub.broadcast('message', frame);
    }
    const loopMs = performance.now() - startedAt;
    const peak = used();
    const bytes = await Promise.race([allFrames, timer(120000, 'sse frames')]);
    const elapsed = performance.now() - startedAt;
    await settle(3);
    const after = mem();
    return {
      base,
      peak,
      after,
      loopMs,
      elapsed,
      bytes: bytes.reduce ? bytes : 0,
      totalBytes: Array.isArray(bytes) ? bytes.reduce((sum, value) => sum + value, 0) : 0,
    };
  })();
  for (const controller of controllers) {
    try {
      controller.abort();
    } catch {
      /* already closed */
    }
  }
  await Promise.allSettled(workers);
  hub.dispose();
  await new Promise((resolve) => server.close(resolve));
  server.closeAllConnections?.();
  const loopMs = receivedBytes.loopMs;
  const elapsed = receivedBytes.elapsed;
  return {
    streams,
    frames,
    totalFrames: streams * frames,
    loopMs: round(loopMs, 1),
    perFrameUs: round((loopMs * 1000) / frames, 2),
    elapsedMs: round(elapsed, 1),
    framesPerSec: Math.round((streams * frames) / (elapsed / 1000)),
    socketBytesPerStream: Math.round(receivedBytes.totalBytes / streams),
    peakHeapDeltaMb: mb(receivedBytes.peak - receivedBytes.base.heapUsed),
    heapAfterGcMb: mb(receivedBytes.after.heapUsed - receivedBytes.base.heapUsed),
    rssDeltaMb: mb(receivedBytes.after.rss - receivedBytes.base.rss),
    frameCounts: counts,
  };
}

/** (h) client.js on-disk/gzip size and the npm pack footprint of the whole package. */
async function phaseBundle() {
  const clientFile = join(PLUGIN_DIR, 'client.js');
  const clientBytes = statSync(clientFile).size;
  const clientBuffer = readFileSync(clientFile);
  const gzip9 = zlib.gzipSync(clientBuffer, { level: 9 }).byteLength;
  const gzip1 = zlib.gzipSync(clientBuffer, { level: 1 }).byteLength;
  const brotli = zlib.brotliCompressSync(clientBuffer, {
    params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11 },
  }).byteLength;

  const temp = mkdtempSync(join(os.tmpdir(), 'dsh-mqtt-pack-'));
  let pack = null;
  let packError = null;
  try {
    const result = spawnSync('npm', ['pack', PLUGIN_DIR, '--dry-run', '--json', '--ignore-scripts'], {
      cwd: temp,
      encoding: 'utf8',
      shell: true,
      timeout: 120000,
      maxBuffer: 16 * MB,
    });
    const text = `${result.stdout ?? ''}\n${result.stderr ?? ''}`;
    const start = text.indexOf('[');
    const end = text.lastIndexOf(']');
    if (start >= 0 && end > start) {
      const parsed = JSON.parse(text.slice(start, end + 1));
      pack = parsed[0] ?? null;
    } else {
      packError = text.slice(-500);
    }
  } catch (error) {
    packError = String(error?.message ?? error);
  } finally {
    cleanupHome(temp);
  }

  const files = pack?.files ?? [];
  const jsFiles = files.filter((entry) => entry.path.endsWith('.js'));
  const jsBytes = jsFiles.reduce((sum, entry) => sum + entry.size, 0);
  const hostJsFiles = jsFiles.filter((entry) => entry.path !== 'client.js');
  const hostJsBytes = hostJsFiles.reduce((sum, entry) => sum + entry.size, 0);
  const docsBytes = files.filter((entry) => entry.path.startsWith('docs/')).reduce((sum, entry) => sum + entry.size, 0);

  const nodeModules = dirSize(join(PLUGIN_DIR, 'node_modules'), { skip: ['.bin', '.cache'] });

  return {
    node: process.version,
    clientJsBytes: clientBytes,
    clientGzip9Bytes: gzip9,
    clientGzip1Bytes: gzip1,
    clientBrotli11Bytes: brotli,
    clientGzipRatio: round(gzip9 / clientBytes, 3),
    npmPack: pack === null
      ? { error: packError }
      : {
          name: pack.name,
          version: pack.version,
          tarballBytes: pack.size,
          unpackedBytes: pack.unpackedSize,
          fileCount: pack.entryCount ?? files.length,
          clientJsBytes: files.find((entry) => entry.path === 'client.js')?.size ?? null,
          hostJsBytes,
          jsOnlyBytes: jsBytes,
          docsBytes,
          otherBytes: pack.unpackedSize - jsBytes - docsBytes,
        },
    nodeModulesBytes: nodeModules.bytes,
    nodeModulesMb: mb(nodeModules.bytes),
    nodeModulesFiles: nodeModules.files,
    hostDependencyVersions: {
      mqtt: packageVersion('mqtt'),
      aedes: packageVersion('aedes'),
      'mqtt-packet': packageVersion('mqtt-packet'),
    },
  };
}

/** Stress: 200 retained x 1 KiB, 20 clients, burst of 500 x 1 KiB; heap growth/recovery. */
async function phaseStress() {
  const harness = await import(HARNESS_URL);
  const engine = await harness.isolatedEngine();
  const clients = [];
  try {
    for (let index = 0; index < 20; index += 1) {
      const client = await harness.connectMqtt(engine.url, { clientId: `stress-${index}` });
      clients.push(client);
      await harness.subscribeAsync(client, 'stress/#', 0);
    }
    const retainedPayloads = Array.from({ length: 200 }, () => randomBytes(1024));
    for (let index = 0; index < retainedPayloads.length; index += 1) {
      await engine.broker.publish({ topic: `ret/${index}`, payload: retainedPayloads[index], retain: true });
    }
    const tracker = deliveryTracker(clients, 500, 'stress burst');
    const burstTemplate = randomBytes(1024);
    await sleep(150);
    await settle(4);
    const base = mem();
    const burstStarted = performance.now();
    for (let index = 0; index < 500; index += 1) {
      burstTemplate.writeUInt32BE(index, 0);
      tracker.publishAt[index] = performance.now();
      await engine.broker.publish({ topic: `stress/${index}`, payload: burstTemplate });
    }
    const publishMs = performance.now() - burstStarted;
    await Promise.race([tracker.all, timer(180000, 'stress delivery')]);
    const deliveredMs = performance.now() - burstStarted;
    const peak = mem();
    await settle(6);
    const recovered = mem();
    const resourcesNow = resourceSummary(resources());
    return {
      clients: 20,
      retained: 200,
      retainedBytes: 200 * 1024,
      burstMessages: 500,
      burstPayloadBytes: 1024,
      deliveries: tracker.delivered(),
      publishMs: round(publishMs, 1),
      deliveredMs: round(deliveredMs, 1),
      msgsPerSec: Math.round(500 / (publishMs / 1000)),
      peakHeapDeltaBytes: peak.heapUsed - base.heapUsed,
      peakHeapDeltaMb: mb(peak.heapUsed - base.heapUsed),
      peakExternalDeltaMb: mb(peak.external - base.external),
      recoveredHeapDeltaBytes: recovered.heapUsed - base.heapUsed,
      recoveredHeapDeltaMb: mb(recovered.heapUsed - base.heapUsed),
      recoveredExternalDeltaMb: mb(recovered.external - base.external),
      recoveredPercentOfPeak: round(((recovered.heapUsed - base.heapUsed) / Math.max(1, peak.heapUsed - base.heapUsed)) * 100, 1),
      transientPeakMb: mb(peak.heapUsed - recovered.heapUsed),
      ringEntries: engine.messages.tail(10000).length,
      brokerClientCount: engine.broker.view().clientCount,
      brokerStats: engine.broker.view().stats,
      resources: resourcesNow,
    };
  } finally {
    for (const client of clients) await harness.endMqtt(client);
    await engine.dispose();
  }
}

/** (j) full engine dispose with 10 connected clients and a running broker. */
async function phaseShutdown() {
  const harness = await import(HARNESS_URL);
  const engine = await harness.isolatedEngine();
  const out = { n: 10 };
  try {
    for (let index = 0; index < 10; index += 1) {
      const saved = await engine.clients.save({ name: `shut-${index}`, url: engine.url, autoConnect: false });
      await engine.clients.connect(saved.id);
    }
    await harness.waitFor(
      () => engine.clients.list().filter((view) => view.status === 'connected').length === 10,
      { timeout: 15000, interval: 15, label: '10 connected GUI clients' },
    );
    await sleep(150);
    await settle(3);
    const before = mem();
    const startedAt = performance.now();
    await engine.dispose();
    const disposeMs = performance.now() - startedAt;
    await sleep(250);
    await settle(3);
    const after = mem();
    out.disposeMs = round(disposeMs, 1);
    out.heapBeforeDisposeMb = mb(before.heapUsed);
    out.heapAfterDisposeMb = mb(after.heapUsed);
    out.heapFreedBytes = before.heapUsed - after.heapUsed;
    out.heapFreedMb = mb(before.heapUsed - after.heapUsed);
    out.rssBeforeDisposeMb = mb(before.rss);
    out.rssAfterDisposeMb = mb(after.rss);
    out.portStillAccepting = await tcpProbe(engine.port, 300);
    out.brokerStatus = engine.broker.view().status;
    out.connectedClientsAfter = engine.clients.list().filter((view) => view.status === 'connected').length;
    out.resourcesAfterDispose = resourceSummary(resources());
    out.refdTimeoutsAfterDispose = refdTimeoutCount(out.resourcesAfterDispose.list);
    out.listenersAfterDispose = listenerSnapshot();
    return out;
  } finally {
    try {
      await engine.dispose();
    } catch {
      /* already disposed */
    }
  }
}

/* --------------------------------------------------------------- orchestration */

async function runPhase(phase, flags) {
  switch (phase) {
    case 'load':
      return phaseLoad();
    case 'dep':
      return phaseDep(flags.dep);
    case 'mount':
      return phaseMount();
    case 'lifecycle':
      return phaseLifecycle();
    case 'conn':
      return phaseConn(Number(flags.n));
    case 'throughput':
      return phaseThroughput();
    case 'payload':
      return phasePayload();
    case 'retained':
      return phaseRetained();
    case 'log':
      return phaseLog();
    case 'sse':
      return phaseSse();
    case 'bundle':
      return phaseBundle();
    case 'stress':
      return phaseStress();
    case 'shutdown':
      return phaseShutdown();
    default:
      throw new Error(`unknown phase: ${phase}`);
  }
}

function parseFlags(argv) {
  const flags = {};
  let phase = null;
  for (const argument of argv) {
    if (argument.startsWith('--phase=')) phase = argument.slice('--phase='.length);
    else if (argument.startsWith('--') && argument.includes('=')) {
      const [key, ...rest] = argument.slice(2).split('=');
      flags[key] = rest.join('=');
    }
  }
  return { phase, flags };
}

// Only enforce process.exit for phases whose children are known to have closed
// everything; the mount/lifecycle phases deliberately let the child exit on its
// own so a leaked ref'd handle shows up as a timeout in the orchestrator.
const NATURAL_EXIT_PHASES = new Set(['mount', 'lifecycle']);

async function main() {
  const { phase, flags } = parseFlags(process.argv.slice(2));
  if (phase !== null) {
    try {
      const result = await runPhase(phase, flags);
      const payload = { phase, ok: true, result };
      process.stdout.write(`PHASE-RESULT ${JSON.stringify(payload)}\n`, () => {
        if (!NATURAL_EXIT_PHASES.has(phase)) process.exit(0);
      });
    } catch (error) {
      const payload = { phase, ok: false, error: String(error?.stack ?? error) };
      process.stdout.write(`PHASE-RESULT ${JSON.stringify(payload)}\n`, () => process.exit(1));
    }
    return;
  }
  await orchestrate();
}

/* ----------------------------------------------------------------- report side */

function runChild(args, { naturalExit = false, timeout = 240000 } = {}) {
  const result = spawnSync(process.execPath, ['--expose-gc', BENCH_FILE, ...args], {
    encoding: 'utf8',
    timeout,
    maxBuffer: 64 * MB,
  });
  const timedOut = result.error?.code === 'ETIMEDOUT' || result.signal !== null && result.signal !== undefined && result.status === null;
  const stdout = result.stdout ?? '';
  const lines = stdout.split(/\r?\n/).filter((line) => line.startsWith('PHASE-RESULT '));
  let parsed = null;
  if (lines.length > 0) {
    try {
      parsed = JSON.parse(lines[lines.length - 1].slice('PHASE-RESULT '.length));
    } catch {
      parsed = null;
    }
  }
  return {
    args,
    naturalExit: naturalExit ? !timedOut : null,
    timedOut,
    status: result.status,
    signal: result.signal,
    result: parsed,
    stderrTail: String(result.stderr ?? '').slice(-1500),
  };
}

const PHASES = [
  { label: 'a: load', args: ['--phase=load'] },
  { label: 'a: dep mqtt', args: ['--phase=dep', '--dep=mqtt'] },
  { label: 'a: dep aedes', args: ['--phase=dep', '--dep=aedes'] },
  { label: 'a: mount', args: ['--phase=mount'], naturalExit: true, timeout: 30000 },
  { label: 'i: lifecycle', args: ['--phase=lifecycle'], naturalExit: true, timeout: 30000 },
  { label: 'c: conn 1', args: ['--phase=conn', '--n=1'] },
  { label: 'c: conn 10', args: ['--phase=conn', '--n=10'] },
  { label: 'c: conn 50', args: ['--phase=conn', '--n=50'] },
  { label: 'b: throughput', args: ['--phase=throughput'] },
  { label: 'd: payload', args: ['--phase=payload'] },
  { label: 'e: retained', args: ['--phase=retained'] },
  { label: 'f: log', args: ['--phase=log'] },
  { label: 'g: sse', args: ['--phase=sse'] },
  { label: 'h: bundle', args: ['--phase=bundle'] },
  { label: 'stress', args: ['--phase=stress'] },
  { label: 'j: shutdown', args: ['--phase=shutdown'] },
];

function formatMs(value) {
  return value === null || value === undefined ? 'n/a' : `${value} ms`;
}

async function orchestrate() {
  const collected = {};
  for (const spec of PHASES) {
    const startedAt = performance.now();
    const child = runChild(spec.args, { naturalExit: spec.naturalExit ?? false, timeout: spec.timeout ?? 240000 });
    const elapsed = performance.now() - startedAt;
    collected[spec.label] = child;
    const marker = child.result?.ok ? 'ok' : 'FAIL';
    process.stdout.write(`[bench] ${spec.label}: ${marker} in ${(elapsed / 1000).toFixed(1)} s\n`);
    if (!child.result?.ok) {
      process.stdout.write(`[bench]   stderr tail: ${child.stderrTail.replace(/\n/g, ' | ').slice(0, 800)}\n`);
    }
  }

  // Flatten the per-phase JSON into the compact report the lead asked for.
  const find = (label) => collected[label]?.result?.result ?? null;
  const load = find('a: load');
  const depMqtt = find('a: dep mqtt');
  const depAedes = find('a: dep aedes');
  const mount = find('a: mount');
  const lifecycle = find('i: lifecycle');
  const conn = [1, 10, 50].map((n) => find(`c: conn ${n}`));
  const throughput = find('b: throughput');
  const payload = find('d: payload');
  const retained = find('e: retained');
  const log = find('f: log');
  const sse = find('g: sse');
  const bundle = find('h: bundle');
  const stress = find('stress');
  const shutdown = find('j: shutdown');

  const rows = [];
  const row = (metric, value, notes = '') => rows.push({ metric, value, notes });

  if (load) {
    row('cold import index.js', formatMs(load.imports.indexMs), `heap +${mb(load.heapDeltas.index)} MiB`);
    row('lazy import mqtt', formatMs(load.imports.mqttMs), `heap +${mb(load.heapDeltas.mqtt)} MiB (from index-only baseline)`);
    row('lazy import aedes', formatMs(load.imports.aedesMs), `heap +${mb(load.heapDeltas.aedes)} MiB (mqtt already loaded)`);
    row('lazy import mqtt-packet', formatMs(load.imports.mqttPacketMs), `heap +${mb(load.heapDeltas.mqttPacket)} MiB`);
  }
  if (depMqtt) row('mqtt first-use (isolated child)', formatMs(depMqtt.firstImportMs), `heap +${mb(depMqtt.heapDelta)} MiB, v${depMqtt.version}`);
  if (depAedes) row('aedes first-use (isolated child)', formatMs(depAedes.firstImportMs), `heap +${mb(depAedes.heapDelta)} MiB, v${depAedes.version}`);
  if (mount) {
    row('apply(ctx) full plugin mount', formatMs(mount.applyMs), `${mount.toolsRegistered} tools, ${mount.routesRegistered} routes`);
    row('heap after mount (incl. mqtt+aedes)', `+${mount.heapDeltaMountMb} MiB`, `dispose call ${mount.disposeMs} ms; ${mount.heapReturnedBytes} B returned`);
    row('active resources after mount idle', String(mount.resourcesAfterMount.total), `${Object.keys(mount.resourcesAfterMount.counts).join(', ') || 'none'}; ref'd timers ${mount.refdTimeoutsAfterMount}; listeners ${JSON.stringify(mount.listenersAfterMount)}`);
    row('active resources after dispose', String(mount.resourcesAfterDispose.total), Object.keys(mount.resourcesAfterDispose.counts).join(', ') || 'none');
  }
  if (lifecycle) {
    row('broker listen after apply', formatMs(lifecycle.brokerListenMs), `running heap +${lifecycle.heapDeltaRunningMb} MiB`);
    row('plugin dispose() call (broker running)', formatMs(lifecycle.disposeCallMs), 'synchronous part only');
    row('TCP port released after dispose', formatMs(lifecycle.portReleasedMs), lifecycle.portReleasedMs === null ? 'PORT STILL OPEN' : 'async broker close');
    row('active resources after dispose (broker)', String(lifecycle.resourcesAfterDispose.total), `${Object.keys(lifecycle.resourcesAfterDispose.counts).join(', ') || 'none'}; ref'd timers ${lifecycle.refdTimeoutsAfterDispose} (was ${lifecycle.refdTimeoutsAfterStart} while running)`);
  }
  for (const entry of conn) {
    if (!entry) continue;
    row(`per-connection overhead (${entry.n} clients)`, `${mb(entry.heapDeltaBytes)} MiB total`, `${entry.bytesPerConnection} B/conn, ${entry.connectMsPerClient} ms connect`);
  }
  if (throughput) {
    for (const key of ['n1', 'n10', 'n50']) {
      const entry = throughput[key];
      if (!entry) continue;
      row(`broker.publish ${entry.clients} subscriber(s)`, `${entry.msgsPerSec} msg/s`, `p50 ${entry.p50Ms} ms, p99 ${entry.p99Ms} ms, heap +${entry.heapDeltaMb} MiB`);
    }
  }
  if (payload) {
    for (const spec of DEP_SPECS) {
      const entry = payload[spec.label];
      if (!entry) continue;
      row(`payload ${spec.label}`, `${entry.msgsPerSec} msg/s (${entry.mibPerSec} MiB/s)`, `p99 ${entry.p99Ms} ms; peak heap +${entry.peakHeapDeltaMb}/ext +${entry.peakExternalDeltaMb} MiB; ring keeps ${entry.ringPayloadChars} chars${entry.logTruncated ? ' (truncated)' : ''}`);
    }
  }
  if (retained) {
    for (const count of ['100', '1000']) {
      const entry = retained[count];
      if (!entry) continue;
      row(`retained ${entry.count} x 1 KiB`, `heap +${entry.heapDeltaMb} / ext +${entry.externalDeltaMb} MiB`, `${entry.bytesPerRetained} B/msg incl. external, ${entry.publishMsEach} ms/publish`);
    }
  }
  if (log) {
    row('RingLog 10k pushes', `${log.pushesMs} ms`, `${log.perPushUs} µs/push = ${log.pushesPerSec} pushes/s`);
    row('RingLog held by 600 x 8 KiB entries', `${log.heldMb} MiB`, `${log.perEntryBytes} B/entry; 2nd 10k burst still ${log.heldAfterSecondBurstMb} MiB (bounded: ${log.bounded})`);
    row('truncated ring entry (600 x 64 KiB binary)', `+${log.truncation.heldMb} MiB`, `${log.truncation.visibleChars} chars shown but ${log.truncation.perEntryBytes} B pinned/entry (${log.truncation.pinnedRatio}x); 600 x 512 KiB ~= ${log.truncation.worstCase600x512KiBmb} MiB`);
  }
  if (sse) {
    row(`SSE fan-out ${sse.streams} streams x ${sse.frames}`, `${sse.framesPerSec} frames/s`, `loop ${sse.loopMs} ms (${sse.perFrameUs} µs/frame), peak +${sse.peakHeapDeltaMb} MiB`);
  }
  if (bundle) {
    row('client.js on disk', `${(bundle.clientJsBytes / 1024).toFixed(1)} KiB`, `gzip ${(bundle.clientGzip9Bytes / 1024).toFixed(1)} KiB (${bundle.clientGzipRatio}), brotli ${(bundle.clientBrotli11Bytes / 1024).toFixed(1)} KiB`);
    if (bundle.npmPack && !bundle.npmPack.error) {
      row('npm pack tarball', `${(bundle.npmPack.tarballBytes / 1024).toFixed(1)} KiB`, `unpacked ${(bundle.npmPack.unpackedBytes / 1024).toFixed(1)} KiB, ${bundle.npmPack.fileCount} files`);
      row('package JS payload', `${(bundle.npmPack.jsOnlyBytes / 1024).toFixed(1)} KiB`, `host ${(bundle.npmPack.hostJsBytes / 1024).toFixed(1)} KiB + client ${(bundle.npmPack.clientJsBytes / 1024).toFixed(1)} KiB`);
      row('package docs images', `${(bundle.npmPack.docsBytes / 1024).toFixed(1)} KiB`, 'docs/*.png inflate the tarball');
    } else {
      row('npm pack', 'FAILED', bundle.npmPack?.error ?? 'no output');
    }
    row('installed node_modules footprint', `${bundle.nodeModulesMb} MiB`, `${bundle.nodeModulesFiles} files (mqtt ${bundle.hostDependencyVersions.mqtt}, aedes ${bundle.hostDependencyVersions.aedes})`);
  }
  if (stress) {
    row('stress: 200 retained + 20 clients + 500 burst', `${stress.msgsPerSec} msg/s publish`, `${stress.deliveries} deliveries in ${stress.deliveredMs} ms`);
    row('stress: transient peak heap', `+${stress.peakHeapDeltaMb} MiB`, `transient above recovered: ${stress.transientPeakMb} MiB`);
    row('stress: heap after GC', `+${stress.recoveredHeapDeltaMb} MiB`, `${stress.recoveredPercentOfPeak}% of peak retained; ring ${stress.ringEntries} entries`);
  }
  if (shutdown) {
    row('engine dispose (10 clients + broker)', formatMs(shutdown.disposeMs), `heap freed ${shutdown.heapFreedMb} MiB (${shutdown.heapBeforeDisposeMb}->${shutdown.heapAfterDisposeMb}); port still accepting: ${shutdown.portStillAccepting}`);
    row('active resources / ref timers after dispose', String(shutdown.resourcesAfterDispose.total), `ref'd timers ${shutdown.refdTimeoutsAfterDispose}; ${Object.keys(shutdown.resourcesAfterDispose.counts).join(', ') || 'none'}`);
  }

  process.stdout.write('\n=== BENCH TABLE ===\n');
  const width = Math.max(...rows.map((entry) => entry.metric.length), 10) + 2;
  for (const entry of rows) {
    process.stdout.write(`${entry.metric.padEnd(width)} | ${entry.value.padEnd(26)} | ${entry.notes}\n`);
  }
  process.stdout.write('\n=== BENCH RESULT JSON ===\n');
  process.stdout.write(`${JSON.stringify({ rows, phases: collected }, null, 2)}\n`);
}

const isMain = process.argv[1] !== undefined && pathToFileURL(process.argv[1]).href === pathToFileURL(BENCH_FILE).href;
if (isMain) await main();

export { percentile, phaseLoad, phaseMount, phaseLifecycle, phaseConn, phaseThroughput, phasePayload, phaseRetained, phaseLog, phaseSse, phaseBundle, phaseStress, phaseShutdown };
