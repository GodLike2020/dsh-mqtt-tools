// Capacity and performance suite for dsh-mqtt-tools.
//
// Nothing here asserts a wall-clock number that a slower machine would fail:
// the assertions are about completeness, boundedness and "it finishes at all".
// The measured rates are printed so a regression is visible in the log.
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  Suite,
  runSuite,
  isolatedEngine,
  connectMqtt,
  endMqtt,
  subscribeAsync,
  recorder,
  assert,
  eq,
  waitFor,
  loadPluginLib,
} from './harness.mjs';

const suite = new Suite('perf', 'Capacity and performance', {
  note: 'functional completeness first, rates reported for regression spotting',
});

function fakeRes() {
  const chunks = [];
  return {
    headersSent: false,
    ended: false,
    writeHead() {
      this.headersSent = true;
      return this;
    },
    write(chunk) {
      chunks.push(String(chunk));
      return true;
    },
    end() {
      this.ended = true;
    },
    on() {
      return this;
    },
    get text() {
      return chunks.join('');
    },
  };
}

function fakeReq() {
  return { headers: {}, on: () => fakeReq };
}

await runSuite(suite, async () => {
  const { Hub } = await loadPluginLib('hub');
  const { Store } = await loadPluginLib('store');

  // -------------------------------------------------------------------------
  suite.group('message path');
  // -------------------------------------------------------------------------

  await suite.test('2000 small messages arrive intact and quickly', async () => {
    const engine = await isolatedEngine();
    let sub = null;
    let pub = null;
    try {
      const seen = recorder(
        await (async () => {
          sub = await connectMqtt(engine.url, { clientId: 'perf-sub' });
          return sub;
        })(),
      );
      await subscribeAsync(sub, 'perf/#', 1);
      pub = await connectMqtt(engine.url, { clientId: 'perf-pub' });
      const total = 2000;
      const startedAt = Date.now();
      for (let index = 0; index < total; index += 1) {
        pub.publish(`perf/${index % 10}`, `payload-${index}-${'x'.repeat(96)}`, { qos: 0 });
      }
      await seen.waitFor(() => seen.messages.length >= total, { timeout: 30000, label: `${total} messages` });
      const elapsed = Date.now() - startedAt;
      const rate = Math.round((total * 1000) / Math.max(1, elapsed));
      assert(rate > 200, `throughput collapsed: ${rate} msg/s`);
      eq(seen.messages.length >= total, true, 'delivered count');
      return `${total} msgs in ${elapsed} ms (${rate} msg/s)`;
    } finally {
      await endMqtt(pub);
      await endMqtt(sub);
      await engine.dispose();
    }
  });

  await suite.test('the message ring log stays bounded under a burst', async () => {
    const engine = await isolatedEngine();
    let pub = null;
    let sub = null;
    try {
      sub = await connectMqtt(engine.url, { clientId: 'perf-ring-sub' });
      const seen = recorder(sub);
      await subscribeAsync(sub, 'ring/#', 0);
      pub = await connectMqtt(engine.url, { clientId: 'perf-ring-pub' });
      const total = 3000;
      for (let index = 0; index < total; index += 1) pub.publish(`ring/${index}`, 'x', { qos: 0 });
      await seen.waitFor(() => seen.messages.length >= total, { timeout: 30000, label: 'ring burst' });
      await waitFor(() => engine.messages.tail(10000).length >= 600, { timeout: 5000, label: 'ring log filled' });
      const tail = engine.messages.tail(10000);
      eq(tail.length, 600, 'ring log grew past its limit');
      const seqs = tail.map((entry) => entry.seq);
      assert(seqs.every((seq, index) => index === 0 || seq > seqs[index - 1]), 'sequence numbers not monotonic');
      eq(engine.messages.since(seqs[0] - 1, 10000).length <= 600, true, 'since() returned more than the log holds');
      return `kept ${tail.length} of ${total}, seq ${seqs[0]}..${seqs[seqs.length - 1]}`;
    } finally {
      await endMqtt(pub);
      await endMqtt(sub);
      await engine.dispose();
    }
  });

  await suite.test('a 512 KiB payload survives the round trip byte for byte', async () => {
    const engine = await isolatedEngine();
    let sub = null;
    let pub = null;
    try {
      sub = await connectMqtt(engine.url, { clientId: 'perf-big-sub' });
      const seen = recorder(sub);
      await subscribeAsync(sub, 'big/#', 1);
      pub = await connectMqtt(engine.url, { clientId: 'perf-big-pub' });
      const payload = Buffer.alloc(512 * 1024);
      for (let index = 0; index < payload.length; index += 1) payload[index] = index % 251;
      const startedAt = Date.now();
      await new Promise((resolve, reject) => {
        pub.publish('big/one', payload, { qos: 1 }, (error) => (error ? reject(error) : resolve()));
      });
      const received = await seen.waitFor((entry) => entry.topic === 'big/one', { timeout: 15000, label: 'big payload' });
      eq(received.payload.length, payload.length, 'payload length');
      eq(received.payload.equals(payload), true, 'payload bytes differ');
      return `512 KiB in ${Date.now() - startedAt} ms`;
    } finally {
      await endMqtt(pub);
      await endMqtt(sub);
      await engine.dispose();
    }
  });

  await suite.test('500 retained topics replay to a late subscriber', async () => {
    const engine = await isolatedEngine();
    let pub = null;
    let sub = null;
    try {
      pub = await connectMqtt(engine.url, { clientId: 'perf-ret-pub' });
      const total = 500;
      for (let index = 0; index < total; index += 1) {
        pub.publish(`ret/${index}`, `value-${index}`, { qos: 0, retain: true });
      }
      // give aedes a moment to store the retained set before subscribing
      await waitFor(() => engine.broker.view().stats?.published >= total, { timeout: 5000, label: 'retained stored' });
      sub = await connectMqtt(engine.url, { clientId: 'perf-ret-sub' });
      const seen = recorder(sub);
      await subscribeAsync(sub, 'ret/#', 1);
      await seen.waitFor(() => seen.messages.length >= total, { timeout: 20000, label: `${total} retained messages` });
      eq(seen.messages.length >= total, true, 'retained replay count');
      return `${seen.messages.length} retained replayed`;
    } finally {
      await endMqtt(pub);
      await endMqtt(sub);
      await engine.dispose();
    }
  });

  await suite.test('30 clients connect at once and all disconnect cleanly', async () => {
    const engine = await isolatedEngine();
    const clients = [];
    try {
      const startedAt = Date.now();
      for (let index = 0; index < 30; index += 1) clients.push(await connectMqtt(engine.url, { clientId: `storm-${index}` }));
      const connectedMs = Date.now() - startedAt;
      await waitFor(() => engine.broker.view().clientCount >= 30, { timeout: 5000, label: 'broker client count' });
      eq(engine.broker.view().clientCount, 30, 'client count');
      await Promise.all(clients.map((client) => endMqtt(client)));
      clients.length = 0;
      await waitFor(() => engine.broker.view().clientCount === 0, { timeout: 5000, label: 'clients gone' });
      return `30 connects in ${connectedMs} ms`;
    } finally {
      for (const client of clients) await endMqtt(client);
      await engine.dispose();
    }
  });

  await suite.test('100 connect/disconnect cycles leave no clients behind', async () => {
    const engine = await isolatedEngine();
    try {
      const startedAt = Date.now();
      for (let index = 0; index < 100; index += 1) {
        const client = await connectMqtt(engine.url, { clientId: `churn-${index}` });
        await endMqtt(client);
      }
      await waitFor(() => engine.broker.view().clientCount === 0, { timeout: 5000, label: 'churn cleaned up' });
      return `100 cycles in ${Date.now() - startedAt} ms`;
    } finally {
      await engine.dispose();
    }
  });

  // -------------------------------------------------------------------------
  suite.group('SSE fan-out');
  // -------------------------------------------------------------------------

  await suite.test('1000 events reach 10 streams without dropping', async () => {
    const hub = new Hub();
    const streams = [];
    try {
      for (let index = 0; index < 10; index += 1) {
        const res = fakeRes();
        hub.attach(fakeReq(), res, { state: {} });
        streams.push(res);
      }
      const total = 1000;
      const startedAt = Date.now();
      for (let index = 0; index < total; index += 1) {
        hub.broadcast('message', { seq: index, topic: `fan/${index}`, payload: 'x'.repeat(200) });
      }
      const elapsed = Date.now() - startedAt;
      for (const [index, res] of streams.entries()) {
        const frames = res.text.split('event: message').length - 1;
        eq(frames, total, `stream ${index} frame count`);
      }
      const rate = Math.round((total * 10 * 1000) / Math.max(1, elapsed));
      assert(elapsed < 8000, `fan-out took ${elapsed} ms`);
      return `10k frames in ${elapsed} ms (${rate} frames/s)`;
    } finally {
      hub.dispose();
    }
  });

  await suite.test('broadcasting with no streams is free', async () => {
    const hub = new Hub();
    try {
      const startedAt = Date.now();
      for (let index = 0; index < 20000; index += 1) hub.broadcast('message', { seq: index });
      const elapsed = Date.now() - startedAt;
      assert(elapsed < 1000, `idle broadcast cost ${elapsed} ms`);
      return `20k idle broadcasts in ${elapsed} ms`;
    } finally {
      hub.dispose();
    }
  });

  // -------------------------------------------------------------------------
  suite.group('state persistence');
  // -------------------------------------------------------------------------

  await suite.test('a 50-client state file flushes fast and reloads intact', async () => {
    const home = mkdtempSync(join(tmpdir(), 'dsh-mqtt-tools-perf-'));
    const previousHome = process.env.DSH_HOME;
    process.env.DSH_HOME = home;
    try {
      const store = new Store();
      await store.ready;
      store.update((state) => {
        for (let index = 0; index < 50; index += 1) {
          state.clients.push({
            id: `c${index}`,
            name: `client-${index}`,
            url: `mqtt://127.0.0.1:${1883 + index}`,
            clientId: `cid-${index}`,
            username: 'u',
            password: '',
            keepalive: 60,
            clean: true,
            protocolVersion: 4,
            connectTimeout: 10000,
            reconnectPeriod: 3000,
            rejectUnauthorized: true,
            autoConnect: false,
            subscriptions: Array.from({ length: 5 }, (_, sub) => ({ topic: `topic/${index}/${sub}/#`, qos: 1 })),
          });
        }
      });
      const startedAt = Date.now();
      const rounds = 20;
      for (let index = 0; index < rounds; index += 1) await store.flush();
      const perFlush = Math.round((Date.now() - startedAt) / rounds);
      const raw = readFileSync(join(home, 'dsh-mqtt-tools.json'), 'utf8');
      const bytes = Buffer.byteLength(raw, 'utf8');
      assert(bytes < 2 * 1024 * 1024, `state file grew to ${bytes} bytes`);
      await store.dispose();
      const reloaded = new Store();
      await reloaded.ready;
      eq(reloaded.data.clients.length, 50, 'reloaded client count');
      eq(reloaded.data.clients[7].subscriptions.length, 5, 'reloaded subscriptions');
      await reloaded.dispose();
      return `${rounds} flushes, ~${perFlush} ms each, ${Math.round(bytes / 1024)} KiB`;
    } finally {
      if (previousHome === undefined) delete process.env.DSH_HOME;
      else process.env.DSH_HOME = previousHome;
      rmSync(home, { recursive: true, force: true });
    }
  });
});
