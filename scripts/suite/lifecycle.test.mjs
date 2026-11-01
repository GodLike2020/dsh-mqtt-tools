// Lifecycle, durability and API-robustness suite for dsh-mqtt-tools.
//
// These are the failures that only show up over time: a hub that leaks event
// streams, a state file that loses its last write, a broker that cannot start
// after its port was taken, a route handler that rejects and takes the web server
// down with it, a plugin that mounts twice after a hot reload.
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import net from 'node:net';
import { join } from 'node:path';
import {
  Suite,
  runSuite,
  isolatedEngine,
  connectMqtt,
  endMqtt,
  subscribeAsync,
  publishAsync,
  recorder,
  assert,
  eq,
  waitFor,
  sleep,
  loadPluginLib,
} from './harness.mjs';

const suite = new Suite('lifecycle', 'Lifecycle, durability and API robustness', {
  note: 'isolated engines only; never touches the live GUI state',
});

// ---------------------------------------------------------------------------
// fake node http objects, enough for the plugin's handlers
// ---------------------------------------------------------------------------

function fakeReq({ method = 'GET', headers = {}, body = '' } = {}) {
  const listeners = new Map();
  const chunks = [];
  if (body) chunks.push(Buffer.isBuffer(body) ? body : Buffer.from(String(body)));
  return {
    method,
    headers,
    chunks,
    on(event, fn) {
      const list = listeners.get(event) ?? [];
      list.push(fn);
      listeners.set(event, list);
      return this;
    },
    emit(event) {
      for (const fn of listeners.get(event) ?? []) fn();
      return this;
    },
    async *[Symbol.asyncIterator]() {
      for (const chunk of chunks) yield chunk;
    },
  };
}

function fakeRes({ deadSocket = false } = {}) {
  const chunks = [];
  const res = {
    headersSent: false,
    ended: false,
    status: null,
    headers: null,
    writeHead(status, headers) {
      if (deadSocket) throw new Error('socket is gone');
      this.status = status;
      this.headers = headers;
      this.headersSent = true;
      return this;
    },
    write(chunk) {
      if (deadSocket) throw new Error('socket is gone');
      chunks.push(String(chunk));
      return true;
    },
    end(chunk) {
      if (chunk !== undefined && chunk !== null && !deadSocket) chunks.push(String(chunk));
      this.ended = true;
    },
    on() {
      return this;
    },
    get text() {
      return chunks.join('');
    },
    get json() {
      try {
        return JSON.parse(chunks.join(''));
      } catch {
        return null;
      }
    },
  };
  return res;
}

function tempHome() {
  const home = mkdtempSync(join(tmpdir(), 'dsh-mqtt-tools-life-'));
  process.env.DSH_HOME = home;
  return {
    home,
    cleanup() {
      delete process.env.DSH_HOME;
      rmSync(home, { recursive: true, force: true });
    },
  };
}

/** A stand-in cordis context: effects run now, their cleanups are disposed in reverse. */
function fakeContext() {
  const routes = [];
  const tools = [];
  const cleanups = [];
  const ctx = {
    webServer: {
      register(route) {
        routes.push(route);
        return () => {
          const index = routes.indexOf(route);
          if (index >= 0) routes.splice(index, 1);
        };
      },
      getOrigin: () => 'http://127.0.0.1:6871',
    },
    tools: {
      register(tool) {
        tools.push(tool);
        return () => {
          const index = tools.indexOf(tool);
          if (index >= 0) tools.splice(index, 1);
        };
      },
    },
    effect(fn) {
      const cleanup = fn();
      cleanups.push(cleanup);
      return () => cleanup?.();
    },
  };
  return {
    ctx,
    routes,
    tools,
    dispose() {
      for (const cleanup of cleanups.reverse()) {
        try {
          cleanup?.();
        } catch (error) {
          // surfaced by the mount test below
          console.error('[lifecycle] cleanup threw:', error);
        }
      }
    },
  };
}

// ---------------------------------------------------------------------------

await runSuite(suite, async () => {
  const { Hub } = await loadPluginLib('hub');
  const { Store } = await loadPluginLib('store');

  // -------------------------------------------------------------------------
  suite.group('SSE hub');
  // -------------------------------------------------------------------------

  await suite.test('attach sends retry + hello, broadcast reaches the page', async () => {
    const hub = new Hub();
    const res = fakeRes();
    hub.attach(fakeReq(), res, { state: { a: 1 } });
    assert(res.text.includes('retry: 2000\n\n'), 'missing retry frame');
    assert(res.text.includes('event: hello'), 'missing hello frame');
    assert(res.text.includes('"a":1'), 'hello payload missing');
    hub.broadcast('state', { b: 2 });
    assert(res.text.includes('event: state'), 'broadcast did not reach the stream');
    hub.dispose();
  });

  await suite.test('a closed request unregisters its stream', async () => {
    const hub = new Hub();
    const req = fakeReq();
    hub.attach(req, fakeRes(), { state: {} });
    eq(hub.clients.size, 1, 'registered clients');
    req.emit('close');
    eq(hub.clients.size, 0, 'clients after close');
    hub.dispose();
  });

  await suite.test('a dead socket never throws and is dropped', async () => {
    const hub = new Hub();
    const dead = fakeRes({ deadSocket: true });
    hub.attach(fakeReq(), dead, { state: {} });
    eq(hub.clients.size, 0, 'dead stream is not registered');
    // a healthy client stays healthy while a dead one is being broadcast to
    const alive = fakeRes();
    hub.attach(fakeReq(), alive, { state: {} });
    hub.broadcast('message', { topic: 'x' });
    assert(alive.text.includes('event: message'), 'healthy stream lost the broadcast');
    hub.dispose();
  });

  await suite.test('ping writes a heartbeat to every stream', async () => {
    const hub = new Hub();
    const res = fakeRes();
    hub.attach(fakeReq(), res, { state: {} });
    const before = res.text.length;
    hub.ping();
    assert(res.text.slice(before).includes(': ping'), 'no heartbeat written');
    hub.dispose();
  });

  await suite.test('dispose ends every stream and clears the registry', async () => {
    const hub = new Hub();
    const res = fakeRes();
    hub.attach(fakeReq(), res, { state: {} });
    hub.dispose();
    assert(res.ended, 'stream was not ended');
    eq(hub.clients.size, 0, 'clients after dispose');
  });

  // -------------------------------------------------------------------------
  suite.group('state file durability');
  // -------------------------------------------------------------------------

  await suite.test('an update survives dispose without waiting for the debounce', async () => {
    const temp = tempHome();
    try {
      const store = new Store();
      await store.ready;
      store.update((state) => {
        state.log.showSystem = true;
      });
      await store.dispose();
      const raw = JSON.parse(readFileSync(join(temp.home, 'dsh-mqtt-tools.json'), 'utf8'));
      eq(raw.log.showSystem, true, 'persisted showSystem');
      const leftovers = readdirSync(temp.home).filter((name) => name.endsWith('.tmp'));
      eq(leftovers, [], 'temporary files left behind');
    } finally {
      temp.cleanup();
    }
  });

  await suite.test('a corrupt state file falls back to defaults instead of throwing', async () => {
    const temp = tempHome();
    try {
      writeFileSync(join(temp.home, 'dsh-mqtt-tools.json'), '{"version":1,"clients":[', 'utf8');
      const store = new Store();
      await store.ready;
      eq(Array.isArray(store.data.clients), true, 'clients recovered as an array');
      eq(store.data.broker.port, 1883, 'broker port fell back to the default');
      store.update((state) => {
        state.broker.port = 1884;
      });
      await store.dispose();
      const raw = JSON.parse(readFileSync(join(temp.home, 'dsh-mqtt-tools.json'), 'utf8'));
      eq(raw.broker.port, 1884, 'the file was repaired and written');
    } finally {
      temp.cleanup();
    }
  });

  await suite.test('rapid updates keep the newest value', async () => {
    const temp = tempHome();
    try {
      const store = new Store();
      await store.ready;
      for (let index = 0; index < 50; index += 1) {
        store.update((state) => {
          state.broker.port = 2000 + index;
        });
      }
      await store.dispose();
      const raw = JSON.parse(readFileSync(join(temp.home, 'dsh-mqtt-tools.json'), 'utf8'));
      eq(raw.broker.port, 2049, 'last update wins');
    } finally {
      temp.cleanup();
    }
  });

  // -------------------------------------------------------------------------
  suite.group('broker lifecycle');
  // -------------------------------------------------------------------------

  await suite.test('start, stop, start again still accepts clients', async () => {
    const engine = await isolatedEngine();
    try {
      await engine.broker.stop();
      eq(engine.broker.view().status, 'stopped', 'status after stop');
      await engine.broker.start();
      eq(engine.broker.view().status, 'running', 'status after restart');
      const client = await connectMqtt(engine.url, { clientId: 'life-restart' });
      await endMqtt(client);
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('a taken port fails loudly and recovers once it is free', async () => {
    const blocker = net.createServer();
    await new Promise((resolve) => blocker.listen(0, '127.0.0.1', resolve));
    const port = blocker.address().port;
    const engine = await isolatedEngine({ autoStart: false, port });
    try {
      let error = null;
      try {
        await engine.broker.start();
      } catch (caught) {
        error = caught;
      }
      assert(error !== null, 'start() resolved even though the port was taken');
      assert(/EADDRINUSE|address already in use|in use/i.test(String(error.message)), `unhelpful error: ${error.message}`);
      eq(engine.broker.view().status !== 'running', true, 'broker claims to be running after a failed start');
      await new Promise((resolve) => blocker.close(resolve));
      await engine.broker.start();
      eq(engine.broker.view().status, 'running', 'status after the port was freed');
    } finally {
      await engine.dispose().catch(() => {});
      await new Promise((resolve) => (blocker.listening ? blocker.close(resolve) : resolve()));
    }
  });

  await suite.test('stopping a stopped broker is harmless', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      await engine.broker.stop();
      await engine.broker.stop();
      eq(engine.broker.view().status, 'stopped', 'still stopped');
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('dispose frees the listening port', async () => {
    const engine = await isolatedEngine();
    const port = engine.port;
    await engine.dispose();
    const probe = net.createServer();
    await new Promise((resolve, reject) => {
      probe.once('error', reject);
      probe.listen(port, '127.0.0.1', resolve);
    });
    await new Promise((resolve) => probe.close(resolve));
  });

  await suite.test('changing listener settings while running asks for a restart', async () => {
    const engine = await isolatedEngine();
    try {
      await engine.broker.saveSettings({ host: '127.0.0.1', port: engine.port, allowAnonymous: true, protocols: ['3.1.1'] });
      const view = engine.broker.view();
      eq(view.status, 'running', 'broker keeps running');
      eq(view.restartHint, true, 'restartHint should be set');
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('a QoS outside 0..2 is refused instead of silently clamped', async () => {
    const engine = await isolatedEngine();
    try {
      let error = null;
      try {
        await engine.broker.publish({ topic: 'life/qos', payload: '1', qos: 9 });
      } catch (caught) {
        error = caught;
      }
      assert(error !== null, 'broker.publish accepted qos 9');
      assert(/qos must be 0, 1 or 2/.test(String(error.message)), `unexpected error: ${error.message}`);
      const profile = await engine.clients.save({ name: 'life-qos', url: engine.url });
      await engine.clients.connect(profile.id);
      await waitFor(() => engine.clients.list().find((entry) => entry.id === profile.id)?.status === 'connected', {
        timeout: 4000,
        label: 'client connected',
      });
      error = null;
      try {
        await engine.clients.publish({ id: profile.id, topic: 'life/qos', payload: '1', qos: 3 });
      } catch (caught) {
        error = caught;
      }
      assert(error !== null, 'clients.publish accepted qos 3');
      assert(/qos must be 0, 1 or 2/.test(String(error.message)), `unexpected error: ${error.message}`);
      error = null;
      try {
        await engine.clients.subscribe({ id: profile.id, topic: 'life/#', qos: -1 });
      } catch (caught) {
        error = caught;
      }
      assert(error !== null, 'clients.subscribe accepted qos -1');
      assert(/qos must be 0, 1 or 2/.test(String(error.message)), `unexpected error: ${error.message}`);
      // In range values still work.
      const published = await engine.broker.publish({ topic: 'life/qos', payload: '2', qos: 2 });
      eq(published.qos, 2, 'in-range qos round-trips');
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('retained messages survive a broker restart and can be cleared', async () => {
    const engine = await isolatedEngine();
    try {
      await engine.broker.publish({ topic: 'life/retained', payload: 'hello', qos: 1, retain: true });
      eq(engine.broker.view().retainedCount, 1, 'retained count before restart');
      // The set has to reach the state file too, not only this process' memory.
      await engine.store.flush();
      const saved = JSON.parse(readFileSync(join(engine.home, 'dsh-mqtt-tools.json'), 'utf8'));
      eq(saved.retained.length, 1, 'retained rows on disk');
      eq(saved.retained[0].topic, 'life/retained', 'persisted topic');
      await engine.broker.stop();
      await engine.broker.start();
      eq(engine.broker.view().retainedCount, 1, 'retained count after restart');
      const observer = await connectMqtt(engine.url, { clientId: 'life-retained-observer' });
      try {
        const seen = recorder(observer);
        await subscribeAsync(observer, 'life/#', 1);
        const message = await seen.waitFor((entry) => entry.topic === 'life/retained', { timeout: 4000, label: 'replayed retained message' });
        eq(String(message.payload), 'hello', 'replayed payload');
      } finally {
        await endMqtt(observer);
      }
      const cleared = await engine.broker.clearRetained();
      eq(cleared.cleared, 1, 'cleared topic count');
      eq(engine.broker.view().retainedCount, 0, 'retained count after clear');
      await engine.broker.stop();
      await engine.broker.start();
      eq(engine.broker.view().retainedCount, 0, 'cleared set stays cleared after a restart');
      let error = null;
      await engine.broker.stop();
      try {
        await engine.broker.clearRetained();
      } catch (caught) {
        error = caught;
      }
      assert(error !== null, 'clearRetained worked while stopped');
      assert(/start the broker first/.test(String(error.message)), `unexpected error: ${error.message}`);
    } finally {
      await engine.dispose();
    }
  });

  // -------------------------------------------------------------------------
  suite.group('client lifecycle');
  // -------------------------------------------------------------------------

  await suite.test('a client reconnects by itself after the broker comes back', async () => {
    const engine = await isolatedEngine();
    let client = null;
    try {
      const profile = await engine.clients.save({ name: 'life-reconnect', url: engine.url, reconnectPeriod: 200 });
      await engine.clients.connect(profile.id);
      const waitConnected = () =>
        waitFor(
          () => engine.clients.list().find((entry) => entry.id === profile.id)?.status === 'connected',
          { timeout: 4000, label: 'client connected' },
        );
      await waitConnected();
      await engine.broker.stop();
      await waitFor(
        () => {
          const status = engine.clients.list().find((entry) => entry.id === profile.id)?.status;
          return status !== 'connected';
        },
        { timeout: 4000, label: 'client notices the broker is gone' },
      );
      await engine.broker.start();
      await waitConnected();
      client = profile.id;
    } finally {
      if (client) await engine.clients.remove(client).catch(() => {});
      await engine.dispose();
    }
  });

  await suite.test('deleting a connected client removes it from the pool', async () => {
    const engine = await isolatedEngine();
    try {
      const profile = await engine.clients.save({ name: 'life-delete', url: engine.url });
      await engine.clients.connect(profile.id);
      await engine.clients.remove(profile.id);
      eq(engine.clients.list().some((entry) => entry.id === profile.id), false, 'deleted profile still listed');
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('re-saving a client keeps its stored password', async () => {
    const engine = await isolatedEngine();
    try {
      const created = await engine.clients.save({ name: 'life-password', url: 'mqtt://127.0.0.1:1', username: 'u', password: 'secret-1' });
      eq(created.hasPassword, true, 'password not stored');
      const updated = await engine.clients.save({ id: created.id, name: 'life-password', url: 'mqtt://127.0.0.1:1', username: 'u', password: '' });
      eq(updated.hasPassword, true, 'empty password wiped the stored one');
      assert(!JSON.stringify(updated).includes('secret-1'), 'the stored password leaked into the API view');
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('connecting twice at once opens one session, not two', async () => {
    const engine = await isolatedEngine();
    try {
      const profile = await engine.clients.save({ name: 'life-double-connect', url: engine.url });
      const before = engine.broker.view().clientCount ?? 0;
      await Promise.all([
        engine.clients.connect(profile.id),
        engine.clients.connect(profile.id),
        engine.clients.connect(profile.id),
      ]);
      await waitFor(() => (engine.broker.view().clientCount ?? 0) > before, { timeout: 5000, label: 'the session came up' });
      await sleep(400);
      eq((engine.broker.view().clientCount ?? 0) - before, 1, 'concurrent connect() opened more than one MQTT session');
      engine.clients.disconnect(profile.id);
      await waitFor(() => (engine.broker.view().clientCount ?? 0) === before, { timeout: 5000, label: 'the session closed' });
      eq(engine.clients.list().find((entry) => entry.id === profile.id)?.status, 'idle', 'status after disconnect');
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('renaming a live connection renames its incoming log rows', async () => {
    const engine = await isolatedEngine();
    let outsider = null;
    try {
      const profile = await engine.clients.save({ name: 'life-name-before', url: engine.url });
      await engine.clients.connect(profile.id);
      await waitFor(() => engine.clients.list().find((entry) => entry.id === profile.id)?.status === 'connected', {
        timeout: 5000,
        label: 'client connected',
      });
      const stored = engine.store.data.clients.find((entry) => entry.id === profile.id);
      await engine.clients.save({ ...stored, name: 'life-name-after' });
      await engine.clients.subscribe({ id: profile.id, topic: 'life/#' });
      outsider = await connectMqtt(engine.url);
      await publishAsync(outsider, 'life/rename', 'ping');
      const row = await waitFor(
        () =>
          engine.messages
            .tail(30)
            .find((entry) => entry.kind === 'client' && entry.clientId === profile.id && entry.topic === 'life/rename' && entry.direction === 'in'),
        { timeout: 5000, label: 'incoming message logged' },
      );
      eq(row.name, 'life-name-after', 'an incoming row kept the name the session was opened with');
    } finally {
      if (outsider !== null) await endMqtt(outsider).catch(() => {});
      await engine.dispose();
    }
  });

  await suite.test('a damaged state file is set aside, not silently overwritten', async () => {
    const temp = tempHome();
    try {
      // A legacy file holds the real work; the newer file was truncated by a crash.
      writeFileSync(
        join(temp.home, 'dsh-mqtt.json'),
        JSON.stringify({ version: 1, clients: [{ id: 'legacy-one', name: 'survivor', url: 'mqtt://127.0.0.1:1883' }] }),
        'utf8',
      );
      writeFileSync(join(temp.home, 'dsh-mqtt-tools.json'), '{"version":1,"clients":[', 'utf8');
      const store = new Store();
      await store.ready;
      eq(store.data.clients.some((client) => client.name === 'survivor'), true, 'the legacy profile was not adopted');
      const damaged = readdirSync(temp.home).filter((name) => name.startsWith('dsh-mqtt-tools.json.corrupt-'));
      eq(damaged.length, 1, 'the damaged file was not kept for inspection');
      store.update((state) => {
        state.broker.port = 1885;
      });
      await store.dispose();
      eq(readdirSync(temp.home).includes(damaged[0]), true, 'a later save deleted the damaged copy');
      const raw = JSON.parse(readFileSync(join(temp.home, 'dsh-mqtt-tools.json'), 'utf8'));
      eq(raw.broker.port, 1885, 'the repaired file was not written');
      eq(raw.clients.some((client) => client.name === 'survivor'), true, 'the adopted profile was lost on save');
    } finally {
      temp.cleanup();
    }
  });

  await suite.test('a failed connection can be dialled again once the broker is back', async () => {
    const engine = await isolatedEngine();
    try {
      const profile = await engine.clients.save({ name: 'life-retry', url: engine.url, reconnectPeriod: 0 });
      await engine.broker.stop();
      await engine.clients.connect(profile.id);
      await sleep(400);
      const failed = engine.clients.list().find((entry) => entry.id === profile.id);
      assert(failed.status !== 'connected', `the dead broker still reported connected (${failed.status})`);
      await engine.broker.start();
      await engine.clients.connect(profile.id);
      await waitFor(() => engine.clients.list().find((entry) => entry.id === profile.id)?.status === 'connected', {
        timeout: 5000,
        label: 'the second connect after the fix',
      });
      await waitFor(() => (engine.broker.view().clientCount ?? 0) >= 1, { timeout: 5000, label: 'the session reached the broker' });
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('saving a new dial setting drops the live session instead of publishing to the old broker', async () => {
    const engine = await isolatedEngine();
    try {
      const profile = await engine.clients.save({ name: 'life-rebind', url: engine.url, reconnectPeriod: 0 });
      await engine.clients.connect(profile.id);
      await waitFor(() => engine.clients.list().find((entry) => entry.id === profile.id)?.status === 'connected', {
        timeout: 5000,
        label: 'connected once',
      });
      const stored = engine.store.data.clients.find((entry) => entry.id === profile.id);
      const saved = await engine.clients.save({ ...stored, url: 'mqtt://127.0.0.1:1' });
      eq(saved.status, 'idle', 'a live session survived a dial change');
      await waitFor(() => (engine.broker.view().clientCount ?? 0) === 0, { timeout: 5000, label: 'the old session closed' });
      let refused = false;
      try {
        await engine.clients.publish({ id: profile.id, topic: 'life/after-save', payload: '1' });
      } catch {
        refused = true;
      }
      eq(refused, true, 'the client still published through the old session');
    } finally {
      await engine.dispose();
    }
  });

  // -------------------------------------------------------------------------
  suite.group('API route robustness (in process)');
  // -------------------------------------------------------------------------

  const { registerRoutes } = await loadPluginLib('routes');
  const routeHome = tempHome();
  const routeStore = new Store();
  await routeStore.ready;
  const routeHub = new Hub();
  const engineStub = {
    snapshot: () => ({ clients: [], broker: { status: 'stopped' } }),
    command: async (cmd) => {
      if (cmd === 'boom') throw new Error('engine refused');
      return { echoed: cmd };
    },
    hub: routeHub,
    messages: () => [],
    events: () => [],
    hello: () => ({ state: { clients: [] }, messages: [], events: [] }),
  };
  const mount = fakeContext();
  registerRoutes({ ctx: mount.ctx, engine: engineStub });

  const route = (path) => {
    const found = mount.routes.find((entry) => entry.path === path);
    assert(found !== undefined, `route ${path} was not registered`);
    return found;
  };
  const call = async (path, req) => {
    const res = fakeRes();
    await route(path).handler(req, res);
    return res;
  };

  await suite.test('GET /state answers JSON with the snapshot', async () => {
    const res = await call('/dsh-mqtt-tools/api/state', fakeReq({ headers: { origin: 'http://127.0.0.1:6871' } }));
    eq(res.status, 200, 'status');
    eq(res.json?.ok, true, 'ok flag');
    assert(res.json?.state !== undefined, 'state missing');
  });

  await suite.test('POST /command without the CSRF header is refused', async () => {
    const res = await call(
      '/dsh-mqtt-tools/api/command',
      fakeReq({ method: 'POST', headers: {}, body: '{"cmd":"broker.stop"}' }),
    );
    eq(res.status, 403, 'status');
    eq(res.json?.ok, false, 'ok flag');
  });

  await suite.test('POST /command from a foreign origin is refused', async () => {
    const res = await call(
      '/dsh-mqtt-tools/api/command',
      fakeReq({ method: 'POST', headers: { 'x-dsh-mqtt-tools': '1', origin: 'http://evil.example' }, body: '{"cmd":"broker.stop"}' }),
    );
    eq(res.status, 403, 'status');
  });

  await suite.test('the wrong method is rejected, not executed', async () => {
    const res = await call('/dsh-mqtt-tools/api/command', fakeReq({ method: 'GET' }));
    eq(res.status, 405, 'status');
    eq(res.json?.ok, false, 'ok flag');
  });

  await suite.test('an engine failure is reported as JSON, not a crash', async () => {
    const res = await call(
      '/dsh-mqtt-tools/api/command',
      fakeReq({ method: 'POST', headers: { 'x-dsh-mqtt-tools': '1' }, body: '{"cmd":"boom"}' }),
    );
    eq(res.status, 200, 'status');
    eq(res.json?.ok, false, 'ok flag');
    assert(String(res.json?.error).includes('engine refused'), 'error text lost');
  });

  await suite.test('malformed JSON is a JSON error', async () => {
    const res = await call(
      '/dsh-mqtt-tools/api/command',
      fakeReq({ method: 'POST', headers: { 'x-dsh-mqtt-tools': '1' }, body: '{"cmd":' }),
    );
    eq(res.headers?.['content-type']?.startsWith('application/json'), true, 'not a JSON response');
    eq(res.json?.ok, false, 'ok flag');
  });

  await suite.test('an oversized body is refused without reading all of it', async () => {
    const huge = 'x'.repeat(2 * 1024 * 1024 + 16);
    const res = await call(
      '/dsh-mqtt-tools/api/command',
      fakeReq({ method: 'POST', headers: { 'x-dsh-mqtt-tools': '1' }, body: `{"cmd":"broker.publish","payload":"${huge}"}` }),
    );
    eq(res.json?.ok, false, 'ok flag');
    assert(/too large/i.test(String(res.json?.error)), `unexpected error: ${res.json?.error}`);
  });

  await suite.test('a non-object body is treated as a missing command', async () => {
    const res = await call(
      '/dsh-mqtt-tools/api/command',
      fakeReq({ method: 'POST', headers: { 'x-dsh-mqtt-tools': '1' }, body: '"just a string"' }),
    );
    eq(res.status, 400, 'status');
  });

  await suite.test('a handler with a dead socket does not reject', async () => {
    const res = fakeRes({ deadSocket: true });
    let rejected = null;
    try {
      await route('/dsh-mqtt-tools/api/state').handler(fakeReq(), res);
    } catch (error) {
      rejected = error;
    }
    eq(rejected, null, `route handler rejected: ${rejected?.message}`);
  });

  await suite.test('the SSE route opens a stream and cleans up on close', async () => {
    const req = fakeReq();
    const res = fakeRes();
    await route('/dsh-mqtt-tools/api/events').handler(req, res);
    assert(res.text.includes('event: hello'), 'no hello frame on the SSE route');
    eq(routeHub.clients.size, 1, 'stream not registered');
    req.emit('close');
    eq(routeHub.clients.size, 0, 'stream leaked after close');
  });

  routeHub.dispose();
  await routeStore.dispose();
  routeHome.cleanup();

  // -------------------------------------------------------------------------
  suite.group('plugin mount / dispose');
  // -------------------------------------------------------------------------

  await suite.test('apply registers 3 routes and 9 tools, and a duplicate apply is refused', async () => {
    const temp = tempHome();
    try {
      const plugin = await import('../../index.js');
      eq(plugin.inject, ['webServer', 'tools'], 'inject list');
      const first = fakeContext();
      plugin.apply(first.ctx);
      await sleep(150);
      eq(first.routes.length, 3, 'routes registered');
      eq(first.tools.length, 9, 'tools registered');
      const names = first.tools.map((tool) => tool.name).sort();
      eq(names, [
        'mqtt_broker',
        'mqtt_connect',
        'mqtt_disconnect',
        'mqtt_list',
        'mqtt_messages',
        'mqtt_publish',
        'mqtt_subscribe',
        'mqtt_unsubscribe',
        'mqtt_wait',
      ], 'tool names');
      const second = fakeContext();
      plugin.apply(second.ctx);
      eq(second.routes.length, 0, 'duplicate apply registered routes');
      eq(second.tools.length, 0, 'duplicate apply registered tools');
      first.dispose();
      const third = fakeContext();
      plugin.apply(third.ctx);
      await sleep(150);
      eq(third.routes.length, 3, 'remount after dispose');
      eq(third.tools.length, 9, 'tools after remount');
      third.dispose();
    } finally {
      temp.cleanup();
    }
  });

  await suite.test('dispose stops the broker so its port is free again', async () => {
    const temp = tempHome();
    try {
      const plugin = await import('../../index.js');
      const home = temp.home;
      // ask the plugin to start a broker on a fixed port by pre-seeding the state file
      const port = 18711;
      writeFileSync(
        join(home, 'dsh-mqtt-tools.json'),
        JSON.stringify({
          version: 1,
          clients: [],
          broker: { host: '127.0.0.1', port, autoStart: true, allowAnonymous: true, protocols: ['3.1', '3.1.1', '5.0'], users: [] },
          log: { showSystem: false },
        }),
        'utf8',
      );
      const mountCtx = fakeContext();
      plugin.apply(mountCtx.ctx);
      await sleep(400);
      const probe = net.createServer();
      let reachable = true;
      probe.once('error', () => {});
      await new Promise((resolve) => {
        probe.once('error', () => {
          reachable = false;
          resolve();
        });
        probe.listen(port, '127.0.0.1', () => {
          reachable = true;
          probe.close(() => resolve());
        });
      });
      // the plugin's broker already owns the port, so our probe must have failed
      eq(reachable, false, 'the plugin broker was not listening after autoStart');
      mountCtx.dispose();
      await sleep(250);
      const after = net.createServer();
      await new Promise((resolve, reject) => {
        after.once('error', reject);
        after.listen(port, '127.0.0.1', resolve);
      });
      await new Promise((resolve) => after.close(resolve));
    } finally {
      temp.cleanup();
    }
  });
});
