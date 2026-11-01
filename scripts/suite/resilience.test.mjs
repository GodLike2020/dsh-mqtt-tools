// Broker lifecycle, resource limits and disposal suite for dsh-mqtt-tools.
//
// These are the failure modes that only appear under concurrency or abuse:
// a panel double-click that starts the broker twice, a stop racing a start,
// a socket that never sends CONNECT, a client that publishes a 100 MB payload,
// a retained-topic flood, and a disposal that reports "stopped" while the port
// is still bound. Every case uses an isolated engine and its own port.
import net from 'node:net';
import {
  Suite,
  runSuite,
  isolatedEngine,
  connectMqtt,
  connectMqttError,
  endMqtt,
  publishAsync,
  openSocket,
  assert,
  eq,
  waitFor,
  sleep,
  loadPluginLib,
} from './harness.mjs';

const suite = new Suite('resilience', 'Broker lifecycle, resource limits and disposal', {
  note: 'isolated engines and temporary state files only',
});

/** Is the TCP port accepting connections right now? */
function portOpen(port, host = '127.0.0.1', timeout = 800) {
  return new Promise((resolve) => {
    const socket = net.connect({ port, host });
    let settled = false;
    const finish = (open) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(open);
    };
    socket.setTimeout(timeout);
    socket.once('connect', () => finish(true));
    socket.once('timeout', () => finish(false));
    socket.once('error', () => finish(false));
  });
}

function closedWithin(client, ms = 3000) {
  return new Promise((resolve) => {
    if (!client || client.connected === false) return resolve(true);
    const timer = setTimeout(() => resolve(false), ms);
    client.once('close', () => {
      clearTimeout(timer);
      resolve(true);
    });
  });
}

const brokerLimits = (broker) => {
  const settings = broker.settings();
  return {
    maxConnections: settings.maxConnections,
    connectTimeoutMs: settings.connectTimeoutMs,
    maxPayloadBytes: settings.maxPayloadBytes,
    maxRetainedMessages: settings.maxRetainedMessages,
  };
};

await runSuite(suite, async () => {
  suite.group('limits in the model');

  await suite.test('a fresh broker has finite resource limits', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      eq(brokerLimits(engine.broker), {
        maxConnections: 200,
        connectTimeoutMs: 10000,
        maxPayloadBytes: 1024 * 1024,
        maxRetainedMessages: 1000,
      });
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('out-of-range limits are clamped, never accepted blindly', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      engine.broker.saveSettings({
        maxConnections: 999999,
        connectTimeoutMs: 1,
        maxPayloadBytes: 12,
        maxRetainedMessages: -3,
      });
      eq(brokerLimits(engine.broker), {
        maxConnections: 5000,
        connectTimeoutMs: 500,
        maxPayloadBytes: 1024,
        maxRetainedMessages: 0,
      });
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('limits survive a save/flush/reload through the state file', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      engine.broker.saveSettings({
        maxConnections: 7,
        connectTimeoutMs: 2500,
        maxPayloadBytes: 65536,
        maxRetainedMessages: 3,
      });
      await engine.store.flush();
      const { Store } = await loadPluginLib('store');
      const reloaded = new Store();
      await reloaded.ready;
      const { maxConnections, connectTimeoutMs, maxPayloadBytes, maxRetainedMessages } = reloaded.data.broker;
      eq(
        { maxConnections, connectTimeoutMs, maxPayloadBytes, maxRetainedMessages },
        {
          maxConnections: 7,
          connectTimeoutMs: 2500,
          maxPayloadBytes: 65536,
          maxRetainedMessages: 3,
        },
      );
      await reloaded.dispose();
    } finally {
      await engine.dispose();
    }
  });

  suite.group('lifecycle serialisation');

  await suite.test('two concurrent starts create exactly one listener', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      const [first, second] = await Promise.all([engine.broker.start(), engine.broker.start()]);
      eq(first.status, 'running');
      eq(second.status, 'running');
      eq(first.boundPort, second.boundPort, 'bound ports');
      eq(first.boundPort, engine.port, 'the requested port');
      eq(await portOpen(engine.port), true);
      const starts = engine.events.tail(50).filter((entry) => entry.action === 'start').length;
      eq(starts, 1, 'start events');
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('a stop racing a start ends stopped with the port free, then restarts', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      const starting = engine.broker.start();
      const stopping = engine.broker.stop();
      await Promise.allSettled([starting, stopping]);
      eq(engine.broker.view().status, 'stopped');
      eq(await portOpen(engine.port), false, 'port after the race');
      const restarted = await engine.broker.start();
      eq(restarted.status, 'running');
      eq(restarted.boundPort, engine.port);
      eq(await portOpen(engine.port), true);
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('a double stop is a no-op, not an error', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      await engine.broker.start();
      const first = await engine.broker.stop();
      const second = await engine.broker.stop();
      eq(first.status, 'stopped');
      eq(second.status, 'stopped');
      eq(await portOpen(engine.port), false);
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('dispose() waits for the port to be released', async () => {
    const engine = await isolatedEngine();
    try {
      const client = await connectMqtt(engine.url);
      const closed = closedWithin(client);
      await engine.broker.dispose();
      eq(await closed, true, 'the connected client is disconnected');
      eq(await portOpen(engine.port), false, 'port after dispose');
      eq(engine.broker.view().status, 'stopped');
    } finally {
      await engine.dispose();
    }
  });

  suite.group('accept guards');

  await suite.test('a socket that never sends CONNECT is closed and logged', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      engine.broker.saveSettings({ connectTimeoutMs: 700 });
      await engine.broker.start();
      const socket = await openSocket(engine.port);
      const closed = await new Promise((resolve) => {
        const timer = setTimeout(() => resolve(false), 4000);
        socket.once('close', () => {
          clearTimeout(timer);
          resolve(true);
        });
        socket.on('error', () => {});
      });
      eq(closed, true, 'the silent socket was closed');
      const logged = await waitFor(
        () => engine.events.tail(80).find((entry) => /no MQTT CONNECT/.test(entry.text ?? '')),
        { timeout: 2000, label: 'connect timeout event' },
      );
      assert(Boolean(logged), 'the timeout is visible in the broker log');
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('connections beyond maxConnections are refused, existing ones stay', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      engine.broker.saveSettings({ maxConnections: 1 });
      await engine.broker.start();
      const first = await connectMqtt(engine.url);
      const error = await connectMqttError(engine.url);
      assert(error instanceof Error, 'the second connection was refused');
      eq(first.connected, true, 'the first client is still connected');
      await waitFor(
        () => engine.events.tail(80).find((entry) => /already holds 1 connections/.test(entry.text ?? '')),
        { timeout: 2000, label: 'connection cap event' },
      );
      await endMqtt(first);
    } finally {
      await engine.dispose();
    }
  });

  suite.group('payload and retained budgets');

  await suite.test('an oversized client publish is refused and the broker stays healthy', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      engine.broker.saveSettings({ maxPayloadBytes: 2048, maxRetainedMessages: 1000 });
      await engine.broker.start();
      const client = await connectMqtt(engine.url);
      const closed = closedWithin(client, 4000);
      await Promise.race([publishAsync(client, 'cap/big', Buffer.alloc(4096, 7), { qos: 1 }).catch(() => {}), sleep(2500)]);
      eq(await closed, true, 'oversized publish disconnects the client');
      const healthy = await connectMqtt(engine.url);
      try {
        await publishAsync(healthy, 'cap/small', 'ok', { qos: 0 });
        await waitFor(() => engine.messages.tail(80).find((entry) => entry.topic === 'cap/small'), {
          timeout: 2500,
          label: 'small publish after the refusal',
        });
      } finally {
        await endMqtt(healthy);
      }
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('an oversized server-side publish fails with a readable error', async () => {
    const engine = await isolatedEngine();
    try {
      engine.broker.saveSettings({ maxPayloadBytes: 1024 });
      let failure = null;
      try {
        await engine.broker.publish({ topic: 'cap/server', payload: 'x'.repeat(2048) });
      } catch (error) {
        failure = error;
      }
      assert(failure instanceof Error, 'the publish was refused');
      assert(/maxPayloadBytes/.test(failure.message), `mentions the setting: ${failure.message}`);
      const small = await engine.broker.publish({ topic: 'cap/server', payload: 'small' });
      eq(small.bytes, 5);
    } finally {
      await engine.dispose();
    }
  });

  await suite.test('the retained cap refuses the next retained topic and tracks clears', async () => {
    const engine = await isolatedEngine({ autoStart: false });
    try {
      engine.broker.saveSettings({ maxRetainedMessages: 2, maxPayloadBytes: 1024 * 1024 });
      await engine.broker.start();
      const first = await connectMqtt(engine.url);
      try {
        await publishAsync(first, 'ret/one', '1', { retain: true, qos: 0 });
        await publishAsync(first, 'ret/two', '2', { retain: true, qos: 0 });
        await waitFor(() => engine.broker.retainedTopics.size === 2, {
          timeout: 2500,
          label: 'retained topics tracked',
        });
        const closed = closedWithin(first, 4000);
        await Promise.race([publishAsync(first, 'ret/three', '3', { retain: true, qos: 0 }).catch(() => {}), sleep(2500)]);
        eq(await closed, true, 'the retained flood is disconnected');
        eq(engine.broker.retainedTopics.size, 2, 'tracked retained topics');

        let failure = null;
        try {
          await engine.broker.publish({ topic: 'ret/three', payload: '3', retain: true });
        } catch (error) {
          failure = error;
        }
        assert(failure instanceof Error, 'server-side retained publish refused');
        assert(/maxRetainedMessages/.test(failure.message), `mentions the setting: ${failure.message}`);

        const second = await connectMqtt(engine.url);
        try {
          await publishAsync(second, 'ret/one', '', { retain: true, qos: 0 });
          await waitFor(() => engine.broker.retainedTopics.size === 1, {
            timeout: 2500,
            label: 'cleared retained topic',
          });
          await engine.broker.publish({ topic: 'ret/three', payload: '3', retain: true });
          await waitFor(() => engine.broker.retainedTopics.size === 2, {
            timeout: 2500,
            label: 'slot freed and reused',
          });
        } finally {
          await endMqtt(second);
        }
      } finally {
        await endMqtt(first);
      }
    } finally {
      await engine.dispose();
    }
  });
});
