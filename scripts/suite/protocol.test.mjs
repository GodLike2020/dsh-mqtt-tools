/**
 * MQTT broker protocol conformance suite — adversarial, byte-level checks.
 *
 * Everything runs against an isolated engine (temp DSH_HOME, random port, real
 * Store/RingLog/ClientManager/BrokerManager). Library clients (mqtt.js) are used
 * where the wire format is not the subject; raw sockets + generated packets are
 * used wherever mqtt.js would hide or repair the behaviour under test.
 *
 * Run alone:  node scripts/suite/protocol.test.mjs
 * Run all:    node scripts/suite/run-all.mjs
 */
import mqtt from 'mqtt'
import {
  isolatedEngine, endMqtt, subscribeAsync, publishAsync, recorder,
  openSocket, packetReader, generatePacket,
  assert, eq, waitFor, sleep, Suite, runSuite,
} from './harness.mjs'

// ---------------------------------------------------------------------------
// Local byte-level MQTT plumbing (deliberately not harness library code).
// ---------------------------------------------------------------------------

let counter = 0
function uid(prefix) {
  counter += 1
  return `${prefix}-${process.pid}-${counter}`
}

/** MQTT variable-byte integer (remaining length / properties length). */
function varint(value) {
  const bytes = []
  let rest = value
  do {
    let digit = rest % 128
    rest = Math.floor(rest / 128)
    if (rest > 0) digit |= 0x80
    bytes.push(digit)
  } while (rest > 0)
  return Buffer.from(bytes)
}

function utf8Field(text) {
  const bytes = Buffer.from(String(text), 'utf8')
  const length = Buffer.alloc(2)
  length.writeUInt16BE(bytes.length)
  return Buffer.concat([length, bytes])
}

/** Build a CONNECT packet byte-for-byte from scratch. */
function buildConnect({
  protocolName = 'MQTT', level = 4, clean = true, keepalive = 60,
  clientId = 'raw-client', will = null,
} = {}) {
  let flags = clean ? 0x02 : 0x00
  let payload = Buffer.concat([
    Buffer.from([(keepalive >> 8) & 0xff, keepalive & 0xff]),
    utf8Field(clientId),
  ])
  if (will) {
    flags |= 0x04 | ((Number(will.qos ?? 0) & 0x03) << 3) | (will.retain ? 0x20 : 0x00)
    payload = Buffer.concat([payload, utf8Field(will.topic), utf8Field(will.payload ?? '')])
  }
  const body = Buffer.concat([utf8Field(protocolName), Buffer.from([level, flags]), payload])
  return Buffer.concat([Buffer.from([0x10]), varint(body.length), body])
}

/** Build a PUBLISH packet. `topic` is deliberately not validated. */
function buildPublish({ topic, payload = Buffer.alloc(0), qos = 0, messageId = 1, retain = false } = {}) {
  const bytes = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), 'utf8')
  const parts = [utf8Field(topic)]
  if (qos > 0) parts.push(Buffer.from([(messageId >> 8) & 0xff, messageId & 0xff]))
  parts.push(bytes)
  const body = Buffer.concat(parts)
  const header = 0x30 | (qos << 1) | (retain ? 0x01 : 0x00)
  return Buffer.concat([Buffer.from([header]), varint(body.length), body])
}

/** Connect a raw socket and collect every byte the broker sends back. */
async function rawConnection(port, protocolVersion = 4) {
  const socket = await openSocket(port)
  socket.on('error', () => {}) // a reset socket must not crash the suite
  const packets = packetReader(socket, protocolVersion)
  const chunks = []
  socket.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
  const closed = new Promise((resolve) => {
    if (socket.destroyed) resolve()
    else socket.once('close', () => resolve())
  })
  return {
    socket,
    packets,
    closed,
    bytes: () => Buffer.concat(chunks),
    hex: () => Buffer.concat(chunks).toString('hex'),
    destroy: () => {
      try { socket.destroy() } catch { /* already gone */ }
    },
  }
}

/** Await socket close; reject the test if the broker does not close in time. */
async function expectClosed(raw, timeout, label) {
  let timer
  const guard = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label}: broker did not close the connection within ${timeout} ms`)), timeout)
  })
  try {
    await Promise.race([raw.closed, guard])
  } finally {
    clearTimeout(timer)
  }
}

/** Assert nothing matching `predicate` was delivered for a quiet window. */
async function expectNone(rec, predicate, { ms = 600, label = 'message' } = {}) {
  await sleep(ms)
  const unwanted = rec.messages.filter(predicate)
  const seen = unwanted.map((message) => [message.topic, message.payload.toString()])
  assert(unwanted.length === 0, `${label}: expected no delivery, got ${JSON.stringify(seen)}`)
}

/** Connect a plain mqtt.js client and record everything it receives. */
async function connectRecording(url, options = {}) {
  const client = mqtt.connect(url, { connectTimeout: 8000, reconnectPeriod: 0, ...options })
  const rec = recorder(client)
  client.on('error', () => {}) // late resets are observed by assertions, not crashes
  const connack = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      try { client.end(true) } catch { /* noop */ }
      reject(new Error(`connect timeout for ${JSON.stringify(options.clientId ?? 'client')}`))
    }, 9000)
    client.once('connect', (packet) => { clearTimeout(timer); resolve(packet) })
    client.once('error', (error) => {
      clearTimeout(timer)
      reject(error)
    })
  })
  return { client, rec, connack }
}

/** Liveness probe: a normal client must still be able to connect and publish. */
async function assertHealthy(url, label) {
  const probe = await connectRecording(url, { clientId: uid('health'), clean: true })
  try {
    await subscribeAsync(probe.client, 'health/probe', 1)
    const payload = `alive-${label}`
    await publishAsync(probe.client, 'health/probe', payload, { qos: 1, timeout: 8000 })
    const message = await probe.rec.waitFor((m) => m.topic === 'health/probe' && m.payload.toString() === payload, {
      timeout: 6000, label: `health probe after ${label}`,
    })
    assert(message !== undefined, `broker unhealthy after ${label}: no delivery`)
  } finally {
    await endMqtt(probe.client)
  }
}

async function stopClient(client, broker, clientId, label) {
  await endMqtt(client)
  await waitFor(() => !broker.view().clients.some((entry) => entry.id === clientId), {
    timeout: 3000, label: `${label}: broker forgot client ${clientId} after disconnect`,
  })
}

/** Send a real DISCONNECT (force=false) and wait for the stream to close. */
function endGracefully(client) {
  return new Promise((resolve) => {
    if (client.connected === false) return resolve()
    client.once('close', () => resolve())
    client.end(false)
  })
}

const suite = new Suite('protocol', 'MQTT broker protocol conformance', {
  note: 'adversarial will/takeover/session/retain/wildcard/QoS/keepalive/abuse/gating checks',
})

await runSuite(suite, async () => {
  const engine = await isolatedEngine()
  const { url, broker, events } = engine
  const port = engine.port
  const clients = new Set()
  const sockets = new Set()

  const newClient = async (options = {}) => {
    const made = await connectRecording(url, options)
    clients.add(made.client)
    return made
  }
  const done = async (client) => {
    clients.delete(client)
    await endMqtt(client)
  }
  const rawConnect = async (protocolVersion = 4) => {
    const raw = await rawConnection(port, protocolVersion)
    sockets.add(raw)
    return raw
  }
  const closeRaw = (raw) => {
    sockets.delete(raw)
    raw.destroy()
  }

  try {
    // -----------------------------------------------------------------------
    // 1. Will messages
    // -----------------------------------------------------------------------
    await suite.test('will is published when the client dies without DISCONNECT', async () => {
      const watcher = await newClient({ clientId: uid('will-watch'), clean: true })
      await subscribeAsync(watcher.client, 'will/death', 1)
      const dying = await newClient({
        clientId: uid('will-dying'), clean: true,
        will: { topic: 'will/death', payload: Buffer.from('gone'), qos: 1, retain: false },
      })
      await sleep(100)
      dying.client.stream.destroy()
      const message = await watcher.rec.waitFor((m) => m.topic === 'will/death', { timeout: 4000, label: 'will message' })
      eq(message.topic, 'will/death', 'will topic')
      eq(message.payload.toString(), 'gone', 'will payload')
      eq(message.packet.qos, 1, 'will QoS honoured')
      await done(watcher.client)
    })

    await suite.test('a clean DISCONNECT publishes no will', async () => {
      const watcher = await newClient({ clientId: uid('will2-watch'), clean: true })
      await subscribeAsync(watcher.client, 'will/clean', 1)
      const leaving = await newClient({
        clientId: uid('will2-leave'), clean: true,
        will: { topic: 'will/clean', payload: 'should-not-appear', qos: 1 },
      })
      await sleep(100)
      clients.delete(leaving.client)
      await endGracefully(leaving.client)
      await expectNone(watcher.rec, (m) => m.topic === 'will/clean', { ms: 700, label: 'will after DISCONNECT' })
      await done(watcher.client)
    })

    await suite.test('will retain flag is honoured for later subscribers', async () => {
      const watcher = await newClient({ clientId: uid('will3-watch'), clean: true })
      await subscribeAsync(watcher.client, 'will/retained', 1)
      const dying = await newClient({
        clientId: uid('will3-dying'), clean: true,
        will: { topic: 'will/retained', payload: 'last-words', qos: 1, retain: true },
      })
      await sleep(100)
      dying.client.stream.destroy()
      // The live copy reaches the existing subscriber (retain is only set on replay).
      const live = await watcher.rec.waitFor((m) => m.topic === 'will/retained', { timeout: 4000, label: 'will published on death' })
      eq(live.payload.toString(), 'last-words', 'live will payload')
      const later = await newClient({ clientId: uid('will3-later'), clean: true })
      try {
        await subscribeAsync(later.client, 'will/retained', 1)
        const message = await later.rec.waitFor((m) => m.topic === 'will/retained', { timeout: 3000, label: 'replayed retained will' })
        eq(message.payload.toString(), 'last-words', 'retained will payload')
        eq(message.packet.retain, true, 'replayed retained will has retain=true')
      } finally {
        await done(later.client)
        await done(watcher.client)
      }
    })

    // -----------------------------------------------------------------------
    // 2. Duplicate clientId takeover
    // -----------------------------------------------------------------------
    await suite.test('a second connect with the same clientId kicks the first, broker keeps serving', async () => {
      const clientId = uid('takeover')
      const first = await newClient({ clientId, clean: true })
      const firstClosed = new Promise((resolve) => first.client.once('close', resolve))
      const second = await newClient({ clientId, clean: true })
      clients.delete(first.client) // already closed by the broker
      await Promise.race([
        firstClosed,
        sleep(4000).then(() => { throw new Error('the first connection was not closed on takeover') }),
      ])
      eq(first.client.connected, false, 'first client is disconnected after takeover')
      await waitFor(() => {
        const listed = broker.view().clients.filter((entry) => entry.id === clientId)
        return listed.length === 1
      }, { timeout: 3000, label: 'broker map has exactly one client after takeover' })
      const idEvents = events.items.filter((entry) => entry.clientId === clientId && (entry.action === 'connect' || entry.action === 'disconnect'))
      assert(idEvents.length >= 2, `expected connect/disconnect events for ${clientId}, saw ${JSON.stringify(idEvents.map((e) => e.action))}`)
      eq(idEvents.at(-2).action, 'disconnect', 'old connection logged as disconnected before the new connect')
      eq(idEvents.at(-1).action, 'connect', 'new connection logged as connect')
      const probe = await newClient({ clientId: uid('takeover-probe'), clean: true })
      try {
        await subscribeAsync(probe.client, 'takeover/alive', 1)
        await publishAsync(second.client, 'takeover/alive', 'still-serving', { qos: 1 })
        const got = await probe.rec.waitFor((m) => m.topic === 'takeover/alive', { timeout: 3000, label: 'broker serving after takeover' })
        eq(got.payload.toString(), 'still-serving', 'broker serves the surviving client')
      } finally {
        await done(probe.client)
        await done(second.client)
      }
    })

    // -----------------------------------------------------------------------
    // 3. Persistent sessions (clean:false)
    // -----------------------------------------------------------------------
    await suite.test('clean:false keeps the subscription and queues offline QoS 1, sets sessionPresent', async () => {
      const clientId = uid('persist')
      const first = await newClient({ clientId, clean: false })
      const firstConnack = first.connack
      eq(firstConnack.sessionPresent, false, 'first clean:false CONNACK has no existing session')
      await subscribeAsync(first.client, 'persist/queued', 1)
      await stopClient(first.client, broker, clientId, 'persist first connect')
      clients.delete(first.client)

      const publisher = await newClient({ clientId: uid('persist-pub'), clean: true })
      await publishAsync(publisher.client, 'persist/queued', 'qos1-offline', { qos: 1 })
      await publishAsync(publisher.client, 'persist/queued', 'qos0-offline', { qos: 0 })
      await sleep(200)

      const second = await newClient({ clientId, clean: false })
      try {
        eq(second.connack.sessionPresent, true, 'reconnect with clean:false reports the stored session')
        const message = await second.rec.waitFor((m) => m.topic === 'persist/queued', { timeout: 4000, label: 'queued QoS 1 message' })
        eq(message.payload.toString(), 'qos1-offline', 'the queued QoS 1 message arrives first')
        eq(message.packet.qos, 1, 'queued message keeps QoS 1')
        await expectNone(second.rec, (m) => m.payload.toString() === 'qos0-offline', {
          ms: 600, label: 'QoS 0 must not be queued for offline sessions',
        })
        // The subscription survived without a new SUBSCRIBE.
        await publishAsync(publisher.client, 'persist/queued', 'after-reconnect', { qos: 1 })
        const after = await second.rec.waitFor((m) => m.payload.toString() === 'after-reconnect', { timeout: 3000, label: 'restored subscription' })
        eq(after.topic, 'persist/queued', 'restored subscription routes live publishes')
      } finally {
        await done(second.client)
        await done(publisher.client)
      }
    })

    await suite.test('clean:true discards the stored session and queued messages', async () => {
      const clientId = uid('clean-session')
      const first = await newClient({ clientId, clean: false })
      await subscribeAsync(first.client, 'clean/dropped', 1)
      await stopClient(first.client, broker, clientId, 'clean test first connect')
      clients.delete(first.client)
      const publisher = await newClient({ clientId: uid('clean-pub'), clean: true })
      try {
        await publishAsync(publisher.client, 'clean/dropped', 'queued-then-dropped', { qos: 1 })
        await sleep(150)
        const second = await newClient({ clientId, clean: true })
        try {
          eq(second.connack.sessionPresent, false, 'clean:true CONNACK must not claim a session')
          await expectNone(second.rec, (m) => m.topic === 'clean/dropped', { ms: 700, label: 'clean:true dropped queued message' })
        } finally {
          await done(second.client)
        }
      } finally {
        await done(publisher.client)
      }
    })

    // -----------------------------------------------------------------------
    // 4. Retained messages
    // -----------------------------------------------------------------------
    await suite.test('retained publish is replayed to a new subscriber', async () => {
      const publisher = await newClient({ clientId: uid('retain-pub'), clean: true })
      try {
        await publishAsync(publisher.client, 'retain/topic', 'v1', { qos: 1, retain: true })
        const subscriber = await newClient({ clientId: uid('retain-sub'), clean: true })
        try {
          await subscribeAsync(subscriber.client, 'retain/topic', 1)
          const message = await subscriber.rec.waitFor((m) => m.topic === 'retain/topic', { timeout: 3000, label: 'retained replay' })
          eq(message.payload.toString(), 'v1', 'retained payload')
          eq(message.packet.retain, true, 'retained replay keeps retain=true')
        } finally {
          await done(subscriber.client)
        }
      } finally {
        await done(publisher.client)
      }
    })

    await suite.test('a retained publish overwrites the previous retained message', async () => {
      const publisher = await newClient({ clientId: uid('retain2-pub'), clean: true })
      try {
        await publishAsync(publisher.client, 'retain/over', 'old', { qos: 1, retain: true })
        await publishAsync(publisher.client, 'retain/over', 'new', { qos: 1, retain: true })
        const subscriber = await newClient({ clientId: uid('retain2-sub'), clean: true })
        try {
          await subscribeAsync(subscriber.client, 'retain/over', 1)
          const message = await subscriber.rec.waitFor((m) => m.topic === 'retain/over', { timeout: 3000, label: 'overwritten retained replay' })
          eq(message.payload.toString(), 'new', 'only the latest retained value is replayed')
        } finally {
          await done(subscriber.client)
        }
      } finally {
        await done(publisher.client)
      }
    })

    await suite.test('an empty retained payload clears the retained message', async () => {
      const publisher = await newClient({ clientId: uid('retain3-pub'), clean: true })
      const subscriber = await newClient({ clientId: uid('retain3-sub'), clean: true })
      try {
        await subscribeAsync(subscriber.client, 'retain/clear', 1)
        await publishAsync(publisher.client, 'retain/clear', 'sticky', { qos: 1, retain: true })
        await subscriber.rec.waitFor((m) => m.topic === 'retain/clear' && m.payload.toString() === 'sticky', {
          timeout: 3000, label: 'live retained publish delivered',
        })
        // Clearing is an empty retained payload: it is delivered live, then gone for new subscribers.
        await publishAsync(publisher.client, 'retain/clear', '', { qos: 1, retain: true })
        await subscriber.rec.waitFor((m) => m.topic === 'retain/clear' && m.payload.length === 0, {
          timeout: 3000, label: 'empty retained publish delivered live',
        })
        const late = await newClient({ clientId: uid('retain3-late'), clean: true })
        try {
          await subscribeAsync(late.client, 'retain/clear', 1)
          await expectNone(late.rec, (m) => m.topic === 'retain/clear', { ms: 600, label: 'cleared retained message' })
        } finally {
          await done(late.client)
        }
      } finally {
        await done(subscriber.client)
        await done(publisher.client)
      }
    })

    // -----------------------------------------------------------------------
    // 5. Wildcards and edge topics
    // -----------------------------------------------------------------------
    await suite.test("'a/+/c' matches exactly one level", async () => {
      const sub = await newClient({ clientId: uid('wild-plus'), clean: true })
      const pub = await newClient({ clientId: uid('wild-plus-pub'), clean: true })
      try {
        await subscribeAsync(sub.client, 'a/+/c', 0)
        await publishAsync(pub.client, 'a/b/c', 'one', { qos: 0 })
        await publishAsync(pub.client, 'a/x/c', 'two', { qos: 0 })
        await publishAsync(pub.client, 'a/b/d/c', 'three', { qos: 0 })
        await publishAsync(pub.client, 'a/c', 'four', { qos: 0 })
        const one = await sub.rec.waitFor((m) => m.payload.toString() === 'one', { timeout: 3000, label: 'a/b/c' })
        eq(one.topic, 'a/b/c', 'single-level wildcard match')
        await sub.rec.waitFor((m) => m.payload.toString() === 'two', { timeout: 3000, label: 'a/x/c' })
        await expectNone(sub.rec, (m) => ['three', 'four'].includes(m.payload.toString()), {
          ms: 500, label: "'a/+/c' must not match two levels or fewer levels",
        })
      } finally {
        await done(pub.client)
        await done(sub.client)
      }
    })

    await suite.test("'a/#' matches the subtree and the parent 'a'", async () => {
      const sub = await newClient({ clientId: uid('wild-hash'), clean: true })
      const pub = await newClient({ clientId: uid('wild-hash-pub'), clean: true })
      try {
        await subscribeAsync(sub.client, 'subtree/#', 1)
        await publishAsync(pub.client, 'subtree/deep/er', 'deep', { qos: 1 })
        await sub.rec.waitFor((m) => m.payload.toString() === 'deep', { timeout: 3000, label: "'a/#' matches a/b/c" })
        await publishAsync(pub.client, 'subtree', 'parent', { qos: 1 })
        await sub.rec.waitFor((m) => m.payload.toString() === 'parent', { timeout: 3000, label: "'a/#' must match the parent topic 'a' (MQTT-4.7.1-2)" })
        await publishAsync(pub.client, 'subtree-other/x', 'unrelated', { qos: 1 })
        await expectNone(sub.rec, (m) => m.payload.toString() === 'unrelated', { ms: 500, label: "unrelated topic under 'subtree/#'" })
      } finally {
        await done(pub.client)
        await done(sub.client)
      }
    })

    await suite.test("plain '#' matches every non-$ topic but never $SYS", async () => {
      const hashSub = await newClient({ clientId: uid('wild-all'), clean: true })
      const another = await newClient({ clientId: uid('wild-all-other'), clean: true })
      try {
        await subscribeAsync(hashSub.client, '#', 0)
        await publishAsync(another.client, 'loose/topic', 'caught', { qos: 0 })
        await hashSub.rec.waitFor((m) => m.payload.toString() === 'caught', { timeout: 3000, label: "'#' catches non-$ topics" })
        // aedes publishes $SYS/<id>/new/clients and /disconnect/clients; a '#' subscriber
        // must never receive those (MQTT-4.7.2-1: '#' does not match $ topics).
        const late = await newClient({ clientId: uid('wild-all-late'), clean: true })
        clients.delete(late.client)
        await endMqtt(late.client)
        await sleep(400)
        const sysMessages = hashSub.rec.messages.filter((m) => m.topic.startsWith('$SYS/'))
        eq(sysMessages.length, 0, "'#' must not match $SYS topics (MQTT-4.7.2-1)")
      } finally {
        await done(another.client)
        await done(hashSub.client)
      }
    })

    await suite.test('overlapping subscriptions deliver 1 or 2 copies, once for a single filter', async () => {
      const sub = await newClient({ clientId: uid('overlap'), clean: true })
      const pub = await newClient({ clientId: uid('overlap-pub'), clean: true })
      try {
        await subscribeAsync(sub.client, 'overlap/#', 1)
        await subscribeAsync(sub.client, 'overlap/x', 1)
        await publishAsync(pub.client, 'overlap/x', 'twice?', { qos: 1 })
        await publishAsync(pub.client, 'overlap/y', 'once', { qos: 1 })
        await sub.rec.waitFor((m) => m.payload.toString() === 'once', { timeout: 3000, label: 'single-filter delivery' })
        await sleep(400)
        const hits = sub.rec.messages.filter((m) => m.payload.toString() === 'twice?')
        assert(hits.length === 1 || hits.length === 2, `overlapping subscriptions delivered ${hits.length} copies (MQTT allows 1 or 2)`)
        const single = sub.rec.messages.filter((m) => m.payload.toString() === 'once')
        eq(single.length, 1, 'a single matching filter delivers exactly one copy')
        return `overlap delivered ${hits.length} cop${hits.length === 1 ? 'y' : 'ies'} (spec allows 1 or 2)`
      } finally {
        await done(pub.client)
        await done(sub.client)
      }
    })

    await suite.test('UTF-8 topics round-trip and zero-length topics are refused', async () => {
      const sub = await newClient({ clientId: uid('utf8-sub'), clean: true })
      const pub = await newClient({ clientId: uid('utf8-pub'), clean: true })
      try {
        const topic = 'utf8/温度/😀'
        await subscribeAsync(sub.client, topic, 1)
        await publishAsync(pub.client, topic, 'ok', { qos: 1 })
        const message = await sub.rec.waitFor((m) => m.topic === topic, { timeout: 3000, label: 'UTF-8 topic delivery' })
        eq(message.payload.toString(), 'ok', 'UTF-8 topic payload')
      } finally {
        await done(pub.client)
        await done(sub.client)
      }
      // Zero-length topic: raw PUBLISH (mqtt.js refuses to generate it) must not be delivered.
      const watcher = await newClient({ clientId: uid('empty-watch'), clean: true })
      const raw = await rawConnect()
      try {
        await subscribeAsync(watcher.client, '#', 1)
        raw.socket.write(buildConnect({ clientId: uid('empty-pub') }))
        await raw.packets.next(3000, 'CONNACK for empty-topic publisher')
        raw.socket.write(buildPublish({ topic: '', payload: 'ghost' }))
        await expectClosed(raw, 3000, 'zero-length topic publish must close the connection')
        await expectNone(watcher.rec, (m) => m.payload.toString() === 'ghost', { ms: 300, label: 'zero-length topic' })
      } finally {
        closeRaw(raw)
        await done(watcher.client)
      }
    })

    await suite.test('publishing to a wildcard topic is refused and not delivered', async () => {
      const watcher = await newClient({ clientId: uid('wild-pub-watch'), clean: true })
      const raw = await rawConnect()
      try {
        await subscribeAsync(watcher.client, '#', 1)
        raw.socket.write(buildConnect({ clientId: uid('wild-publisher') }))
        await raw.packets.next(3000, 'CONNACK for wildcard publisher')
        raw.socket.write(buildPublish({ topic: 'invalid/#', payload: 'wild-pub', qos: 1 }))
        await expectClosed(raw, 3000, 'publish to a wildcard topic must close the connection')
        await expectNone(watcher.rec, (m) => m.payload.toString() === 'wild-pub', { ms: 300, label: 'wildcard publish' })
      } finally {
        closeRaw(raw)
        await done(watcher.client)
      }
    })

    // -----------------------------------------------------------------------
    // 6. QoS
    // -----------------------------------------------------------------------
    await suite.test('QoS 0/1/2 are delivered end-to-end with the published QoS', async () => {
      const sub = await newClient({ clientId: uid('qos-sub'), clean: true })
      const pub = await newClient({ clientId: uid('qos-pub'), clean: true })
      try {
        await subscribeAsync(sub.client, 'qos/levels', 2)
        await publishAsync(pub.client, 'qos/levels', 'q0', { qos: 0 })
        await publishAsync(pub.client, 'qos/levels', 'q1', { qos: 1 })
        await publishAsync(pub.client, 'qos/levels', 'q2', { qos: 2 })
        const message = await sub.rec.waitFor((m) => m.payload.toString() === 'q0', { timeout: 3000, label: 'QoS 0 delivery' })
        eq(message.packet.qos, 0, 'QoS 0 delivery QoS')
        const one = await sub.rec.waitFor((m) => m.payload.toString() === 'q1', { timeout: 3000, label: 'QoS 1 delivery' })
        eq(one.packet.qos, 1, 'QoS 1 delivery QoS')
        const two = await sub.rec.waitFor((m) => m.payload.toString() === 'q2', { timeout: 3000, label: 'QoS 2 delivery' })
        eq(two.packet.qos, 2, 'QoS 2 delivery QoS')
      } finally {
        await done(pub.client)
        await done(sub.client)
      }
    })

    await suite.test('SUBACK grants the requested QoS', async () => {
      const sub = await newClient({ clientId: uid('suback'), clean: true })
      try {
        const zero = await subscribeAsync(sub.client, 'grant/zero', 0)
        eq(zero[0].qos, 0, 'granted QoS for a QoS 0 request')
        const one = await subscribeAsync(sub.client, 'grant/one', 1)
        eq(one[0].qos, 1, 'granted QoS for a QoS 1 request')
        const two = await subscribeAsync(sub.client, 'grant/two', 2)
        eq(two[0].qos, 2, 'granted QoS for a QoS 2 request')
      } finally {
        await done(sub.client)
      }
    })

    await suite.test('delivery QoS is downgraded to the subscription QoS', async () => {
      const subOne = await newClient({ clientId: uid('downgrade-1'), clean: true })
      const subZero = await newClient({ clientId: uid('downgrade-0'), clean: true })
      const pub = await newClient({ clientId: uid('downgrade-pub'), clean: true })
      try {
        await subscribeAsync(subOne.client, 'qos/downgrade', 1)
        await subscribeAsync(subZero.client, 'qos/downgrade', 0)
        await publishAsync(pub.client, 'qos/downgrade', 'down-1', { qos: 2 })
        await publishAsync(pub.client, 'qos/downgrade', 'down-0', { qos: 2 })
        const one = await subOne.rec.waitFor((m) => m.payload.toString() === 'down-1', { timeout: 3000, label: 'QoS 2->1 downgrade' })
        eq(one.packet.qos, 1, 'delivery to a QoS 1 subscription is QoS 1')
        const zero = await subZero.rec.waitFor((m) => m.payload.toString() === 'down-0', { timeout: 3000, label: 'QoS 2->0 downgrade' })
        eq(zero.packet.qos, 0, 'delivery to a QoS 0 subscription is QoS 0')
      } finally {
        await done(pub.client)
        await done(subZero.client)
        await done(subOne.client)
      }
    })

    await suite.test('QoS 2 packet-id handshake (PUBLISH/PUBREC/PUBREL/PUBCOMP) at byte level', async () => {
      const raw = await rawConnect()
      try {
        raw.socket.write(buildConnect({ clientId: uid('raw-qos2') }))
        const connack = await raw.packets.next(3000, 'CONNACK for raw QoS 2 client')
        eq(connack.cmd, 'connack', 'raw CONNACK')
        // Subscribe to the same topic so the broker also delivers the QoS 2 publish back to us.
        raw.socket.write(generatePacket({ cmd: 'subscribe', messageId: 1, subscriptions: [{ topic: 'raw/qos2', qos: 2 }] }, 4))
        const suback = await raw.packets.next(3000, 'SUBACK for raw QoS 2 client')
        eq(suback.cmd, 'suback', 'SUBACK command')
        eq(suback.granted[0], 2, 'SUBACK grants QoS 2')

        const messageId = 7
        raw.socket.write(generatePacket({ cmd: 'publish', topic: 'raw/qos2', payload: Buffer.from('handshake'), qos: 2, messageId }, 4))

        const seen = []
        let pubrec = null
        let pubcomp = null
        let delivered = null
        let deliveredPubrel = false
        for (let i = 0; i < 12 && !(pubrec && pubcomp && delivered && deliveredPubrel); i += 1) {
          const packet = await raw.packets.next(4000, `QoS 2 packet #${i}`)
          seen.push(packet.cmd)
          if (packet.cmd === 'publish') {
            eq(packet.qos, 2, 'delivered QoS 2 publish has QoS 2')
            eq(packet.payload.toString(), 'handshake', 'delivered QoS 2 payload')
            delivered = packet
            raw.socket.write(generatePacket({ cmd: 'pubrec', messageId: packet.messageId }, 4))
          } else if (packet.cmd === 'pubrec') {
            eq(packet.messageId, messageId, 'PUBREC references the published packet id')
            pubrec = packet
            raw.socket.write(generatePacket({ cmd: 'pubrel', messageId }, 4))
          } else if (packet.cmd === 'pubrel') {
            deliveredPubrel = true
            raw.socket.write(generatePacket({ cmd: 'pubcomp', messageId: packet.messageId }, 4))
          } else if (packet.cmd === 'pubcomp') {
            eq(packet.messageId, messageId, 'PUBCOMP references the published packet id')
            pubcomp = packet
          }
        }
        assert(pubrec !== null, `expected PUBREC from the broker, saw ${JSON.stringify(seen)}`)
        assert(pubcomp !== null, `expected PUBCOMP after PUBREL, saw ${JSON.stringify(seen)}`)
        assert(delivered !== null, `expected the QoS 2 publish to be delivered back, saw ${JSON.stringify(seen)}`)
        assert(deliveredPubrel, `expected the broker to complete the delivery handshake (PUBREL), saw ${JSON.stringify(seen)}`)
        return `handshake order: ${JSON.stringify(seen)}`
      } finally {
        closeRaw(raw)
      }
    })

    // -----------------------------------------------------------------------
    // 7. Keepalive
    // -----------------------------------------------------------------------
    await suite.test('a client with keepalive 1 that never pings is disconnected', async () => {
      const raw = await rawConnect()
      try {
        const clientId = uid('keepalive-silent')
        raw.socket.write(buildConnect({ clientId, keepalive: 1 }))
        const connack = await raw.packets.next(3000, 'CONNACK for keepalive client')
        eq(connack.cmd, 'connack', 'CONNACK before keepalive expiry')
        await expectClosed(raw, 3500, 'silent keepalive=1 client')
        eq(broker.view().clients.some((entry) => entry.id === clientId), false, 'expired client removed from the broker view')
      } finally {
        closeRaw(raw)
      }
      await assertHealthy(url, 'keepalive timeout')
    })

    await suite.test('a client that keeps pinging stays connected past the keepalive window', async () => {
      const raw = await rawConnect()
      try {
        raw.socket.write(buildConnect({ clientId: uid('keepalive-ping'), keepalive: 1 }))
        await raw.packets.next(3000, 'CONNACK for pinging client')
        for (let i = 0; i < 5; i += 1) {
          await sleep(400) // 2 s total exceeds keepalive*1.5 = 1.5 s
          raw.socket.write(Buffer.from([0xc0, 0x00]))
          const response = await raw.packets.next(2000, `PINGRESP #${i + 1}`)
          eq(response.cmd, 'pingresp', `PINGRESP #${i + 1}`)
        }
        eq(raw.socket.destroyed, false, 'a pinging keepalive=1 client stays connected')
      } finally {
        closeRaw(raw)
      }
    })

    // -----------------------------------------------------------------------
    // 8. Abuse: malformed / oversized / unsupported input
    // -----------------------------------------------------------------------
    await suite.test('non-MQTT first bytes (HTTP request) are refused, broker survives', async () => {
      const raw = await rawConnect()
      try {
        raw.socket.write(Buffer.from('GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n'))
        await expectClosed(raw, 3000, 'HTTP junk connection')
        eq(raw.bytes().length, 0, 'broker must not answer an HTTP request with bytes')
      } finally {
        closeRaw(raw)
      }
      await assertHealthy(url, 'http junk')
    })

    await suite.test('reserved packet type 0 is refused, broker survives', async () => {
      const raw = await rawConnect()
      try {
        raw.socket.write(Buffer.from([0x00, 0x00]))
        await expectClosed(raw, 3000, 'reserved packet type 0')
        eq(raw.bytes().length, 0, 'no CONNACK for a reserved packet type')
      } finally {
        closeRaw(raw)
      }
      await assertHealthy(url, 'reserved packet type 0')
    })

    await suite.test('a truncated CONNECT is neither answered nor fatal, broker survives', async () => {
      const raw = await rawConnect()
      try {
        const connect = buildConnect({ clientId: uid('truncated') })
        raw.socket.write(connect.subarray(0, Math.min(8, connect.length)))
        await sleep(500)
        eq(raw.socket.destroyed, false, 'the broker should wait for the rest of a truncated CONNECT instead of hanging up')
        eq(raw.bytes().length, 0, 'a truncated CONNECT must not be answered with a CONNACK')
      } finally {
        closeRaw(raw)
      }
      await assertHealthy(url, 'truncated CONNECT')
    })

    await suite.test('a CONNECT declaring more than the 1 MiB cap is destroyed, broker survives', async () => {
      const raw = await rawConnect()
      try {
        // Fixed header + remaining length of 2 MiB, then stop: only the length is needed to reject it.
        raw.socket.write(Buffer.concat([Buffer.from([0x10]), varint(2 * 1024 * 1024)]))
        await expectClosed(raw, 3000, 'oversized CONNECT length')
        eq(raw.bytes().length, 0, 'no CONNACK for an oversized CONNECT')
      } finally {
        closeRaw(raw)
      }
      await assertHealthy(url, 'oversized CONNECT')
    })

    await suite.test('protocol levels 2 and 6 are refused, broker survives', async () => {
      const observed = []
      for (const level of [2, 6]) {
        const raw = await rawConnect()
        try {
          raw.socket.write(buildConnect({ level, protocolName: level === 2 ? 'MQIsdp' : 'MQTT', clientId: uid(`level-${level}`) }))
          await expectClosed(raw, 2500, `protocol level ${level}`)
          observed.push(`${level}:${raw.hex() || 'no bytes'}`)
          eq(raw.socket.destroyed, true, `socket for protocol level ${level} is closed`)
        } finally {
          closeRaw(raw)
        }
        await assertHealthy(url, `protocol level ${level}`)
      }
      return `broker bytes: ${observed.join(' ')}`
    })

    await suite.test('an unsupported protocol level must get CONNACK 0x01 before the close (MQTT-3.1.2-2)', async () => {
      // MQTT-3.1.2-2: "If the protocol level is not supported by the Server, it MUST
      // respond with CONNACK return code 0x01 and then close". Level 2 is unambiguous:
      // a 3.1.1-format CONNACK (20 02 00 01) is the only valid answer.
      const level2 = await rawConnect()
      try {
        level2.socket.write(buildConnect({ level: 2, clientId: uid('strict-level2') }))
        await expectClosed(level2, 2500, 'unsupported level 2 close')
        const bytes = level2.bytes()
        eq(bytes.length, 4, `level 2: expected the 4-byte CONNACK 20 02 00 01, got ${bytes.length} bytes (${level2.hex()})`)
        eq(bytes[0], 0x20, 'level 2: CONNACK first byte')
        eq(bytes[1], 0x02, 'level 2: CONNACK remaining length')
        eq(bytes[2], 0x00, 'level 2: CONNACK acknowledge flags')
        eq(bytes[3], 0x01, 'level 2: CONNACK return code 0x01 (unacceptable protocol version)')
      } finally {
        closeRaw(level2)
      }
      // Level 6 claims a version above MQTT 5, so either a v3 CONNACK 0x01 or a v5
      // CONNACK 0x84 (unsupported protocol version) is defensible — but bytes must be sent.
      const level6 = await rawConnect()
      try {
        level6.socket.write(buildConnect({ level: 6, clientId: uid('strict-level6') }))
        await expectClosed(level6, 2500, 'unsupported level 6 close')
        const hex = level6.hex()
        assert(hex === '20020001' || hex === '2003008400',
          `level 6: expected CONNACK 20 02 00 01 or 20 03 00 84 00 before close, got "${hex}"`)
      } finally {
        closeRaw(level6)
      }
    })

    await suite.test('a ~1 MiB publish payload is delivered intact', async () => {
      const sub = await newClient({ clientId: uid('big-sub'), clean: true })
      const pub = await newClient({ clientId: uid('big-pub'), clean: true })
      try {
        const payload = Buffer.alloc(1024 * 1024, 0x41)
        await subscribeAsync(sub.client, 'big/payload', 1)
        await publishAsync(pub.client, 'big/payload', payload, { qos: 1, timeout: 15000 })
        const message = await sub.rec.waitFor((m) => m.topic === 'big/payload', { timeout: 15000, label: '1 MiB payload delivery' })
        eq(message.payload.length, payload.length, '1 MiB payload length')
        assert(message.payload.equals(payload), '1 MiB payload content is intact')
      } finally {
        await done(pub.client)
        await done(sub.client)
      }
    })

    // -----------------------------------------------------------------------
    // 9. Protocol gating at byte level (separate engine with only 3.1.1 enabled)
    // -----------------------------------------------------------------------
    await suite.test('disabled 5.0 yields a v5 CONNACK with reason code 0x84', async () => {
      const gated = await isolatedEngine({ protocols: ['3.1.1'] })
      try {
        const raw = await rawConnection(gated.port, 5)
        raw.socket.on('error', () => {})
        raw.socket.write(buildConnect({ level: 5, clientId: uid('gated-v5') }))
        const packet = await raw.packets.next(4000, 'v5 CONNACK for disabled 5.0')
        eq(packet.cmd, 'connack', 'CONNACK command for disabled 5.0')
        eq(packet.reasonCode, 0x84, 'v5 CONNACK reason code 0x84 (unsupported protocol version)')
        // Byte-level: 0x20 0x03 0x00 0x84 0x00 (CONNACK, 3 bytes, flags 0, 0x84, no properties).
        eq(raw.hex(), '2003008400', 'v5 rejection bytes are 20 03 00 84 00')
        raw.destroy()
      } finally {
        await gated.dispose()
      }
    })

    await suite.test('disabled 3.1 yields a CONNACK return code 0x01', async () => {
      const gated = await isolatedEngine({ protocols: ['5.0'] })
      try {
        const raw = await rawConnection(gated.port, 3)
        raw.socket.on('error', () => {})
        raw.socket.write(buildConnect({ level: 3, protocolName: 'MQIsdp', clientId: uid('gated-v3') }))
        const packet = await raw.packets.next(4000, 'CONNACK for disabled 3.1')
        eq(packet.cmd, 'connack', 'CONNACK command for disabled 3.1')
        eq(packet.returnCode, 0x01, 'CONNACK return code 0x01 (unacceptable protocol version)')
        eq(raw.hex(), '20020001', 'v3 rejection bytes are 20 02 00 01')
        raw.destroy()
      } finally {
        await gated.dispose()
      }
    })

    await suite.test('with only 3.1.1 enabled a 3.1.1 client still connects', async () => {
      const gated = await isolatedEngine({ protocols: ['3.1.1'] })
      try {
        const client = mqtt.connect(gated.url, { protocolVersion: 4, connectTimeout: 5000, reconnectPeriod: 0 })
        client.on('error', () => {})
        const connack = await new Promise((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error('3.1.1 client failed to connect to a 3.1.1-only broker')), 7000)
          client.once('connect', (packet) => { clearTimeout(timer); resolve(packet) })
          client.once('error', (error) => { clearTimeout(timer); reject(error) })
        })
        eq(connack.cmd, 'connack', '3.1.1 client connects')
        await endMqtt(client)
      } finally {
        await gated.dispose()
      }
    })
  } finally {
    // Always let the process exit cleanly: end every tracked client and socket.
    for (const client of clients) {
      try { await endMqtt(client) } catch { /* already gone */ }
    }
    for (const raw of sockets) raw.destroy()
    await engine.dispose()
  }
})
