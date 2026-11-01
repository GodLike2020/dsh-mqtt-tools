// Hardening suite: the bounded-memory and backpressure guarantees added after the
// architecture review. Everything here is pure module behaviour — no sockets.
import { Suite, runSuite, loadPluginLib, assert, eq } from './harness.mjs'

const suite = new Suite('hardening', 'Log payload bounds + SSE backpressure')

/** Minimal stand-in for a ServerResponse (only what Hub touches). */
function fakeResponse({ writableLength = 0, failWrite = false } = {}) {
  const res = {
    writableLength,
    failWrite,
    writes: [],
    ended: false,
    destroyed: false,
    writeHead() {},
    write(chunk) {
      if (res.failWrite) throw new Error('broken pipe')
      const text = String(chunk)
      res.writes.push(text)
      res.writableLength += text.length
      return true
    },
    end() {
      res.ended = true
    },
    destroy() {
      res.destroyed = true
      return res
    },
    on() {},
    once() {},
  }
  return res
}

function fakeRequest() {
  return { on() {}, once() {} }
}

await runSuite(suite, async () => {
  const { describePayload } = await loadPluginLib('log')
  const { Hub } = await loadPluginLib('hub')

  suite.group('payload previews are bounded')

  await suite.test('small utf-8 payload is untouched', () => {
    const entry = describePayload('hello mqtt')
    eq(entry.payload, 'hello mqtt', 'payload')
    eq(entry.encoding, 'utf8', 'encoding')
    eq(entry.bytes, 10, 'bytes')
  })

  await suite.test('64 KiB of ascii is clipped to 8 KiB plus an ellipsis', () => {
    const entry = describePayload('a'.repeat(70000))
    eq(entry.encoding, 'utf8', 'encoding')
    eq(entry.bytes, 70000, 'bytes')
    eq(entry.payload.length, 8193, 'payload length')
    assert(entry.payload.endsWith('…'), 'the preview ends with an ellipsis')
    assert(entry.payload.startsWith('a'.repeat(64)), 'the preview keeps the head of the payload')
  })

  await suite.test('multibyte text is clipped by characters, not bytes', () => {
    const entry = describePayload('中'.repeat(10000))
    eq(entry.bytes, 30000, 'bytes')
    eq(entry.payload.length, 8193, 'payload length')
    eq(entry.payload.slice(0, 2), '中中', 'head is valid text')
  })

  await suite.test('binary payload becomes an aligned base64 preview', () => {
    const payload = Buffer.alloc(32 * 1024)
    for (let index = 0; index < payload.length; index += 1) payload[index] = (index * 37 + 11) % 256
    const entry = describePayload(payload)
    eq(entry.encoding, 'base64', 'encoding')
    eq(entry.bytes, 32768, 'bytes')
    eq(entry.payload.length, 8193, 'payload length')
    const body = entry.payload.slice(0, -1)
    eq(body.length % 4, 0, 'base64 body is a whole number of groups')
  })

  await suite.test('a 512 KiB binary payload keeps a small preview and true byte count', () => {
    const payload = Buffer.alloc(512 * 1024)
    for (let index = 0; index < payload.length; index += 1) payload[index] = (index * 91 + 7) % 256
    const entry = describePayload(payload)
    eq(entry.bytes, 512 * 1024, 'bytes')
    eq(entry.payload.length, 8193, 'payload length')
    assert(entry.payload.length < 16 * 1024, 'the ring entry stays far below the payload size')
  })

  await suite.test('a custom character limit is honoured', () => {
    const entry = describePayload('abcdefghijklmnop', 10)
    eq(entry.payload, 'abcdefghij…', 'payload')
  })

  await suite.group('SSE fan-out respects backpressure')

  await suite.test('attach sends retry plus the hello snapshot', () => {
    const hub = new Hub()
    try {
      const res = fakeResponse()
      hub.attach(fakeRequest(), res, { state: { ok: true }, messages: [], events: [] })
      eq(hub.clients.size, 1, 'one client registered')
      eq(res.writes[0], 'retry: 2000\n\n', 'retry hint')
      assert(res.writes[1].startsWith('event: hello\ndata: '), 'hello frame')
      assert(res.writes[1].includes('"ok":true'), 'hello frame carries the snapshot')
    } finally {
      hub.dispose()
    }
  })

  await suite.test('one broadcast is encoded once and written to every client', () => {
    const hub = new Hub()
    try {
      const first = fakeResponse()
      const second = fakeResponse()
      hub.attach(fakeRequest(), first, {})
      hub.attach(fakeRequest(), second, {})
      const before = first.writes.length
      hub.broadcast('message', { seq: 7, topic: 'a/b', direction: 'in' })
      const frame = first.writes[before]
      eq(second.writes[before], frame, 'both streams receive the same frame')
      assert(frame.startsWith('event: message\ndata: '), 'named event')
      assert(frame.includes('"topic":"a/b"'), 'frame carries the entry')
    } finally {
      hub.dispose()
    }
  })

  await suite.test('a client that stopped reading is dropped instead of buffering forever', () => {
    const hub = new Hub()
    try {
      const fast = fakeResponse()
      const slow = fakeResponse()
      hub.attach(fakeRequest(), fast, {})
      hub.attach(fakeRequest(), slow, {})
      slow.writableLength = 8 * 1024 * 1024
      const before = fast.writes.length
      hub.broadcast('message', { seq: 1 })
      eq(hub.clients.size, 1, 'only the slow stream was removed')
      eq(slow.destroyed, true, 'the slow stream is destroyed')
      assert(fast.writes.length === before + 1, 'the fast stream still gets the frame')
    } finally {
      hub.dispose()
    }
  })

  await suite.test('a stream that cannot even open is never registered', () => {
    const hub = new Hub()
    try {
      const broken = fakeResponse({ failWrite: true })
      hub.attach(fakeRequest(), broken, {})
      eq(hub.clients.size, 0, 'no client registered')
      eq(broken.ended, true, 'the failed response is ended')
    } finally {
      hub.dispose()
    }
  })

  await suite.test('a write failure drops that client without throwing', () => {
    const hub = new Hub()
    try {
      const broken = fakeResponse()
      hub.attach(fakeRequest(), broken, {})
      eq(hub.clients.size, 1, 'stream registered')
      broken.failWrite = true
      hub.broadcast('message', { seq: 2 })
      eq(hub.clients.size, 0, 'broken stream removed')
      eq(broken.destroyed, true, 'broken stream destroyed')
    } finally {
      hub.dispose()
    }
  })

  await suite.test('ping keeps streams alive with a comment frame', () => {
    const hub = new Hub()
    try {
      const res = fakeResponse()
      hub.attach(fakeRequest(), res, {})
      const before = res.writes.length
      hub.ping()
      eq(res.writes[before], ': ping\n\n', 'ping frame')
    } finally {
      hub.dispose()
    }
  })

  await suite.test('dispose ends every stream and stops the heartbeat', () => {
    const hub = new Hub()
    const res = fakeResponse()
    hub.attach(fakeRequest(), res, {})
    hub.dispose()
    eq(res.ended, true, 'stream ended')
    eq(hub.clients.size, 0, 'no clients left')
  })
})
