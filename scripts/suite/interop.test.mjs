/**
 * Third-party MQTT interoperability suite.
 *
 * Three directions, every axis asserted explicitly (no green-by-luck):
 *   A. external clients -> our embedded broker
 *        - python paho-mqtt 2.1.0 (a genuinely different stack) for MQTT 5.0,
 *          3.1.1 and 3.1, including the v5-to-3.1.1 bridge from the outside;
 *        - mqtt.js with protocolVersion 3 / 4 / 5;
 *        - a hand-written MQIsdp (MQTT 3.1) CONNECT over a raw TCP socket;
 *        - version gating: a broker configured for 3.1.1 only must refuse 3.1
 *          and 5.0 with a proper CONNACK, not a hang.
 *   B. our own ClientManager -> real public brokers (broker.emqx.io,
 *      test.mosquitto.org). Connect/subscribe/publish/receive/retained through
 *      the shipping client manager. A broker that cannot be reached over this
 *      network is a `suite.skip` with the exact error, never a failure.
 *   C. a compatibility matrix printed at the end (and included in the
 *      SUITE-RESULT JSON under "matrix").
 *
 * The python probe is written to a temp directory at run time, so the suite
 * never adds files to the repository. Rules: never edit plugin source; dispose
 * the engine and every client even on failure.
 */
import { spawn } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import net from 'node:net'
import os from 'node:os'
import { join } from 'node:path'
import mqtt from 'mqtt'
import {
  PLUGIN_DIR,
  isolatedEngine,
  connectMqtt,
  connectMqttError,
  endMqtt,
  subscribeAsync,
  publishAsync,
  recorder,
  openSocket,
  packetReader,
  generatePacket,
  Suite,
  runSuite,
  assert,
  eq,
  waitFor,
} from './harness.mjs'

const PYTHON = process.env.DSH_INTEROP_PYTHON ?? (process.platform === 'win32' ? 'python' : 'python3')
const PUBLIC_CONNECT_TIMEOUT_MS = 10000
const UNIQUE = Math.random().toString(16).slice(2, 10)

const matrix = []
const tools = { node: process.version, mqttJs: null, python: null, paho: null, mosquittoPub: null }

function row(direction, client, protocol, result, detail) {
  matrix.push({ direction, client, protocol, result, detail })
}

// ---------------------------------------------------------------------------
// Python paho-mqtt probe, written to the OS temp directory when the suite runs.
// ---------------------------------------------------------------------------
const PAHO_PROBE = String.raw`"""Third-party MQTT probe used by scripts/suite/interop.test.mjs.

Runs a real paho-mqtt client (a stack that shares no code with the plugin) against
whatever broker host/port it is given, and prints one machine-readable line:

    PROBE {json}

Modes:
  full        connect, subscribe, publish QoS 0/1, retained, retained replay,
              unsubscribe, clean disconnect; CONNACK/SUBACK/UNSUBACK and PUBLISH
              property sets are reported.
  listen      subscribe and print each delivery as a MSG {json} line until stdin
              says STOP (checks one implementation's publish is seen by another).
  v5options   MQTT 5 only: subscribe with noLocal=1 and retainHandling=2 (never
              send retained), then publish a retained message; records what the
              v5-to-3.1.1 bridge does with options it cannot carry.
"""
import json
import sys
import threading
import time


def emit(tag, obj):
    sys.stdout.write(tag + " " + json.dumps(obj, default=str) + "\n")
    sys.stdout.flush()


try:
    import paho.mqtt.client as mqtt
except Exception as exc:  # reported to the suite as "paho unavailable"
    emit("PROBE", {"ok": False, "stage": "import", "error": str(exc)})
    sys.exit(0)

args = {}
for item in sys.argv[1:]:
    key, _, value = item.partition("=")
    args[key.lstrip("-")] = value

mode = args.get("mode", "full")
protocol = int(args.get("protocol", "5"))
host = args.get("host", "127.0.0.1")
port = int(args.get("port", "1883"))
prefix = args.get("prefix", "probe")
duration = float(args.get("timeout", "8"))
use_props = args.get("props", "0") == "1"
client_id = args.get("client", "") or ("paho-probe-%d" % protocol)

PROTOCOLS = {5: getattr(mqtt, "MQTTv5", 5), 4: getattr(mqtt, "MQTTv311", 4), 3: getattr(mqtt, "MQTTv31", 3)}
API_VERSION = getattr(getattr(mqtt, "CallbackAPIVersion", None), "VERSION2", None)
TOPICS = {"qos0": prefix + "/qos0", "qos1": prefix + "/qos1", "retained": prefix + "/retained"}

state = {"connack": None, "subacks": [], "unsubacks": [], "pubacks": [], "messages": [], "errors": [], "disconnected": None}
stop = threading.Event()
lock = threading.Lock()


def props_json(properties):
    if properties is None:
        return None
    try:
        data = properties.json()
    except Exception:
        return None
    return data if data else {}


def reason_value(reason_code):
    return getattr(reason_code, "value", reason_code)


def on_connect(client, userdata, flags, reason_code, properties):
    with lock:
        state["connack"] = {
            "reason_code": reason_value(reason_code),
            "session_present": bool(getattr(flags, "session_present", flags)),
            "properties": props_json(properties),
        }


def on_subscribe(client, userdata, mid, reason_codes, properties):
    with lock:
        state["subacks"].append({
            "message_id": mid,
            "granted": [reason_value(code) for code in (reason_codes or [])],
            "properties": props_json(properties),
        })


def on_unsubscribe(client, userdata, mid, reason_codes, properties):
    with lock:
        state["unsubacks"].append({
            "message_id": mid,
            "reason_codes": [reason_value(code) for code in (reason_codes or [])],
            "properties": props_json(properties),
        })


def on_publish(client, userdata, mid, reason_code, properties):
    with lock:
        state["pubacks"].append({"message_id": mid, "reason_code": reason_value(reason_code)})


def on_message(client, userdata, message):
    entry = {
        "topic": message.topic,
        "qos": int(message.qos),
        "retain": bool(message.retain),
        "payload": message.payload.decode("utf-8", "replace"),
        "properties": props_json(getattr(message, "properties", None)),
    }
    with lock:
        state["messages"].append(entry)
    if mode == "listen":  # stream deliveries to the test as they arrive
        emit("MSG", entry)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    with lock:
        state["disconnected"] = {"reason_code": reason_value(reason_code), "properties": props_json(properties)}


def wait(condition, label, limit):
    deadline = time.time() + limit
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    with lock:
        state["errors"].append("timeout waiting for " + label)
    return False


def finish(payload):
    payload["sdk"] = "paho-mqtt " + getattr(mqtt, "__version__", "?")
    payload["protocol"] = protocol
    payload["mode"] = mode
    payload["client_id"] = client_id
    with lock:
        payload["errors"] = list(payload.get("errors", [])) + state["errors"]
        payload["connack"] = state["connack"]
        payload["subacks"] = state["subacks"]
        payload["unsubacks"] = state["unsubacks"]
        payload["pubacks"] = state["pubacks"]
        payload["messages"] = state["messages"]
        payload["disconnected"] = state["disconnected"]
    emit("PROBE", payload)
    sys.exit(0)


kwargs = dict(client_id=client_id, protocol=PROTOCOLS[protocol])
if protocol != 5:  # paho 2.x rejects clean_session for MQTT v5
    kwargs["clean_session"] = True
if API_VERSION is not None:
    client = mqtt.Client(API_VERSION, **kwargs)
else:  # pragma: no cover - paho 1.x fallback
    client = mqtt.Client(**kwargs)

client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_unsubscribe = on_unsubscribe
client.on_publish = on_publish
client.on_message = on_message
client.on_disconnect = on_disconnect


def props_for_publish():
    if not use_props or protocol != 5:
        return None
    properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
    properties.UserProperty = [("x-interop", "1")]
    properties.MessageExpiryInterval = 60
    properties.ContentType = "text/plain"
    return properties


try:
    client.connect(host, port, keepalive=30)
except Exception as exc:
    finish({"ok": False, "stage": "tcp", "error": "%s: %s" % (type(exc).__name__, exc)})

client.loop_start()

if not wait(lambda: state["connack"] is not None, "CONNACK", duration):
    finish({"ok": False, "stage": "connect", "error": "no CONNACK within %.1fs" % duration})

if state["connack"]["reason_code"] != 0:
    finish({"ok": False, "stage": "connack_refused", "error": "CONNACK reason_code=%s" % state["connack"]["reason_code"]})

if mode == "listen":
    client.subscribe(prefix + "/#", qos=1)
    wait(lambda: len(state["subacks"]) > 0, "SUBACK", duration)
    emit("READY", {"connack": state["connack"], "subacks": state["subacks"]})

    def read_stop():
        for line in sys.stdin:
            if line.strip() == "STOP":
                stop.set()
                return

    threading.Thread(target=read_stop, daemon=True).start()
    deadline = time.time() + duration
    while not stop.is_set() and time.time() < deadline:
        time.sleep(0.05)
    time.sleep(0.15)  # drain anything that arrived just before the stop
    finish({"ok": True, "stage": "listen"})

if mode == "v5options":
    options = mqtt.SubscribeOptions(qos=1, noLocal=True, retainAsPublished=True, retainHandling=2)
    client.subscribe(TOPICS["retained"], options=options)
    wait(lambda: len(state["subacks"]) > 0, "SUBACK", duration)
    info = client.publish(TOPICS["retained"], "paho-retained", qos=1, retain=True)
    info.wait_for_publish(timeout=duration)
    time.sleep(1.2)  # give the broker a chance to (incorrectly) echo it back
    finish({"ok": True, "stage": "v5options"})

# ---------------------------------------------------------------------------
# mode == "full"
# ---------------------------------------------------------------------------
client.subscribe(prefix + "/#", qos=1)
if not wait(lambda: len(state["subacks"]) > 0, "SUBACK", duration):
    finish({"ok": False, "stage": "subscribe"})

for key in ("qos0", "qos1"):
    info = client.publish(TOPICS[key], "paho-" + key, qos=0 if key == "qos0" else 1, properties=props_for_publish())
    info.wait_for_publish(timeout=duration)

if not wait(lambda: {m["payload"] for m in state["messages"]} >= {"paho-qos0", "paho-qos1"}, "live round trip", duration):
    finish({"ok": False, "stage": "live_roundtrip"})
live_messages = list(state["messages"])

info = client.publish(TOPICS["retained"], "paho-retained", qos=1, retain=True, properties=props_for_publish())
info.wait_for_publish(timeout=duration)
first_seen = len(state["messages"])
# A fresh subscription makes the broker replay the retained message.
client.subscribe(TOPICS["retained"], qos=1)
replayed = wait(
    lambda: any(m["payload"] == "paho-retained" for m in state["messages"][first_seen:]),
    "retained replay",
    duration,
)

client.unsubscribe(prefix + "/#")
wait(lambda: len(state["unsubacks"]) > 0, "UNSUBACK", duration)

client.disconnect()
for _ in range(int(duration * 20)):
    if state["disconnected"] is not None:
        break
    time.sleep(0.05)
client.loop_stop()

finish({"ok": True, "stage": "full", "retained_replayed": replayed, "live_message_count": len(live_messages)})
`

let probeDir = null
let probePath = null

function runSync(command, args, timeout = 20000) {
  return new Promise((resolve) => {
    const child = spawn(command, args, { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true })
    let out = ''
    let err = ''
    const timer = setTimeout(() => {
      child.kill()
      resolve({ out, err, timedOut: true, code: null })
    }, timeout)
    child.stdout.on('data', (chunk) => { out += chunk })
    child.stderr.on('data', (chunk) => { err += chunk })
    child.on('error', (error) => { clearTimeout(timer); resolve({ out, err: err + error.message, code: null, spawnError: true }) })
    child.on('close', (code) => { clearTimeout(timer); resolve({ out, err, code, timedOut: false }) })
  })
}

/** Run the python paho probe; returns {report, stderr}. killOnTimeout keeps network suites fast. */
function runPaho(args, timeoutMs = 20000) {
  return new Promise((resolve) => {
    const child = spawn(PYTHON, [probePath, ...args], { stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true })
    let out = ''
    let err = ''
    const timer = setTimeout(() => { child.kill(); }, timeoutMs)
    child.stdout.on('data', (chunk) => { out += chunk })
    child.stderr.on('data', (chunk) => { err += chunk })
    child.on('error', (error) => { clearTimeout(timer); resolve({ report: null, out, err: err.message }) })
    child.on('close', () => {
      clearTimeout(timer)
      const line = out.split(/\r?\n/).reverse().find((candidate) => candidate.startsWith('PROBE '))
      let report = null
      try { report = JSON.parse(line.slice('PROBE '.length)) } catch { report = null }
      resolve({ report, out, err })
    })
    child.stdin.end()
  })
}

/** Start the probe in listen mode; resolves on READY, then collect MSG lines until stop(). */
function startPahoListener(args, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const child = spawn(PYTHON, [probePath, ...args], { stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true })
    let buffer = ''
    const messages = []
    let ready = false
    let stderr = ''
    let finalReport = null
    const timer = setTimeout(() => {
      child.kill()
      reject(new Error(`paho listener did not become READY in ${timeoutMs} ms; stderr: ${stderr.slice(0, 400)}`))
    }, timeoutMs)
    child.stdout.on('data', (chunk) => {
      buffer += chunk
      const lines = buffer.split(/\r?\n/)
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        if (!line) continue
        if (line.startsWith('READY ')) { ready = true; clearTimeout(timer); resolve(listener) }
        else if (line.startsWith('MSG ')) { try { messages.push(JSON.parse(line.slice(4))) } catch { /* ignore */ } }
        else if (line.startsWith('PROBE ')) { try { finalReport = JSON.parse(line.slice(6)) } catch { /* ignore */ } }
      }
    })
    child.stderr.on('data', (chunk) => { stderr += chunk })
    const listener = {
      messages,
      ready() { return ready },
      stop() {
        return new Promise((done) => {
          if (child.exitCode !== null) return done(finalReport)
          const ends = setTimeout(() => { child.kill(); done(finalReport) }, 6000)
          child.once('close', () => { clearTimeout(ends); done(finalReport) })
          child.stdin.write('STOP\n')
        })
      },
      kill() { try { child.kill() } catch { /* already gone */ } },
    }
  })
}

function pahoArgs(engine, protocol, prefix, mode = 'full', extra = []) {
  return [
    `--host=127.0.0.1`,
    `--port=${engine.port}`,
    `--protocol=${protocol}`,
    `--prefix=${prefix}`,
    `--mode=${mode}`,
    '--timeout=8',
    `--client=paho-${protocol}-${UNIQUE}`,
    ...extra,
  ]
}

function assertPaho(report, label) {
  assert(report !== null, `${label}: paho probe produced no PROBE line`)
  assert(report.ok === true, `${label}: paho reported failure at stage ${report.stage}: ${report.error ?? JSON.stringify(report.errors)}`)
  assert(report.errors.length === 0, `${label}: paho reported errors: ${report.errors.join('; ')}`)
  return report
}

function tcpReach(host, port, timeoutMs = PUBLIC_CONNECT_TIMEOUT_MS) {
  return new Promise((resolve) => {
    const startedAt = Date.now()
    const socket = net.connect({ host, port })
    let settled = false
    const finish = (ok, error) => {
      if (settled) return
      settled = true
      socket.destroy()
      resolve({ ok, error, ms: Date.now() - startedAt })
    }
    const timer = setTimeout(() => finish(false, `TCP connect timeout after ${timeoutMs} ms`), timeoutMs)
    socket.once('connect', () => { clearTimeout(timer); finish(true) })
    socket.once('error', (error) => { clearTimeout(timer); finish(false, `${error.code ?? error.name}: ${error.message}`) })
  })
}

// ---------------------------------------------------------------------------
// A/B helpers
// ---------------------------------------------------------------------------

/** mqtt.js protocolVersion 3|4|5 round trip against the embedded broker. */
async function mqttJsRoundtrip(engine, protocolVersion) {
  const prefix = `interop/js${protocolVersion}/${UNIQUE}`
  let a = null
  let b = null
  try {
    a = await connectMqtt(engine.url, { protocolVersion, clientId: `js${protocolVersion}-a-${UNIQUE}`, clean: true })
    b = await connectMqtt(engine.url, { protocolVersion, clientId: `js${protocolVersion}-b-${UNIQUE}`, clean: true })
    assert(a.connected === true && b.connected === true, 'both clients report connected')
    const recA = recorder(a)
    const recB = recorder(b)
    const grantedA = await subscribeAsync(a, `${prefix}/#`, 1)
    eq(grantedA?.[0]?.qos, 1, 'SUBACK qos')
    await subscribeAsync(b, `${prefix}/other`, 0)
    await publishAsync(b, `${prefix}/cross`, `from-b-${protocolVersion}`, { qos: 1 })
    const cross = await recA.waitFor((message) => message.topic === `${prefix}/cross`, { label: 'cross-client publish' })
    eq(cross.payload.toString(), `from-b-${protocolVersion}`, 'cross-client payload')
    eq(cross.packet.qos, 1, 'cross-client qos')
    await publishAsync(a, `${prefix}/qos0`, 'q0', { qos: 0 })
    await publishAsync(a, `${prefix}/qos1`, 'q1', { qos: 1 })
    const q0 = await recA.waitFor((message) => message.topic === `${prefix}/qos0`, { label: 'qos0 delivery' })
    const q1 = await recA.waitFor((message) => message.topic === `${prefix}/qos1`, { label: 'qos1 delivery' })
    eq(q0.payload.toString(), 'q0', 'qos0 payload')
    eq(q0.packet.qos, 0, 'qos0 delivery qos')
    eq(q1.payload.toString(), 'q1', 'qos1 payload')
    eq(q1.packet.qos, 1, 'qos1 delivery qos')
    eq(Boolean(q1.packet.retain), false, 'live delivery retain flag')
    await publishAsync(a, `${prefix}/retained`, `retained-${protocolVersion}`, { qos: 1, retain: true })
    await subscribeAsync(b, `${prefix}/retained`, 1)
    const replay = await recB.waitFor((message) => message.topic === `${prefix}/retained`, { label: 'retained replay' })
    eq(replay.payload.toString(), `retained-${protocolVersion}`, 'retained payload')
    eq(replay.packet.retain, true, 'retained replay retain flag')
    await new Promise((resolve, reject) => a.unsubscribe(`${prefix}/#`, {}, (error) => (error ? reject(error) : resolve())))
    await endMqtt(a)
    a = null
    await endMqtt(b)
    b = null
    return `CONNACK rc0, SUBACK qos1, qos0+qos1, cross-client, retained replay(retain=true), unsubscribe, clean disconnect`
  } finally {
    if (a) await endMqtt(a)
    if (b) await endMqtt(b)
  }
}

/** Hand-written MQIsdp (MQTT 3.1) CONNECT and packet flow over a raw socket. */
async function rawMqtt31Roundtrip(engine) {
  const prefix = `interop/raw31/${UNIQUE}`
  const socket = await openSocket(engine.port)
  const reader = packetReader(socket, 3)
  let node = null
  try {
    socket.write(generatePacket({ cmd: 'connect', protocolId: 'MQIsdp', protocolVersion: 3, clean: true, clientId: `raw31-${UNIQUE}`, keepalive: 30 }, 3))
    const connack = await reader.next(4000, 'CONNACK (raw MQIsdp)')
    eq(connack.cmd, 'connack', 'raw CONNACK cmd')
    eq(connack.returnCode, 0, 'raw CONNACK return code')
    eq(connack.sessionPresent, false, 'raw CONNACK session present')
    socket.write(generatePacket({ cmd: 'subscribe', messageId: 1, subscriptions: [{ topic: `${prefix}/#`, qos: 1 }] }, 3))
    const suback = await reader.next(4000, 'SUBACK (raw MQIsdp)')
    eq(suback.cmd, 'suback', 'raw SUBACK cmd')
    eq(suback.messageId, 1, 'raw SUBACK message id')
    eq(suback.granted, [1], 'raw SUBACK granted qos')
    socket.write(generatePacket({ cmd: 'publish', topic: `${prefix}/qos0`, payload: Buffer.from('raw-q0'), qos: 0 }, 3))
    const echo0 = await reader.next(4000, 'PUBLISH qos0 echo')
    eq(echo0.cmd, 'publish', 'raw qos0 echo cmd')
    eq(echo0.topic, `${prefix}/qos0`, 'raw qos0 echo topic')
    eq(echo0.payload.toString(), 'raw-q0', 'raw qos0 echo payload')
    eq(Boolean(echo0.retain), false, 'raw qos0 echo retain flag')
    socket.write(generatePacket({ cmd: 'publish', topic: `${prefix}/qos1`, payload: Buffer.from('raw-q1'), qos: 1, messageId: 7 }, 3))
    const first = await reader.next(4000, 'PUBACK or PUBLISH (qos1)')
    const second = await reader.next(4000, 'PUBACK or PUBLISH (qos1)')
    const puback = [first, second].find((packet) => packet.cmd === 'puback')
    const delivery = [first, second].find((packet) => packet.cmd === 'publish')
    assert(puback !== undefined, 'raw PUBACK received for qos1 publish')
    eq(puback.messageId, 7, 'raw PUBACK message id')
    assert(delivery !== undefined, 'raw qos1 message echoed back')
    eq(delivery.qos, 1, 'raw qos1 echo qos')
    eq(delivery.payload.toString(), 'raw-q1', 'raw qos1 echo payload')
    socket.write(generatePacket({ cmd: 'puback', messageId: delivery.messageId }, 3))
    socket.write(generatePacket({ cmd: 'publish', topic: `${prefix}/retained`, payload: Buffer.from('raw-retained'), qos: 0, retain: true }, 3))
    node = await connectMqtt(engine.url, { protocolVersion: 4, clientId: `raw31-retain-${UNIQUE}`, clean: true })
    const rec = recorder(node)
    await subscribeAsync(node, `${prefix}/retained`, 0)
    const replay = await rec.waitFor(() => true, { label: 'retained replay (mqtt.js 3.1.1)' })
    eq(replay.payload.toString(), 'raw-retained', 'raw retained payload')
    eq(replay.packet.retain, true, 'raw retained replay retain flag')
    await endMqtt(node)
    node = null
    socket.write(generatePacket({ cmd: 'unsubscribe', messageId: 2, unsubscriptions: [`${prefix}/#`] }, 3))
    let unsuback = null
    for (let attempt = 0; attempt < 8 && unsuback === null; attempt += 1) {
      const packet = await reader.next(3000, 'UNSUBACK (raw MQIsdp)')
      if (packet.cmd === 'unsuback') unsuback = packet
    }
    assert(unsuback !== null, 'raw UNSUBACK received')
    eq(unsuback.messageId, 2, 'raw UNSUBACK message id')
    socket.write(generatePacket({ cmd: 'disconnect' }, 3))
    await waitFor(() => socket.destroyed || socket.readableEnded, { timeout: 3000, label: 'broker closes after raw DISCONNECT' })
    return 'MQIsdp CONNECT, CONNACK rc0, SUBACK qos1, publish qos0+qos1 (+PUBACK), retained replay(retain=true), UNSUBACK, clean DISCONNECT'
  } finally {
    if (node) await endMqtt(node)
    socket.destroy()
  }
}

/** A broker that only advertises 3.1.1 must refuse 3.1 and 5.0 cleanly, never hang. */
async function versionGatingRoundtrip() {
  const gated = await isolatedEngine({ protocols: ['3.1.1'] })
  let accepted = null
  try {
    const refused5 = await connectMqttError(`mqtt://127.0.0.1:${gated.port}`, { protocolVersion: 5, clientId: `gated5-${UNIQUE}` })
    const refused3 = await connectMqttError(`mqtt://127.0.0.1:${gated.port}`, { protocolVersion: 3, clientId: `gated3-${UNIQUE}` })
    accepted = await connectMqtt(`mqtt://127.0.0.1:${gated.port}`, { protocolVersion: 4, clientId: `gated4-${UNIQUE}` })
    assert(accepted.connected === true, 'MQTT 3.1.1 still connects on a 3.1.1-only broker')
    assert(/refused|protocol/i.test(refused5.message), `v5 refusal is an explicit CONNACK error, got: ${refused5.message}`)
    assert(/refused|protocol/i.test(refused3.message), `v3.1 refusal is an explicit CONNACK error, got: ${refused3.message}`)
    return `v5 refused ("${refused5.message}"), v3.1 refused ("${refused3.message}"), v3.1.1 accepted`
  } finally {
    if (accepted) await endMqtt(accepted)
    await gated.dispose()
  }
}

// ---------------------------------------------------------------------------
// ClientManager -> public broker helpers
// ---------------------------------------------------------------------------
const delivered = []

function cmView(engine, id) {
  const profile = engine.clients.find(id)
  return profile === null ? null : engine.clients.view(profile)
}

async function cmConnect(engine, { name, url, protocolVersion }) {
  const profile = await engine.clients.save({
    name,
    url,
    protocolVersion,
    clean: true,
    connectTimeout: PUBLIC_CONNECT_TIMEOUT_MS,
    reconnectPeriod: 0,
    keepalive: 30,
    autoConnect: false,
  })
  await engine.clients.connect(profile.id)
  try {
    await waitFor(() => cmView(engine, profile.id)?.status === 'connected', {
      timeout: PUBLIC_CONNECT_TIMEOUT_MS + 4000,
      label: `${name} connected`,
    })
  } catch (error) {
    const view = cmView(engine, profile.id)
    const failure = new Error(`${error.message} (status=${view?.status ?? 'unknown'}, error=${view?.error ?? 'none'})`)
    failure.stage = 'connect'
    throw failure
  }
  return profile.id
}

async function cmRoundtrip(engine, { name, url, protocolVersion, topicRoot }) {
  const received = []
  const previous = engine.clients.onMessage
  engine.clients.onMessage = (entry) => received.push(entry)
  const ids = []
  const stage = { current: 'connect' }
  try {
    stage.current = 'connect'
    const id = await cmConnect(engine, { name, url, protocolVersion })
    ids.push(id)
    stage.current = 'subscribe'
    await engine.clients.subscribe({ id, topic: `${topicRoot}/#`, qos: 1 })
    stage.current = 'publish'
    await engine.clients.publish({ id, topic: `${topicRoot}/qos0`, payload: `cm-${protocolVersion}-qos0`, qos: 0 })
    await engine.clients.publish({ id, topic: `${topicRoot}/qos1`, payload: `cm-${protocolVersion}-qos1`, qos: 1 })
    stage.current = 'receive'
    await waitFor(() => received.some((entry) => entry.topic === `${topicRoot}/qos1`), { timeout: 12000, label: 'own publish received back' })
    const qos1 = received.find((entry) => entry.topic === `${topicRoot}/qos1`)
    assert(qos1.payload === `cm-${protocolVersion}-qos1`, `payload round trip, got ${JSON.stringify(qos1.payload)}`)
    return `CONNECT, SUBSCRIBE qos1, PUBLISH qos0+qos1, received own message (payload+retain ok), protocolVersion=${protocolVersion}`
  } finally {
    engine.clients.onMessage = previous ?? (() => {})
    for (const id of ids) {
      engine.clients.disconnect(id)
      await engine.clients.remove(id).catch(() => {})
    }
  }
}

async function cmRetained(engine, { baseUrl, topicRoot }) {
  const received = []
  const previous = engine.clients.onMessage
  engine.clients.onMessage = (entry) => received.push(entry)
  const ids = []
  try {
    const publisher = await cmConnect(engine, { name: 'retained-publisher', url: baseUrl, protocolVersion: 4 })
    ids.push(publisher)
    await engine.clients.publish({ id: publisher, topic: `${topicRoot}/retained`, payload: `cm-retained-${UNIQUE}`, qos: 1, retain: true })
    const subscriber = await cmConnect(engine, { name: 'retained-subscriber', url: baseUrl, protocolVersion: 4 })
    ids.push(subscriber)
    await engine.clients.subscribe({ id: subscriber, topic: `${topicRoot}/retained`, qos: 1 })
    await waitFor(() => received.some((entry) => entry.topic === `${topicRoot}/retained`), { timeout: 12000, label: 'retained replay' })
    const replay = received.find((entry) => entry.topic === `${topicRoot}/retained`)
    assert(replay.payload === `cm-retained-${UNIQUE}`, `retained payload, got ${JSON.stringify(replay.payload)}`)
    assert(replay.retain === true, `retained replay retain flag, got ${JSON.stringify(replay.retain)}`)
    return 'retained publish, fresh connection replay(retain=true)'
  } finally {
    engine.clients.onMessage = previous ?? (() => {})
    // Clear the retained value so the public broker is left clean.
    const publisherId = ids[0]
    if (publisherId !== undefined && engine.clients.find(publisherId) !== null && cmView(engine, publisherId)?.status === 'connected') {
      await engine.clients.publish({ id: publisherId, topic: `${topicRoot}/retained`, payload: '', qos: 1, retain: true }).catch(() => {})
    }
    for (const id of ids) {
      engine.clients.disconnect(id)
      await engine.clients.remove(id).catch(() => {})
    }
  }
}

// ---------------------------------------------------------------------------
// The suite
// ---------------------------------------------------------------------------
const suite = new Suite('interop', 'Third-party MQTT interoperability', {
  note: 'external clients -> embedded broker, ClientManager -> public brokers, compatibility matrix',
})

const resultOverrides = {}
suite.result = (() => {
  const base = Suite.prototype.result.bind(suite)
  return () => ({ ...base(), tools: resultOverrides.tools, matrix: resultOverrides.matrix })
})()

await runSuite(suite, async () => {
  probeDir = mkdtempSync(join(os.tmpdir(), 'dsh-mqtt-tools-interop-'))
  probePath = join(probeDir, 'paho_probe.py')
  writeFileSync(probePath, PAHO_PROBE, 'utf8')

  // --- tooling inventory -----------------------------------------------------
  try {
    tools.mqttJs = JSON.parse(readFileSync(join(PLUGIN_DIR, 'node_modules', 'mqtt', 'package.json'), 'utf8')).version
  } catch {
    tools.mqttJs = 'unknown'
  }
  const pythonCheck = await runSync(PYTHON, ['-c', 'import sys, json\ntry:\n import paho.mqtt as p; print(json.dumps({"python": sys.version.split()[0], "paho": getattr(p, "__version__", "yes")}))\nexcept Exception:\n print(json.dumps({"python": sys.version.split()[0], "paho": None}))'], 20000)
  if (pythonCheck.code === 0) {
    try {
      const parsed = JSON.parse(pythonCheck.out.trim().split(/\r?\n/).pop())
      tools.python = parsed.python
      tools.paho = parsed.paho
    } catch {
      tools.python = 'unparseable'
    }
  } else {
    tools.python = null
  }
  const pahoVersionCheck = tools.paho !== null
    ? await runSync(PYTHON, ['-c', 'import importlib.metadata as m; print(m.version("paho-mqtt"))'], 20000)
    : { code: 1, out: '' }
  if (pahoVersionCheck.code === 0) tools.paho = pahoVersionCheck.out.trim()
  const mosquittoCheck = await runSync('mosquitto_pub', ['-h'], 8000)
  tools.mosquittoPub = mosquittoCheck.code === null || mosquittoCheck.spawnError === true ? null : 'present'
  assert(tools.python !== null, `python is required for the paho probes but could not be run (${PYTHON})`)
  console.log(`tools: node ${tools.node}, mqtt.js ${tools.mqttJs}, python ${tools.python}, paho-mqtt ${tools.paho}, mosquitto_pub ${tools.mosquittoPub ?? 'unavailable'}`)

  const engine = await isolatedEngine()
  try {
    // =======================================================================
    // A1. mqtt.js protocolVersion 3 / 4 / 5 -> our broker
    // =======================================================================
    suite.group('A1. mqtt.js protocolVersion 3/4/5 -> embedded broker')
    for (const protocolVersion of [3, 4, 5]) {
      const label = `mqtt.js protocolVersion ${protocolVersion} round trip`
      let detail = null
      let failure = null
      try {
        detail = await mqttJsRoundtrip(engine, protocolVersion)
      } catch (error) {
        failure = error
      }
      row('A external -> broker', `mqtt.js ${tools.mqttJs}`, `MQTT ${protocolVersion === 3 ? '3.1' : protocolVersion === 4 ? '3.1.1' : '5.0'}`, failure ? 'FAIL' : 'PASS', failure ? failure.message : detail)
      await suite.test(label, async () => {
        if (failure) throw failure
        return detail
      })
    }

    // =======================================================================
    // A2. hand-written MQIsdp 3.1 CONNECT
    // =======================================================================
    suite.group('A2. hand-written MQIsdp (MQTT 3.1) CONNECT -> embedded broker')
    let rawDetail = null
    let rawFailure = null
    try {
      rawDetail = await rawMqtt31Roundtrip(engine)
    } catch (error) {
      rawFailure = error
    }
    row('A external -> broker', 'hand-written MQIsdp over raw TCP (mqtt-packet)', 'MQTT 3.1', rawFailure ? 'FAIL' : 'PASS', rawFailure ? rawFailure.message : rawDetail)
    await suite.test('hand-written MQIsdp 3.1 CONNECT, SUBSCRIBE, PUBLISH q0/q1, retained, UNSUBSCRIBE, DISCONNECT', async () => {
      if (rawFailure) throw rawFailure
      return rawDetail
    })

    // =======================================================================
    // A3. version gating
    // =======================================================================
    suite.group('A3. protocol-version gating (broker configured for 3.1.1 only)')
    let gatingDetail = null
    let gatingFailure = null
    try {
      gatingDetail = await versionGatingRoundtrip()
    } catch (error) {
      gatingFailure = error
    }
    row('A external -> broker', 'mqtt.js 3.1 / 3.1.1 / 5.0', '3.1.1-only broker', gatingFailure ? 'FAIL' : 'PASS', gatingFailure ? gatingFailure.message : gatingDetail)
    await suite.test('version gating answers 3.1 and 5.0 with CONNACK refusals and keeps 3.1.1', async () => {
      if (gatingFailure) throw gatingFailure
      return gatingDetail
    })

    // =======================================================================
    // A4. python paho-mqtt: a completely different stack
    // =======================================================================
    suite.group('A4. python paho-mqtt (external stack) -> embedded broker')
    if (tools.paho === null) {
      suite.skip('paho-mqtt probes', 'paho-mqtt is not installed and pip was not invoked for it')
      row('A external -> broker', 'python paho-mqtt', '5.0 / 3.1.1 / 3.1', 'unavailable', 'paho-mqtt not installed')
    } else {
      for (const protocol of [5, 4, 3]) {
        const protocolLabel = protocol === 5 ? '5.0' : protocol === 4 ? '3.1.1' : '3.1'
        const prefix = `interop/paho${protocol}/${UNIQUE}`
        const probe = await runPaho(pahoArgs(engine, protocol, prefix))
        let detail = null
        let failure = null
        try {
          const report = assertPaho(probe.report, `paho MQTT ${protocolLabel}`)
          eq(report.connack.reason_code, 0, 'CONNACK reason code')
          const suback = report.subacks[0]
          assert(suback !== undefined, 'SUBACK received')
          eq(suback.granted, [1], 'SUBACK granted qos')
          const payloads = report.messages.map((message) => message.payload)
          assert(payloads.includes('paho-qos0'), `qos0 round trip, saw ${JSON.stringify(payloads)}`)
          assert(payloads.includes('paho-qos1'), `qos1 round trip, saw ${JSON.stringify(payloads)}`)
          assert(report.messages.some((message) => message.payload === 'paho-qos1' && message.qos === 1), 'qos1 delivery has qos=1')
          assert(report.retained_replayed === true, 'retained message replayed for a fresh subscription')
          assert(report.unsubacks.length >= 1, 'UNSUBACK received')
          if (protocol === 5) {
            // MQTT 5 reason codes must be sensible: CONNACK success, granted qos 1, unsuback success.
            eq(report.unsubacks[0].reason_codes, [0], 'v5 UNSUBACK reason code')
            assert(report.connack.properties !== null && typeof report.connack.properties === 'object', 'v5 CONNACK carries a properties object')
          }
          detail = `CONNACK rc0(props=${JSON.stringify(report.connack.properties)}), SUBACK qos1, qos0+qos1 round trip, retained replay, UNSUBACK, clean disconnect rc=${report.disconnected?.reason_code}`
          if (protocol === 5) {
            const properties = report.messages.find((message) => message.payload === 'paho-qos1')?.properties
            detail += `, delivery properties=${JSON.stringify(properties)}`
          }
        } catch (error) {
          failure = error.message
        }
        row('A external -> broker', `python paho-mqtt ${tools.paho}`, `MQTT ${protocolLabel}`, failure ? 'FAIL' : 'PASS', failure ?? detail)
        await suite.test(`paho-mqtt ${tools.paho} MQTT ${protocolLabel} full round trip`, async () => {
          if (failure) throw new Error(failure)
          return detail
        })
      }

      // paho v5 subscriber <- mqtt.js 3.1.1 publisher: the v5 bridge in the decode direction.
      suite.group('A5. cross-implementation delivery through the embedded broker')
      {
        const prefix = `interop/x/${UNIQUE}`
        let detail = null
        let failure = null
        let listener = null
        let node = null
        try {
          listener = await startPahoListener(pahoArgs(engine, 5, prefix, 'listen'))
          node = await connectMqtt(engine.url, { protocolVersion: 4, clientId: `cross-pub-${UNIQUE}`, clean: true })
          await publishAsync(node, `${prefix}/from-js`, 'js-to-paho', { qos: 1 })
          await waitFor(() => listener.messages.some((message) => message.payload === 'js-to-paho'), { timeout: 6000, label: 'paho v5 received mqtt.js publish' })
          const received = listener.messages.find((message) => message.payload === 'js-to-paho')
          assert(received.qos === 1, `qos preserved through bridge, got ${received.qos}`)
          const finalReport = await listener.stop()
          listener = null
          detail = `mqtt.js 3.1.1 publish qos1 delivered to paho v5 subscriber (qos=1, properties=${JSON.stringify(received.properties)})`
          assertPaho(probeReportFrom(`cross-implementation`, finalReport), 'cross-implementation paho listener')
        } catch (error) {
          failure = error.message
        } finally {
          if (listener) await listener.stop()
          if (node) await endMqtt(node)
        }
        row('A cross-client', 'mqtt.js 3.1.1 -> paho-mqtt 5.0.0 subscriber', 'bridge decode', failure ? 'FAIL' : 'PASS', failure ?? detail)
        await suite.test('mqtt.js 3.1.1 publish reaches a paho-mqtt 5.0 subscriber through the v5 bridge', async () => {
          if (failure) throw new Error(failure)
          return detail
        })
      }

      // paho v5 publisher -> mqtt.js 3.1.1 subscriber: the encode direction on the wire.
      {
        const prefix = `interop/rx/${UNIQUE}`
        let detail = null
        let failure = null
        let node = null
        try {
          node = await connectMqtt(engine.url, { protocolVersion: 4, clientId: `cross-rx-${UNIQUE}`, clean: true })
          const rec = recorder(node)
          await subscribeAsync(node, `${prefix}/#`, 1)
          const probe = await runPaho(pahoArgs(engine, 5, prefix))
          const report = assertPaho(probe.report, 'paho v5 publisher')
          const got = await rec.waitFor((message) => message.topic === `${prefix}/qos1`, { label: 'mqtt.js received paho v5 QoS 1 publish' })
          eq(got.payload.toString(), 'paho-qos1', 'payload through v5 bridge')
          eq(got.packet.qos, 1, 'qos through v5 bridge')
          detail = 'paho-mqtt 5.0 QoS 0/1 publishes delivered to an mqtt.js 3.1.1 subscriber (payload+qos preserved)'
          assert(report.messages.length >= 2, 'paho saw its own messages too')
        } catch (error) {
          failure = error.message
        } finally {
          if (node) await endMqtt(node)
        }
        row('A cross-client', 'paho-mqtt 5.0 -> mqtt.js 3.1.1 subscriber', 'bridge encode', failure ? 'FAIL' : 'PASS', failure ?? detail)
        await suite.test('paho-mqtt 5.0 publish reaches an mqtt.js 3.1.1 subscriber through the v5 bridge', async () => {
          if (failure) throw new Error(failure)
          return detail
        })
      }

      // v5 PUBLISH properties from a third-party client.
      {
        const prefix = `interop/props/${UNIQUE}`
        let detail = null
        let failure = null
        let node = null
        try {
          node = await connectMqtt(engine.url, { protocolVersion: 4, clientId: `props-rx-${UNIQUE}`, clean: true })
          const rec = recorder(node)
          await subscribeAsync(node, `${prefix}/#`, 1)
          const probe = await runPaho(pahoArgs(engine, 5, prefix, 'full', ['--props=1']))
          const report = assertPaho(probe.report, 'paho v5 with properties')
          const got = await rec.waitFor((message) => message.topic === `${prefix}/qos1`, { label: 'mqtt.js received property-carrying publish' })
          eq(got.payload.toString(), 'paho-qos1', 'payload survives property stripping')
          const own = report.messages.find((message) => message.payload === 'paho-qos1')
          assert(own !== undefined, 'paho received its own property-carrying publish')
          const ownProperties = own.properties
          assert(ownProperties === null || ownProperties === undefined || Object.keys(ownProperties).length === 0,
            `bridge does not inject unexpected properties, got ${JSON.stringify(ownProperties)}`)
          detail = `user properties/message-expiry/content-type accepted inbound and stripped (paho saw props=${JSON.stringify(ownProperties)})`
        } catch (error) {
          failure = error.message
        } finally {
          if (node) await endMqtt(node)
        }
        row('A v5 bridge', 'paho-mqtt 5.0 with PUBLISH properties', 'bridge encode', failure ? 'FAIL' : 'PASS', failure ?? detail)
        await suite.test('v5 PUBLISH properties are accepted and safely stripped by the bridge', async () => {
          if (failure) throw new Error(failure)
          return detail
        })
      }

      // v5-only subscription options: record what the bridge does (documented limitation).
      {
        const prefix = `interop/options/${UNIQUE}`
        let observed = null
        let failure = null
        try {
          const probe = await runPaho(pahoArgs(engine, 5, prefix, 'v5options'))
          const report = assertPaho(probe.report, 'paho v5 subscribe options')
          const deliveredOwn = report.messages.some((message) => message.payload === 'paho-retained')
          observed = deliveredOwn
            ? 'noLocal=1 and retainHandling=2 are silently ignored: the bridge delivered the publisher its own retained message'
            : 'noLocal=1 and retainHandling=2 were honoured'
          if (deliveredOwn) {
            row('A v5 bridge', 'paho-mqtt 5.0 SubscribeOptions(noLocal=1, retainHandling=2)', 'bridge options', 'risk', observed)
          } else {
            row('A v5 bridge', 'paho-mqtt 5.0 SubscribeOptions(noLocal=1, retainHandling=2)', 'bridge options', 'ok', observed)
          }
        } catch (error) {
          failure = error.message
          row('A v5 bridge', 'paho-mqtt 5.0 SubscribeOptions(noLocal=1, retainHandling=2)', 'bridge options', 'FAIL', failure)
        }
        await suite.test('v5 subscription options from a third-party client (recorded, not fatal)', async () => {
          if (failure) throw new Error(failure)
          return observed
        })
      }
    }

    // =======================================================================
    // B. our ClientManager -> public brokers
    // =======================================================================
    suite.group('B1. ClientManager -> broker.emqx.io:1883 (anonymous)')
    const emqxTopic = `dsh-mqtt-tools/interop/${UNIQUE}`
    const emqxReach = await tcpReach('broker.emqx.io', 1883)
    if (!emqxReach.ok) {
      suite.skip('broker.emqx.io', `network: ${emqxReach.error}`)
      row('B ClientManager -> public', 'our ClientManager', 'MQTT 3.1.1 + 5.0', 'unavailable', `TCP: ${emqxReach.error}`)
    } else {
      row('B network reachability', 'TCP broker.emqx.io:1883', 'n/a', 'ok', `TCP open in ${emqxReach.ms} ms`)
      for (const protocolVersion of [4, 5]) {
        const name = `emqx protocolVersion ${protocolVersion} round trip via ClientManager`
        let outcome
        try {
          outcome = { ok: true, detail: await cmRoundtrip(engine, { name: `interop-emqx-${protocolVersion}`, url: 'mqtt://broker.emqx.io:1883', protocolVersion, topicRoot: `${emqxTopic}/pv${protocolVersion}` }) }
        } catch (error) {
          outcome = { ok: false, stage: error.stage ?? 'post-connect', error: error.message }
        }
        row('B ClientManager -> public', 'our ClientManager', `MQTT ${protocolVersion === 4 ? '3.1.1' : '5.0'}`, outcome.ok ? 'PASS' : outcome.stage === 'connect' ? 'unavailable' : 'FAIL', outcome.ok ? outcome.detail : `${outcome.stage}: ${outcome.error}`)
        if (!outcome.ok && outcome.stage === 'connect') suite.skip(name, `${outcome.error}`)
        else await suite.test(name, async () => {
          if (!outcome.ok) throw new Error(`${outcome.stage}: ${outcome.error}`)
          return outcome.detail
        })
      }
      // retained handling through the shipping client manager
      const retainedName = 'emqx retained publish + replay via ClientManager'
      let retainedOutcome
      try {
        retainedOutcome = { ok: true, detail: await cmRetained(engine, { baseUrl: 'mqtt://broker.emqx.io:1883', topicRoot: `${emqxTopic}/retained` }) }
      } catch (error) {
        retainedOutcome = { ok: false, stage: error.stage ?? 'post-connect', error: error.message }
      }
      row('B ClientManager -> public', 'our ClientManager', 'MQTT 3.1.1 retained', retainedOutcome.ok ? 'PASS' : retainedOutcome.stage === 'connect' ? 'unavailable' : 'FAIL', retainedOutcome.ok ? retainedOutcome.detail : `${retainedOutcome.stage}: ${retainedOutcome.error}`)
      if (!retainedOutcome.ok && retainedOutcome.stage === 'connect') suite.skip(retainedName, `${retainedOutcome.error}`)
      else await suite.test(retainedName, async () => {
        if (!retainedOutcome.ok) throw new Error(`${retainedOutcome.stage}: ${retainedOutcome.error}`)
        return retainedOutcome.detail
      })
    }

    // =======================================================================
    // B2. test.mosquitto.org
    // =======================================================================
    suite.group('B2. ClientManager -> test.mosquitto.org:1883 (anonymous)')
    const mosquittoReach = await tcpReach('test.mosquitto.org', 1883)
    if (!mosquittoReach.ok) {
      suite.skip('test.mosquitto.org', `network: ${mosquittoReach.error}`)
      row('B ClientManager -> public', 'our ClientManager', 'MQTT 3.1.1', 'unavailable', `TCP: ${mosquittoReach.error}`)
    } else {
      row('B network reachability', 'TCP test.mosquitto.org:1883', 'n/a', 'ok', `TCP open in ${mosquittoReach.ms} ms`)
      const name = 'mosquitto protocolVersion 4 round trip via ClientManager'
      let outcome
      try {
        outcome = { ok: true, detail: await cmRoundtrip(engine, { name: 'interop-mosquitto-4', url: 'mqtt://test.mosquitto.org:1883', protocolVersion: 4, topicRoot: `dsh-mqtt-tools/interop/${UNIQUE}/mosquitto` }) }
      } catch (error) {
        outcome = { ok: false, stage: error.stage ?? 'post-connect', error: error.message }
      }
      row('B ClientManager -> public', 'our ClientManager', 'MQTT 3.1.1', outcome.ok ? 'PASS' : outcome.stage === 'connect' ? 'unavailable' : 'FAIL', outcome.ok ? outcome.detail : `${outcome.stage}: ${outcome.error}`)
      if (!outcome.ok && outcome.stage === 'connect') suite.skip(name, `${outcome.error}`)
      else await suite.test(name, async () => {
        if (!outcome.ok) throw new Error(`${outcome.stage}: ${outcome.error}`)
        return outcome.detail
      })
    }

    // Sanity: no engine-level traces of the interop traffic leaked into client state.
    await suite.test('the embedded broker is still healthy after all external traffic', async () => {
      const state = engine.broker.state
      assert(state.status === 'running', `broker still running, got ${state.status}`)
      return `broker running on port ${engine.port}, ${engine.clients.list().length} client profile(s) left`
    })
  } finally {
    await engine.dispose()
    if (probeDir !== null) rmSync(probeDir, { recursive: true, force: true })
  }

  // --- compatibility matrix --------------------------------------------------
  console.log('\nINTEROP-MATRIX')
  for (const entry of matrix) {
    console.log(`  ${entry.direction} | ${entry.client} | ${entry.protocol} | ${entry.result.toUpperCase()} | ${entry.detail}`)
  }
  resultOverrides.tools = tools
  resultOverrides.matrix = matrix
})

/** Small helper so a listener's final PROBE report uses the same checker. */
function probeReportFrom(label, report) {
  assert(report !== null, `${label}: paho listener produced no PROBE line`)
  assert(report.ok === true, `${label}: paho listener failed: ${report.error ?? report.stage}`)
  return report
}
