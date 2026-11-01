/**
 * MQTT 5 support for the embedded broker.
 *
 * aedes only speaks MQTT 3.1 and 3.1.1 (it refuses protocol level 5 with
 * "unacceptable protocol version"), and there is no maintained Node.js broker
 * that speaks 5.0. Rather than tell every MQTT 5 client to downgrade, an MQTT 5
 * connection is translated packet by packet onto a 3.1.1 connection:
 *
 *   client (v5)  鈹€鈹€v5 bytes鈹€鈹€鈻? v5 parser  鈹€鈹€鈻? v3.1.1 packets  鈹€鈹€鈻? aedes
 *   client (v5)  鈼€鈹€鈹€v5 bytes鈹€鈹€  v5 encoder 鈼€鈹€鈹€  v3.1.1 packets  鈼€鈹€鈹€  aedes
 *
 * Everything the protocol needs for ordinary publish/subscribe survives:
 * CONNECT (client id, credentials, will, clean start, keepalive), SUBSCRIBE,
 * UNSUBSCRIBE, PUBLISH at QoS 0/1/2, the QoS handshake, retained messages,
 * persistent sessions, PING and DISCONNECT. MQTT 5-only features are not
 * advertised (no topic aliases, no shared subscriptions, no subscription
 * identifiers, no message expiry), so a v5 client falls back to the 3.1.1
 * semantics it would have used anyway instead of failing to connect.
 * Subscription options (noLocal, retainAsPublished, retainHandling) cannot be
 * expressed on a 3.1.1 connection; they are reported through `onUnsupported`
 * so the user sees a broker warning instead of silently wrong behaviour.
 */
import { Duplex } from 'node:stream'
import mqttPacket from 'mqtt-packet'

/** CONNACK return codes (3.x) to the MQTT 5 reason codes with the same meaning. */
const CONNACK_REASONS = new Map([
  [0, 0x00],
  [1, 0x84],
  [2, 0x85],
  [3, 0x88],
  [4, 0x86],
  [5, 0x87],
  [6, 0x88],
])

function report(onError, error) {
  if (typeof onError === 'function') onError(error)
}

const DEBUG = process.env.DSH_MQTT_DEBUG === '1'

function debug(direction, packet) {
  if (!DEBUG) return
  console.error(`[dsh-mqtt-tools:v5 ${direction}]`, packet?.cmd, packet?.topic ?? '', packet?.retain ? 'retain' : '', packet?.qos ?? 0)
}

/** Turn an MQTT 5 packet from the client into a 3.1.1 packet for aedes. */
function toV311(packet, unsubacks, warnUnsupported) {
  switch (packet.cmd) {
    case 'connect':
      return {
        cmd: 'connect',
        protocolId: 'MQTT',
        protocolVersion: 4,
        clean: packet.clean !== false,
        clientId: typeof packet.clientId === 'string' ? packet.clientId : '',
        keepalive: Number(packet.keepalive ?? 0),
        username: packet.username,
        password: packet.password,
        will: packet.will
          ? {
              topic: packet.will.topic,
              payload: packet.will.payload,
              qos: Number(packet.will.qos ?? 0),
              retain: Boolean(packet.will.retain),
            }
          : undefined,
      }
    case 'publish':
      return {
        cmd: 'publish',
        topic: packet.topic,
        payload: packet.payload,
        qos: Number(packet.qos ?? 0),
        retain: Boolean(packet.retain),
        dup: Boolean(packet.dup),
        messageId: packet.messageId,
      }
    case 'subscribe': {
      const subscriptions = (packet.subscriptions ?? []).map((subscription) => {
        const options = subscription.options ?? {}
        const noLocal = Boolean(options.noLocal ?? subscription.nl)
        const retainAsPublished = Boolean(options.retainAsPublished ?? subscription.rap)
        const retainHandling = Number(options.retainHandling ?? subscription.rh ?? 0)
        if (noLocal || retainAsPublished || retainHandling !== 0) {
          // These options cannot be expressed on a 3.1.1 connection. Tell the
          // user once instead of silently pretending they were honoured.
          warnUnsupported(
            `MQTT 5 subscription options are not supported by this broker: "${subscription.topic}" asked for noLocal=${noLocal ? 'on' : 'off'}, retainAsPublished=${retainAsPublished ? 'on' : 'off'}, retainHandling=${retainHandling}; MQTT 3.1.1 semantics apply`,
          )
        }
        return { topic: subscription.topic, qos: Number(subscription.qos ?? 0) }
      })
      return { cmd: 'subscribe', messageId: packet.messageId, subscriptions }
    }
    case 'unsubscribe': {
      const topics = packet.unsubscriptions ?? []
      if (typeof packet.messageId === 'number') unsubacks.set(packet.messageId, topics.length)
      return { cmd: 'unsubscribe', messageId: packet.messageId, unsubscriptions: topics }
    }
    case 'puback':
    case 'pubrec':
    case 'pubrel':
    case 'pubcomp':
      return { cmd: packet.cmd, messageId: packet.messageId }
    case 'pingreq':
      return { cmd: 'pingreq' }
    case 'disconnect':
      return { cmd: 'disconnect' }
    case 'auth':
      // 3.1.1 has no enhanced authentication: ignore rather than break the stream.
      return null
    default:
      return null
  }
}

/** Turn a 3.1.1 packet from aedes into an MQTT 5 packet for the client. */
function toV5(packet, unsubacks) {
  switch (packet.cmd) {
    case 'connack':
      return {
        cmd: 'connack',
        reasonCode: CONNACK_REASONS.get(Number(packet.returnCode ?? 0)) ?? 0x80,
        sessionPresent: Boolean(packet.sessionPresent),
        properties: {},
      }
    case 'publish':
      return {
        cmd: 'publish',
        topic: packet.topic,
        payload: packet.payload,
        qos: Number(packet.qos ?? 0),
        retain: Boolean(packet.retain),
        dup: Boolean(packet.dup),
        messageId: packet.messageId,
        properties: {},
      }
    case 'puback':
    case 'pubrec':
    case 'pubrel':
    case 'pubcomp':
      return { cmd: packet.cmd, messageId: packet.messageId, reasonCode: 0, properties: {} }
    case 'suback':
      return {
        cmd: 'suback',
        messageId: packet.messageId,
        // In MQTT 5 the granted vector is the reason-code vector: same bytes.
        granted: (packet.granted ?? []).map((qos) => (Number(qos) <= 2 ? Number(qos) : 0x80)),
        properties: {},
      }
    case 'unsuback': {
      const count = unsubacks.get(packet.messageId) ?? 0
      unsubacks.delete(packet.messageId)
      return {
        cmd: 'unsuback',
        messageId: packet.messageId,
        granted: new Array(Math.max(1, count)).fill(0),
        properties: {},
      }
    }
    case 'pingresp':
      return { cmd: 'pingresp' }
    case 'disconnect':
      return { cmd: 'disconnect', reasonCode: 0, properties: {} }
    default:
      return null
  }
}

/**
 * Wrap a TCP socket connected to an MQTT 5 client in a stream aedes can handle.
 *
 * `socket` must not have a data listener yet: the caller hands the bytes it has
 * already read through `ingest()` and the bridge takes over the socket from
 * there. Returns a Duplex that goes to `aedes.handle()`.
 */
export function createMqtt5Bridge(socket, { onError, onUnsupported, remoteAddress } = {}) {
  const fromClient = mqttPacket.parser({ protocolVersion: 5 })
  const fromServer = mqttPacket.parser({ protocolVersion: 4 })
  const unsubacks = new Map()
  const warned = new Set()
  const warnUnsupported = (message) => {
    if (warned.has(message)) return
    warned.add(message)
    if (typeof onUnsupported === 'function') onUnsupported(message)
  }

  const stream = new Duplex({
    read() {
      /* aedes pulls through the stream's readable side; nothing to fetch here */
    },
    write(chunk, encoding, callback) {
      // Bytes aedes wrote (3.1.1) go to the MQTT 5 client.
      try {
        fromServer.parse(chunk)
        callback()
      } catch (error) {
        callback(error)
      }
    },
    final(callback) {
      try {
        socket.end()
      } catch {
        /* the socket may already be gone */
      }
      callback()
    },
    destroy(error, callback) {
      try {
        socket.destroy()
      } catch {
        /* ignore */
      }
      callback(error)
    },
  })
  stream.mqtt5 = true
  stream.remoteAddress = remoteAddress ?? socket.remoteAddress ?? null
  stream.remotePort = socket.remotePort ?? null

  fromClient.on('packet', (packet) => {
    debug('client', packet)
    try {
      const translated = toV311(packet, unsubacks, warnUnsupported)
      if (translated === null) return
      stream.push(mqttPacket.generate(translated, { protocolVersion: 4 }))
    } catch (error) {
      report(onError, error)
      stream.destroy(error instanceof Error ? error : new Error(String(error)))
    }
  })
  fromClient.on('error', (error) => {
    report(onError, error)
    if (!stream.destroyed) stream.destroy(error)
  })

  fromServer.on('packet', (packet) => {
    debug('broker', packet)
    try {
      const translated = toV5(packet, unsubacks)
      if (translated === null) return
      const bytes = mqttPacket.generate(translated, { protocolVersion: 5 })
      if (!socket.destroyed) socket.write(bytes)
    } catch (error) {
      report(onError, error)
      if (!socket.destroyed) socket.destroy()
    }
  })
  fromServer.on('error', (error) => {
    report(onError, error)
    if (!socket.destroyed) socket.destroy()
  })

  const onData = (chunk) => {
    try {
      fromClient.parse(chunk)
    } catch (error) {
      report(onError, error)
      if (!stream.destroyed) stream.destroy(error)
    }
  }

  const onClose = () => {
    socket.removeListener('data', onData)
    if (!stream.destroyed) stream.destroy()
  }

  socket.on('data', onData)
  socket.once('close', onClose)
  socket.once('error', () => {
    if (!stream.destroyed) stream.destroy()
  })
  // The caller hands us a paused socket (bytes already read are passed through
  // ingest()); a paused stream does not start flowing just because a data
  // listener was added, so resume explicitly.
  if (socket.isPaused()) socket.resume()

  return {
    stream,
    /** Feed bytes the caller already read before the bridge existed. */
    ingest(chunk) {
      if (chunk && chunk.length > 0) onData(chunk)
    },
  }
}



