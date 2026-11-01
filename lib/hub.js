/** Server-sent-event fan-out for the MQTT panel. */

const HEARTBEAT_MS = 20000
/**
 * A page that stops reading (background tab, throttled renderer) makes Node buffer
 * every written frame in memory. Above this much queued output the stream is
 * dropped and the browser reconnects on its own, which resyncs from a hello frame.
 */
const MAX_BUFFERED_BYTES = 4 * 1024 * 1024

function log(...args) {
  try {
    console.error('[dsh-mqtt-tools]', ...args)
  } catch {
    /* ignore */
  }
}

/** Encode one named event once — `broadcast` must not re-stringify per stream. */
function encodeFrame(type, data) {
  try {
    return `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`
  } catch (error) {
    log(`cannot encode the ${type} event: ${String(error?.message ?? error)}`)
    return null
  }
}

/** Keeps one response stream per open page and broadcasts named events. */
export class Hub {
  constructor() {
    this.clients = new Set()
    this.timer = setInterval(() => this.ping(), HEARTBEAT_MS)
    this.timer.unref?.()
  }

  /** Open one SSE response, send the hello snapshot, and keep it registered. */
  attach(req, res, hello) {
    try {
      res.writeHead(200, {
        'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-cache, no-transform',
        connection: 'keep-alive',
        'x-accel-buffering': 'no',
      })
      res.write('retry: 2000\n\n')
    } catch (error) {
      log(`cannot open the event stream: ${String(error?.message ?? error)}`)
      try {
        res.end()
      } catch {
        /* ignore */
      }
      return
    }
    const client = { res, closed: false }
    this.clients.add(client)
    const cleanup = () => {
      if (client.closed) return
      client.closed = true
      this.clients.delete(client)
      try {
        res.end()
      } catch {
        /* ignore */
      }
    }
    req.on('close', cleanup)
    req.on('error', cleanup)
    res.on('close', cleanup)
    res.on('error', cleanup)
    this.send(client, 'hello', hello)
  }

  /** Drop a stream that can no longer keep up, so its backlog is freed. */
  drop(client, reason) {
    if (client.closed) return
    client.closed = true
    this.clients.delete(client)
    log(`event stream dropped: ${reason}`)
    try {
      client.res.destroy?.() ?? client.res.end()
    } catch {
      try {
        client.res.end()
      } catch {
        /* ignore */
      }
    }
  }

  /** Write one pre-encoded frame, respecting the stream's backpressure budget. */
  writeFrame(client, frame) {
    if (client.closed) return
    try {
      const res = client.res
      if (typeof res.writableLength === 'number' && res.writableLength > MAX_BUFFERED_BYTES) {
        this.drop(client, 'the page stopped reading')
        return
      }
      if (res.write(frame) === false && typeof res.once === 'function') {
        // The frame is buffered; keep draining notifications from piling up.
        if (client.draining !== true) {
          client.draining = true
          res.once('drain', () => {
            client.draining = false
          })
        }
      }
    } catch (error) {
      this.drop(client, String(error?.message ?? error))
    }
  }

  send(client, type, data) {
    if (client.closed) return
    const frame = encodeFrame(type, data)
    if (frame !== null) this.writeFrame(client, frame)
  }

  broadcast(type, data) {
    if (this.clients.size === 0) return
    const frame = encodeFrame(type, data)
    if (frame === null) return
    for (const client of [...this.clients]) this.writeFrame(client, frame)
  }

  ping() {
    for (const client of [...this.clients]) this.writeFrame(client, ': ping\n\n')
  }

  dispose() {
    clearInterval(this.timer)
    for (const client of [...this.clients]) {
      client.closed = true
      try {
        client.res.end()
      } catch {
        /* ignore */
      }
    }
    this.clients.clear()
  }
}
