/** HTTP surface: state snapshot, command dispatch, and the SSE event stream. */
import { oneLine } from './log.js'

const BASE = '/dsh-mqtt-tools/api'
const BODY_LIMIT = 2 * 1024 * 1024

function log(...args) {
  try {
    console.error('[dsh-mqtt-tools]', ...args)
  } catch {
    /* ignore */
  }
}

function json(res, status, body) {
  if (res.headersSent) {
    try {
      res.end()
    } catch {
      /* ignore */
    }
    return
  }
  // A socket can die between the request arriving and the answer being written;
  // a throw here would escape `guard` and reject a promise the web server does not
  // necessarily catch, so answering is best-effort and never fatal.
  try {
    res.writeHead(status, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    })
    res.end(JSON.stringify(body))
  } catch (error) {
    log(`cannot answer the request: ${oneLine(error)}`)
  }
}

/** An error caused by the caller: answered with 4xx, not 500. */
function clientError(message, statusCode) {
  const error = new Error(message)
  error.statusCode = statusCode
  return error
}

/** Read one JSON request body with a hard size bound. */
async function readJson(req) {
  const chunks = []
  let size = 0
  for await (const chunk of req) {
    size += chunk.length
    if (size > BODY_LIMIT) throw clientError('request body is too large', 413)
    chunks.push(chunk)
  }
  if (size === 0) return {}
  let text
  try {
    text = Buffer.concat(chunks).toString('utf8')
  } catch (error) {
    throw clientError(`cannot read the request body: ${oneLine(error)}`, 400)
  }
  try {
    const parsed = JSON.parse(text)
    return parsed === null || typeof parsed !== 'object' ? {} : parsed
  } catch {
    throw clientError('request body is not valid JSON', 400)
  }
}

/** Wrap every handler so a failure becomes a JSON error, never a rejected promise. */
function guard(handler) {
  return async (req, res) => {
    try {
      await handler(req, res)
    } catch (error) {
      const status = Number.isInteger(error?.statusCode) ? error.statusCode : 500
      if (status >= 500) log(`request failed: ${oneLine(error)}`)
      json(res, status, { ok: false, error: oneLine(error, 500) })
    }
  }
}

/**
 * Register the three API routes and return their disposers.
 * `engine` supplies `snapshot()`, `command(cmd, input)`, `hub`, and `hello()`.
 */
export function registerRoutes({ ctx, engine }) {
  const origin = () => {
    try {
      return ctx.webServer.getOrigin()
    } catch {
      return null
    }
  }

  const sameOrigin = (req) => {
    const expected = origin()
    if (expected === null) return true
    const header = req.headers.origin
    return typeof header !== 'string' || header === expected
  }

  const routes = [
    {
      kind: 'exact',
      path: `${BASE}/state`,
      handler: guard(async (req, res) => {
        if (req.method !== 'GET') return json(res, 405, { ok: false, error: 'GET only' })
        if (!sameOrigin(req)) return json(res, 403, { ok: false, error: 'unexpected origin' })
        json(res, 200, { ok: true, state: engine.snapshot(), messages: engine.messages(), events: engine.events() })
      }),
    },
    {
      kind: 'exact',
      path: `${BASE}/command`,
      handler: guard(async (req, res) => {
        if (req.method !== 'POST') return json(res, 405, { ok: false, error: 'POST only' })
        if (req.headers['x-dsh-mqtt-tools'] !== '1') return json(res, 403, { ok: false, error: 'missing x-dsh-mqtt-tools header' })
        if (!sameOrigin(req)) return json(res, 403, { ok: false, error: 'unexpected origin' })
        const body = await readJson(req)
        const { cmd, ...input } = body
        if (typeof cmd !== 'string' || cmd === '') return json(res, 400, { ok: false, error: 'missing command' })
        try {
          const result = await engine.command(cmd, input)
          json(res, 200, { ok: true, result: result ?? null, state: engine.snapshot() })
        } catch (error) {
          json(res, 200, { ok: false, error: oneLine(error, 500), state: engine.snapshot() })
        }
      }),
    },
    {
      kind: 'exact',
      path: `${BASE}/events`,
      handler: guard(async (req, res) => {
        if (req.method !== 'GET') return json(res, 405, { ok: false, error: 'GET only' })
        if (!sameOrigin(req)) return json(res, 403, { ok: false, error: 'unexpected origin' })
        engine.hub.attach(req, res, engine.hello())
      }),
    },
  ]

  return routes.map((route) => ctx.webServer.register(route))
}
