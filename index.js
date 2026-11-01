/**
 * dsh-mqtt-tools: an MQTT client pool plus an embedded broker for the DSH web GUI.
 * The browser half (client.js) talks to this half over /dsh-mqtt-tools/api/*; agent
 * tools (lib/tools.js) drive the same engine so GUI and agents share state.
 */
import { appendFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { Store } from './lib/store.js'
import { RingLog } from './lib/log.js'
import { Hub } from './lib/hub.js'
import { ClientManager } from './lib/clients.js'
import { BrokerManager } from './lib/broker.js'
import { registerRoutes } from './lib/routes.js'
import { createTools } from './lib/tools.js'
import { oneLine } from './lib/log.js'

/** Stable cordis plugin name. */
export const name = 'mqtt'
/** Services required before the panel's API and the agent tools can mount. */
export const inject = ['webServer', 'tools']

const MOUNTED = Symbol.for('dsh-mqtt-tools.mounted')

/**
 * Settles when the last disposed mount finished closing its broker and state
 * file. A new mount can start before that, but its auto-start waits here so two
 * listeners never fight over the same port.
 */
let teardown = Promise.resolve()

function log(...args) {
  try {
    console.error('[dsh-mqtt-tools]', ...args)
  } catch {
    /* ignore */
  }
}

/**
 * Tool registration failures are invisible in the GUI, and a rejected definition
 * would only show up as "the agent has no mqtt tools", so they are written next
 * to the state file where a person can read them afterwards.
 */
function recordToolFailure(file, message) {
  try {
    appendFileSync(join(dirname(file), 'dsh-mqtt-tools.log'), `${new Date().toISOString()} ${message}\n`)
  } catch {
    /* diagnostics must never take the plugin down */
  }
}

/** Mount the connection pool, the broker, and the HTTP/SSE surface. */
export function apply(ctx) {
  if (globalThis[MOUNTED] === true) {
    log('already mounted; skipping the duplicate apply')
    return
  }
  globalThis[MOUNTED] = true

  /** Flipped by the routes disposer so a late load callback cannot restart anything. */
  let disposed = false
  let store
  let messages
  let events
  let hub
  let clients
  let broker
  let stateTimer = null
  /** What the tool registry accepted, so `GET /state` can say whether agents see MQTT. */
  const toolReport = { registered: 0, names: [], error: null }

  const scheduleState = () => {
    if (stateTimer !== null || disposed) return
    stateTimer = setTimeout(() => {
      stateTimer = null
      if (disposed) return
      hub.broadcast('state', snapshot())
    }, 80)
    stateTimer.unref?.()
  }

  try {
    store = new Store()
    messages = new RingLog(600)
    events = new RingLog(200)
    hub = new Hub()
    clients = new ClientManager({ store, messages, onChange: scheduleState })
    broker = new BrokerManager({ store, messages, events, onChange: scheduleState })
  } catch (error) {
    // A half-built engine must not latch the guard: one bad construction would
    // otherwise keep the plugin dead until the whole process restarts.
    globalThis[MOUNTED] = false
    try {
      store?.dispose?.()
    } catch {
      /* nothing to release */
    }
    log(`cannot mount the MQTT engine: ${oneLine(error)}`)
    throw error
  }
  // Warm both lazy imports so the panel can report availability honestly.
  clients.warm()
  broker.warm()
  clients.onMessage = (entry) => hub.broadcast('message', entry)
  broker.onMessage = (entry) => hub.broadcast('message', entry)
  broker.onEvent = (entry) => hub.broadcast('broker-event', entry)

  const snapshot = () => ({
    clients: clients.list(),
    broker: broker.view(),
    settings: { showSystem: store.data.log.showSystem },
    stateFile: store.file,
    library: {
      mqtt: clients.library().mqtt,
      aedes: broker.view().library.aedes,
      error: clients.library().error ?? broker.view().library.error ?? null,
    },
    tools: { ...toolReport, names: [...toolReport.names] },
  })

  const command = async (cmd, input) => {
    switch (cmd) {
      case 'client.save':
        return clients.save(input.profile)
      case 'client.delete':
        await clients.remove(input.id)
        return null
      case 'client.connect':
        await clients.connect(input.id)
        return null
      case 'client.disconnect':
        clients.disconnect(input.id)
        return null
      case 'client.publish':
        return clients.publish(input)
      case 'client.subscribe':
        return clients.subscribe(input)
      case 'client.unsubscribe':
        return clients.unsubscribe(input)
      case 'broker.save':
        return broker.saveSettings(input.broker)
      case 'broker.start':
        await broker.start()
        return null
      case 'broker.stop':
        await broker.stop()
        return null
      case 'broker.publish':
        return broker.publish(input)
      case 'broker.clearRetained':
        return broker.clearRetained()
      case 'settings.save':
        store.update((state) => {
          state.log.showSystem = Boolean(input.showSystem)
        })
        return null
      case 'log.clear': {
        // Each log pane clears its own slice: wiping one connection's messages must
        // not drop another connection's rows or the broker's event history (and the
        // reverse). A command without a scope keeps the old "clear everything" shape
        // so an older page or an agent calling log.clear still works.
        const kind = input?.kind === 'messages' || input?.kind === 'events' ? input.kind : 'both'
        const clientId = typeof input?.clientId === 'string' ? input.clientId : ''
        const scope = input?.scope === 'broker' ? 'broker' : clientId === '' ? 'all' : 'client'
        if (kind !== 'events') {
          if (scope === 'broker') messages.clear((entry) => entry.kind === 'broker')
          else if (scope === 'client') messages.clear((entry) => entry.clientId === clientId)
          else messages.clear()
        }
        if (kind !== 'messages') events.clear()
        hub.broadcast('cleared', scope === 'all' ? { kind } : scope === 'broker' ? { kind, scope } : { kind, clientId })
        return null
      }
      default:
        throw new Error(`unknown command: ${cmd}`)
    }
  }

  const engine = {
    snapshot,
    command,
    hub,
    messages: () => messages.tail(200),
    events: () => events.tail(100),
    hello: () => ({ state: snapshot(), messages: messages.tail(200), events: events.tail(100) }),
  }

  ctx.effect(
    () => {
      const disposers = []
      try {
        disposers.push(...registerRoutes({ ctx, engine }))
      } catch (error) {
        log(`cannot register the API routes: ${oneLine(error)}`)
      }
      return () => {
        disposed = true
        for (const dispose of disposers.reverse()) {
          try {
            dispose()
          } catch (error) {
            log(`cannot release a route: ${oneLine(error)}`)
          }
        }
        hub.dispose()
        clients.dispose()
        // Reset the mount guard before the asynchronous close, so a reload that
        // happens right now can mount again immediately. Auto-start waits for
        // `teardown`, so the old listener has released the port by then.
        globalThis[MOUNTED] = false
        teardown = Promise.allSettled([broker.dispose(), store.dispose()]).then(() => {})
        if (stateTimer !== null) {
          clearTimeout(stateTimer)
          stateTimer = null
        }
        return teardown
      }
    },
    'dsh-mqtt-tools: routes',
  )

  ctx.effect(
    () => {
      const disposers = []
      const names = []
      let failure = null
      let tools = []
      try {
        tools = createTools({ store, clients, broker, messages })
      } catch (error) {
        failure = `cannot build the agent tools: ${oneLine(error)}`
        log(failure)
        recordToolFailure(store.file, failure)
      }
      for (const tool of tools) {
        try {
          disposers.push(ctx.tools.register(tool))
          names.push(tool.name)
        } catch (error) {
          failure = `${tool.name}: ${oneLine(error)}`
          log(`cannot register the agent tool ${failure}`)
          recordToolFailure(store.file, `cannot register the agent tool ${failure}`)
        }
      }
      toolReport.registered = names.length
      toolReport.names = names
      toolReport.error = failure
      return () => {
        for (const dispose of disposers.reverse()) {
          try {
            dispose()
          } catch (error) {
            log(`cannot release an agent tool: ${oneLine(error)}`)
          }
        }
        toolReport.registered = 0
        toolReport.names = []
      }
    },
    'dsh-mqtt-tools: tools',
  )

  store.ready
    .then(async () => {
      if (disposed) return
      await teardown.catch(() => {})
      if (disposed) return
      if (store.data.broker.autoStart) {
        broker.start().catch((error) => log(`cannot start the broker automatically: ${oneLine(error)}`))
      }
      for (const profile of store.data.clients) {
        if (!profile.autoConnect) continue
        clients.connect(profile.id).catch((error) => log(`cannot connect ${profile.name}: ${oneLine(error)}`))
      }
    })
    .catch((error) => log(`cannot load the saved state: ${oneLine(error)}`))
}
