# dsh-mqtt-tools test suites

Everything here exercises the shipping code (`index.js`, `lib/*.js`, `client.js`)
the way a user or an agent would, and every suite is safe to run on a machine
where the plugin is already installed in a DSH profile.

## Setup

```powershell
npm install      # runtime + dev dependencies (react, react-dom, jsdom, esbuild, puppeteer-core)
```

The UI suite loads its dev packages from `node_modules`; a checkout that keeps
them in `.testtools/` instead (the way this repository was developed) still
works — both locations are tried.

## Running

```powershell
cd <path to dsh-mqtt-tools>
node scripts/suite/run-all.mjs            # every *.test.mjs, sequential, one summary
node scripts/suite/run-all.mjs protocol   # only suites whose filename contains "protocol"
node scripts/suite/lifecycle.test.mjs     # one suite on its own
```

Exit code is non-zero when any suite fails. Each suite prints its own log and a
last line `SUITE-RESULT {...}`; the runner parses that line, so a crash (no
result line) is reported as `CRASH` instead of a silent pass.

## Isolation rules

- `harness.mjs#isolatedEngine()` gives every suite its own temporary
  `DSH_HOME`, its own random port, and real `Store` / `RingLog` /
  `ClientManager` / `BrokerManager` instances. Nothing touches
  `C:\Users\wmd\.dsh\dsh-mqtt-tools.json`.
- Suites that must hit the installed plugin (API, tools, UI live render) use the
  live helpers (`apiJson`, `liveState`, `liveCommand`, `snapshotLiveBroker`,
  `restoreLiveBroker`) and must restore the state they changed.
- The live GUI port changes between DSH restarts: set `DSH_BASE` when it does.
  `http://127.0.0.1:6871` is only the current default.

## Suites

| File | Layer | What it protects |
| --- | --- | --- |
| `protocol.test.mjs` | broker | MQTT 3.1 / 3.1.1 / 5.0 conformance, wills, sessions, retained, wildcards, QoS, keepalive, malformed input, version gating |
| `interop.test.mjs` | edges | external clients against our broker, our client against public brokers, MQTT 5 bridge from a third-party perspective |
| `api-tools.test.mjs` | contract | HTTP command/state/SSE contract, CSRF/origin/limits, security leaks, the 9 agent tools end to end |
| `ui.test.mjs` | browser | SSR of every panel state, jsdom interaction (tabs, pagination, re-seed), i18n parity, dark-theme contrast audit |
| `lifecycle.test.mjs` | runtime | hub/SSE leaks, state-file durability, broker port/restart/dispose, client reconnect, route robustness, mount/dispose |
| `perf.test.mjs` | capacity | message throughput, ring-log bound, large payloads, retained replay, connection storms, SSE fan-out, state flush size |

## Writing a suite

```js
import { Suite, runSuite, isolatedEngine } from './harness.mjs';

const suite = new Suite('my-area', 'My area of behaviour');

await runSuite(suite, async () => {
  suite.group('something specific');
  await suite.test('does the thing', async () => {
    const engine = await isolatedEngine();
    try {
      // ... assertions ...
      return 'optional detail printed after the test name';
    } finally {
      await engine.dispose();
    }
  });
});
```

Rules that keep the suites useful:

1. **No green-by-luck.** An assertion must fail when the behaviour is wrong;
   asserting only that "no exception was thrown" is not a test.
2. **Dispose everything.** Clients, broker, store, hubs, jsdom windows and
   temporary directories; the suite must let Node exit on its own.
3. **Skip the environment, not the product.** Missing network, a GUI token, or a
   tool that is not installed is `suite.skip(name, reason)`, never a failure and
   never a silent pass.
4. **Report numbers, assert bounds.** Performance tests print rates and assert
   completeness/limits, so a slower machine shows a number drop instead of a red
   suite.
5. **One finding per failure message.** Include observed vs expected values so
   the log is actionable without a rerun.
