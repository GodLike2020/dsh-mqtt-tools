# Changelog

## 0.3.13 — 2026-11-04

Host compatibility pass: the plugin now loads on DSH builds whose profile resolver is broken.

- On the affected DSH versions the plugin's tools still register, but `library.mqtt` is
  false: loading `mqtt` → `readable-stream@4.x` dies in the host resolver with
  `TypeError: createRequire.resolve.paths is not a function or its return value is not
  iterable`. Range: DSH `0.1.7-rc.2` … `0.2.2-alpha.2`, including the desktop
  `0.2.1-rc.5` build.
- A new `lib/dsh-native-streams.js` synchronously preloads `readable-stream` in native
  Node stream mode (`READABLE_STREAM=disable`) before `mqtt` is imported, so
  `readable-stream/lib/internal/shims/process.js` never issues `require('process/')`.
- `readable-stream@4.8.0` is a pinned direct dependency so that, under DSH's hoisted
  profile layout, the preloaded module is the same instance `mqtt` resolves.
- Transitional workaround only: fixed DSH builds no longer need it; it still runs there
  and is harmless, and it will be removed once the host fix is widely released.
- It covers the `readable-stream` family only; other trailing-slash requests such as
  `punycode/` and `buffer/index` still need the host fix.

## 0.3.12 — 2026-11-01

Packaging and docs pass; no code change.

- The package is on npm as `dsh-mqtt-tools` (https://www.npmjs.com/package/dsh-mqtt-tools).
  The install section now treats `plugin_manager install_bundle` from npm as the
  recommended route and keeps the source checkout as the alternative.
- npm version and weekly-download badges sit next to the CI badge.
- README.md is the Chinese page and README.en.md the English one, each with a
  switch line at the top so the repository lands on Chinese first.
- Both READMEs carry the official “powered by dsh” badge in the same row as
  the CI and npm badges, linking to the harness repository.

## 0.3.11 — 2026-11-01

The broker's send card can be resized again.

- The card at the bottom of the broker pane has the same grip as a client's
  composer: drag it (or focus it and use the arrow keys) to set the message box
  height. The log gives up the room and keeps its floor, so a tall payload never
  squeezes the rows away or pushes the card out of the pane.
- A height picked by hand survives sends and tab switches; the auto-grown height
  still falls back to the starting size after a send.
- UI suite: the publisher grip has its own drag + arrow-key test; the headless
  probe records the resized layout in both sizes.

## 0.3.10 — 2026-11-01

Retained-message handling, QoS validation and panel error text.

- Retained messages now survive a broker restart: the broker saves the retained
  set with the profile and replays it when it starts again, so a subscriber that
  reconnects still gets the last value instead of silence. The set is bounded
  (500 topics) so the state file stays small.
- A running broker's settings drawer shows how many topics retain a value and
  offers one action to clear them all; `mqtt_broker` accepts the same action as
  `clear-retained`, which publishes an empty retained message per topic the way
  MQTT intends.
- QoS is validated instead of clamped: a publish or subscribe asking for 9 (or
  -1) now fails with "qos must be 0, 1 or 2" rather than silently becoming 2 and
  reporting success.
- Host errors are restated in the panel language (connection refused, host not
  found, bad credentials, timeout, unusable protocol version, ...), with the raw
  message kept after the sentence for debugging.
- The message-log tests caught up with the message-row markup, the retain badge
  got an explicit background that passes AA contrast, and the pagination checks
  in scripts/render-check.mjs look at the current row classes.

## 0.3.9 — 2026-11-01

Connectivity and panel behaviour pass, after a usage audit of the 0.3.8 build.

- A connection that failed or dropped can be dialled again. Pressing connect used
  to be a silent no-op for the rest of the session because the dead MQTT client
  was never released; now a retry closes the stale client and opens a fresh one.
  Edited credentials are honoured on the second attempt.
- Saving a new URL, username or protocol option while connected drops the live
  session instead of keeping the old socket. The card no longer claims the new
  address is "connected" while publishes still go to the old broker; press
  connect to use the new settings.
- A damaged state file is set aside as `dsh-mqtt-tools.json.corrupt-<timestamp>`
  and the legacy `dsh-mqtt.json` is adopted, so the next save can no longer
  overwrite a truncated file (and the profiles it held) without a trace.
- Tools: `mqtt_list` / `mqtt_broker status` report the broker client version as a
  readable label instead of `null`; `mqtt_connect { url }` reuses the existing
  profile with that URL instead of piling up copies (and keeps its subscriptions);
  `mqtt_broker { action: "start" }` while running now says which address is still
  listening instead of claiming the new one.
- Panel: after connecting, the settings drawer re-seeds from the saved profile,
  so reopening it shows the URL in use rather than the stale one it would save
  back on the next connect. A message log's clear button empties only that
  connection (or only the broker rows), not every pane at once.
- Panel: half-typed messages, the log filter, direction and pause switch survive
  a tab switch. Enter is no longer swallowed when sending is impossible (offline
  or no broker); the text stays put and keeps its newline. Pausing pins the
  visible window and counts only rows the current filter would reveal. A failed
  "new connection" shows its error instead of doing nothing, the password hint
  follows the row's username after a delete, and a broker failure is labelled
  "error" with the host message as a tooltip instead of "stopped".
- Tests: lifecycle covers reconnect-after-failure, dial-change disconnect and
  damaged-file quarantine; the UI suite covers the drawer re-seed, scoped clears,
  offline Enter and the error badge.

## 0.3.8 — 2026-11-01

Bug hunt pass: session leaks, stale names in the log, and clears that were too wide.

- Pressing connect twice at the same moment used to open two real MQTT sessions for
  one connection. The second one replaced the pool entry, so disconnect closed only
  one of them and the other kept reconnecting in the background, invisible to the
  panel and to the broker client list. connect() now reuses a session that is still
  being dialled, and a session abandoned by disconnect while dialling is closed
  before it is registered. Disconnecting no longer leaves a socket behind.
- Renaming a connected connection now also renames its incoming messages: the
  message handler reads the live profile instead of the copy captured when the
  socket was opened.
- Clearing a log is scoped: clearing a connection's (or the broker's) message log
  keeps the broker event history, and the broker events have their own clear
  button. A host that predates scoped clears still wipes both rings.
- Lifecycle suite covers the concurrent-connect and rename cases; the UI suite
  covers scoped clears.

## 0.3.7 — 2026-11-01

Connect-panel and broker-panel pass; no protocol or API change.

- The client's connect button saves the edits first: change the URL, username or
  keepalive while the settings are folded and press connect — the draft is written
  before dialling out. An empty password field still keeps the saved one.
- The broker pane got the same shape as a connection: a status bar, a foldable
  settings drawer, the message log and a send card at the bottom. A running broker
  opens folded so the log gets the room; stopping it leaves the drawer as it was
  until you press settings.
- Pressing start saves the current broker draft before the server starts, so the
  listener, the protocol versions and the password/user table you just typed are
  the ones in use.
- The user list moved next to the connected clients and now says what it needs: an
  anonymous broker ignores the list entirely, and a saved list is only checked
  after a broker restart.
- The broker publishes through the same composer card as a client: topic line,
  message box that grows with the text, Enter sends and Shift+Enter keeps the
  newline, QoS and retain sit in the footer, and a disabled button explains why.
  Log and drawer share the pane without squeezing either to nothing; narrow panes
  stack the settings in one column and scroll the drawer.
- UI suite gained broker drawer, save-before-start and connect-saves-draft tests;
  docs/broker-dark.png refreshed.

## 0.3.6 — 2026-11-01

Composer layout and keyboard pass; no protocol or API change.

- The publish box is one card read top to bottom: a topic line with its label,
  the message box under it, and a bottom row with QoS/retain on the left and the
  send button on the right. No longer one cramped row where the 180 px topic
  field, the options and the button all competed with the textarea.
- The message box grows with the text up to the room the pane can spare (the log
  keeps at least two rows) and falls back to its starting height when the draft
  is sent or deleted; a manual drag still wins and scrolls inside the box.
- Enter sends. IME composition and Shift+Enter are excluded, so a Chinese input
  method no longer fires a half-typed message and Shift+Enter keeps the newline.
  An empty topic moves the focus to the topic field instead of doing nothing,
  ArrowUp on the first line hops to the topic, ArrowDown returns to the box.
- QoS and retain are remembered per connection, like the topic.
- Opening the connection settings drawer no longer squeezes the log to zero
  height and pushes the send button out of the pane: the drawer scrolls.
- UI suite gained composer keyboard tests (Enter, Shift+Enter, composing Enter,
  focus round-trip, published payload); `docs/clients-*.png` refreshed.

## 0.3.5 — 2026-10-31

Resizable message composer; no protocol or API change.

- The publish box at the bottom of a connection grows a drag grip: drag it up or
  down to choose the height (44–360 px), or focus it and use ↑/↓. The height is
  shared across connections and remembered for the session, so a long JSON
  payload is readable instead of one clipped line. Enter still sends,
  Shift+Enter still inserts a newline.
- UI suite gained a composer test (drag +100 px, ArrowDown −16 px, textarea
  follows the grip); the headless-Edge preview now also reports the composer
  height change.

## 0.3.4 — 2026-10-31

Panel clean-up and burst handling; no protocol or API change.

- Removed the legacy single-connection view the redesign had left behind (it was
  only reachable from the test surface); the clients tab now has exactly one
  detail view.
- The three copies of the "disable controls, run one command, flash the outcome"
  helper collapsed into one shared hook, and unused panel state was removed.
- Message and broker-event rows are memoised and the stream is batched: frames
  are buffered per listener and flushed at most every 60 ms, so a busy broker
  re-renders the log once per batch and only paints newly arrived rows instead
  of re-rendering the whole panel for every message.
- The panel's error boundary is re-created when switching tabs, so a render
  failure in one view no longer follows the user to the other view.
- The UI suite gained burst-batching and structure audits (offline suites: 123
  passed; live API suite: 57 passed).

## 0.3.3 — 2026-10-31

Panel usability and accessibility pass; no protocol or API change.

- The Broker tab now edits the resource limits introduced in 0.3.2 (max
  connections, CONNECT timeout, max payload, max retained messages) instead of
  requiring the API or an agent tool; saving shows the existing restart hint.
- The message log keeps following the newest row once the page is pinned at its
  size limit (it used to stop as soon as the visible count stopped changing),
  and a paused log shows how many newer messages are waiting.
- A page that never receives the first stream frame now says so after 8 seconds
  instead of sitting on "loading MQTT…" forever; a command that never answers
  fails after 20 seconds instead of spinning the button indefinitely.
- Accessibility: the tab strip exposes `tablist`/`tab`/`aria-selected`, the log
  is a labelled `log` region, the resize handle is a focusable separator that can
  be moved with the arrow keys, and the message filter has an accessible name.
- Dragging the list divider no longer sticks when the mouse is released outside
  the window (the drag also ends on window blur).
- Removed unused panel state; the UI suite gained resource-limit and
  accessibility cases (offline suites: 121 passed).

## 0.3.2 — 2026-10-31

Resilience pass after the architecture review; the happy path is unchanged.

- **Broker lifecycle is serialised**: start and stop run through one queue, so a
  quick stop→start or a double click can no longer bind the port twice or leave a
  listener the broker reports as stopped. `dispose()` now waits for the listener
  and every client socket to close, and a new mount waits for the previous
  teardown before auto-starting.
- **Connection guards**: a socket that never sends a CONNECT is closed after
  `connectTimeoutMs` (default 10 s) and the reason is written to the broker log;
  connections beyond `maxConnections` (default 200) are refused and logged.
- **Payload and retained budgets**: `maxPayloadBytes` (default 1 MiB) and
  `maxRetainedMessages` (default 1000) are enforced for client publishes, wills
  and the server-side publish form. An over-budget client is disconnected with a
  readable broker event; the API/tools get a normal error message.
  All four limits are saved in the state file and can be changed through
  `mqtt_broker save` or `POST /api/command {"cmd":"broker.save", ...}`.
- **Domain model split**: defaults and normalisation moved to `lib/model.js`;
  `lib/store.js` keeps the state file and re-exports the model helpers, so the
  persistence layer and the rules it enforces can be understood separately.
- New `resilience` suite (12 cases); the full offline set is 119 passed.

## 0.3.1 — 2026-10-31

Hardening pass after the architecture review; no behaviour change for the happy path.

- **Bounded message previews**: a payload is decoded only up to 64 KiB and clipped
  to 8 KiB of text *before* it enters the ring log. Previously the preview kept the
  whole decoded payload alive (a 64 KiB binary message pinned ~86 KiB per row); the
  measured per-row retention drops to about 1.04× the visible text.
- **SSE backpressure**: each broadcast frame is encoded once for every open page, and
  a page that stops reading is dropped after 4 MiB of queued output instead of
  buffering without bound (the browser reconnects and resyncs from a `hello` frame).
- **Never-reject boundaries**: async connection routing, tool construction, and the
  optional `dispatch` promise are guarded, so no path can become an unhandled
  rejection (fatal on Node 24) from one malformed client or one failed mount.
- **Lifecycle**: the duplicate-mount guard no longer latches when construction
  throws, auto-start/auto-connect is skipped after dispose, and the broker listen
  timer plus the unsubscribe fallback timer are cleared when they should be.
- New `hardening` test suite (13 cases); offline suites now 107 passed.

## 0.3.0 — 2026-10-29

First public release under the name `dsh-mqtt-tools`.

- **Panel**: a "MQTT" entry in the sidebar opens a workspace with two tabs —
  *Clients* (connections, chat-style message log, composer) and *Broker*
  (the embedded server, connected clients, broker event log, publish form).
- **Clients**: saved connection profiles, connect / disconnect, subscribe and
  unsubscribe with QoS, publish with QoS 0–2 and retain, per-connection message
  log, resizable layout, collapsible settings.
- **Embedded broker**: start a local MQTT server from the panel, with host/port,
  anonymous access or a user table, auto-start on launch, protocol versions
  MQTT 3.1 / 3.1.1 / 5.0 (5.0 is translated to 3.1.1 on the wire), retained
  messages, sessions, `$SYS` toggle, connected-client table and event log.
- **Agent tools**: `mqtt_list`, `mqtt_connect`, `mqtt_disconnect`,
  `mqtt_subscribe`, `mqtt_unsubscribe`, `mqtt_publish`, `mqtt_messages`,
  `mqtt_wait`, `mqtt_broker`.
- **API**: `GET /dsh-mqtt-tools/api/state`, `POST /dsh-mqtt-tools/api/command`
  (guarded by the `x-dsh-mqtt-tools` header and same-origin check),
  `GET /dsh-mqtt-tools/api/events` (Server-Sent Events).
- **State**: `$DSH_HOME/dsh-mqtt-tools.json`; the previous file name
  `dsh-mqtt.json` is migrated automatically on first load.
- **Theme**: light and dark; surfaces are derived from the current text colour
  so the panel stays readable with monochrome themes. Native controls (selects,
  checkboxes) follow the harness colour scheme, and dropdown option lists are
  painted explicitly, so the popup is readable in the dark theme too.
- **Logs**: newest page of 100 messages first, with "load older" paging.

### Earlier internal iterations

0.1.0 introduced the client manager, broker manager and panel; 0.2.0 added the
MQTT 5.0 bridge, protocol-version selection, log pagination and the dark-theme
contrast fixes.
