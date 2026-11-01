# Publishing dsh-mqtt-tools

Checklist for publishing this bundle to GitHub, npm, and the DSH plugin market.
Nothing here edits a DSH profile by hand — installation always goes through the
harness/plugin manager.

## 0. Before you publish

```powershell
cd <this repo>
npm install                     # dev dependencies too (suites need them)
node --check index.js client.js
npm run test:offline            # protocol / lifecycle / perf / ui suites
npm pack --dry-run              # inspect the exact tarball contents
```

`npm pack --dry-run` must list only `index.js`, `client.js`, `lib/`, `locale/`,
`icon.svg`, `cordis.patch.yml`, `README*.md`, `CHANGELOG.md`, `LICENSE`,
`docs/` and `package.json`. `scripts/`, `.testtools/` and stray logs must not
appear (they are excluded by `files`).

The version lives in `package.json` and the top entry of `CHANGELOG.md`; bump
both before making a release tag.

## 1. GitHub repository and release

Create the repository as `dsh-mqtt-tools` (empty — no README, no license; this
checkout already has both). Then, from the checkout:

```powershell
git init -b main
git add -A
git status --short              # source, docs, scripts, .github, config files only
git commit -m "Release v0.3.11"
git tag -a v0.3.11 -m "dsh-mqtt-tools 0.3.11"
git remote add origin https://github.com/GodLike2020/dsh-mqtt-tools.git
git push -u origin main --tags
```

Fill in `GodLike2020` everywhere before pushing — `package.json`
(`repository` / `homepage` / `bugs`), `docs/release-notes.md` (screenshot URLs)
and, optionally, a CI badge under the README title:

```markdown
![CI](https://github.com/GodLike2020/dsh-mqtt-tools/actions/workflows/ci.yml/badge.svg)
```

The workflow in `.github/workflows/ci.yml` runs on every push and pull request
(Node 20/22/24, `npm ci` → `npm run check` → `npm run test:offline`).

### Release

`docs/release-notes.md` is the GitHub Release body (English, then Chinese).
Replace `GodLike2020` first, then:

```powershell
# with the GitHub CLI
gh release create v0.3.11 --title "dsh-mqtt-tools 0.3.11" --notes-file docs/release-notes.md

# or web UI: Releases → Draft a new release → choose tag v0.3.11 →
# paste the contents of docs/release-notes.md
```

Attach the `npm pack` tarball to the release for offline installs.

### About text and topics

Repository → About (gear icon):

- Description (EN): `MQTT client and embedded broker for DeepSeek Harness — one panel, nine mqtt_* agent tools.`
- Description (ZH): `DeepSeek Harness 的 MQTT 面板：客户端 + 内嵌 Broker + 九个 mqtt_* 智能体工具。`
- Website: the npm page once published.
- Topics: `mqtt`, `mqtt-client`, `mqtt-broker`, `mqtt5`, `iot`,
  `deepseek-harness`, `dsh-plugin`, `agent-tools`.

## 2. Publish to npm

The package is unscoped, so no scope/registry setup is needed beyond being
logged in. The `repository` field should point at the GitHub repo first.

```powershell
npm login
npm publish --access public
npm view dsh-mqtt-tools version dist.tarball
```

Every published version is immutable — bump before republishing.

## 3. Install it into a profile and verify

```powershell
# from the local checkout (development)
#   Settings → Plugins → Install bundle → target: <this folder>
#   or via plugin_manager: install_bundle { target: "<abs path>" }

# from npm
#   install the package into the profile, then enable the "MQTT Tools" bundle
```

After the reload, verify:

- `GET /dsh-mqtt-tools/api/state` → `ok: true`, `tools.registered: 9`,
  `tools.error: ""`, `stateFile` ends with `dsh-mqtt-tools.json`;
- the sidebar shows **MQTT Tools** and the panel opens;
- an agent can call `mqtt_list` and `mqtt_broker` with `action: "status"`.

## 4. Store page / market listing

Prepare the listing text from `locale/en.json` + `locale/zh.json`
(`meta.title`, `meta.description`), the repository URL, and screenshots from
`docs/` (`clients-dark.png`, `clients-light.png`, `broker-dark.png`).

The curated DSH plugin market (`awesome-dsh-plugin.com`, catalog file
`plugins.json`) is maintained in a separate repository; submit the entry by
opening a pull request there with:

- `name`: `dsh-mqtt-tools`
- `description`: the catalog-style one-liner from `locale/en.json`
- `url` / `repository`: the GitHub repository of this package
- `icon`: `icon.svg`
- `category`: `ui` (it ships a panel) — the catalog also carries `usage` and
  `agi` categories; a panel plugin belongs in `ui`
- `keywords`: `mqtt`, `mqtt-client`, `mqtt-broker`, `iot`, `agent-tools`

Note that the market copies package metadata at review time; keep
`package.json` and the catalog entry in sync on every release.

## 5. After publishing

- Keep `CHANGELOG.md` in date lockstep with the tag; the release body comes
  from `docs/release-notes.md`.
- Re-run `node scripts/suite/run-all.mjs` with `DSH_BASE` pointing at a GUI from
  which the npm version is installed, not only the linked checkout.
