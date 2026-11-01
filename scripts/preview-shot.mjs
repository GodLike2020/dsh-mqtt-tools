// Scratch preview harness (not shipped): builds the real client.js bundle with
// esbuild, serves it behind a tiny same-origin proxy to the running plugin API,
// and screenshots it with headless Edge so the panel's layout, colours and
// interactions can be inspected without the token-protected GUI shell.
// Usage: node scripts/preview-shot.mjs [theme=dark|light]
import { createRequire } from 'node:module'
import { fileURLToPath } from 'node:url'
import { createServer, request as httpRequest } from 'node:http'
import { request as httpsRequest } from 'node:https'
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs'
import path from 'node:path'

const here = path.dirname(fileURLToPath(import.meta.url))
const root = path.join(here, '..')
const tools = path.join(root, '.testtools')
const toolsRequire = createRequire(path.join(tools, 'noop.js'))
const rootRequire = createRequire(path.join(root, 'package.json'))
// Dev packages live in `.testtools/` in this checkout, or in the root
// `devDependencies` after `npm install`; try both.
const devRequire = (id) => {
  for (const load of [toolsRequire, rootRequire]) {
    try {
      return load(id)
    } catch {
      /* try the next location */
    }
  }
  throw new Error(`${id} is not installed (run npm install, or install it in .testtools)`)
}
const esbuild = devRequire('esbuild')
const puppeteer = devRequire('puppeteer-core')

const BASE = process.env.DSH_BASE ?? 'http://127.0.0.1:10570'
const theme = process.argv[2] === 'light' ? 'light' : 'dark'
const port = 8899
const shots = path.join(tools, 'shots')
mkdirSync(shots, { recursive: true })

// ------------------------------------------------------------------ bundle
const clientPath = path.join(root, 'client.js').replace(/\\/g, '/')
const entry = `
import React from 'react'
import { createRoot } from 'react-dom/client'
const registrations = []
const noop = () => {}
const ctx = {
  slots: {
    inject: (name, cb) => { cb(); return noop },
    register: (reg, component) => { registrations.push({ reg, component }); return noop },
    registerFactory: () => noop,
  },
  get: (key) => key === 'theme'
    ? { getTheme: () => ({ preference: '${theme}', active: { id: '${theme}', colorScheme: '${theme}', tokens: {} } }) }
    : { selectPanel: () => {}, getLocale: () => 'zh', subscribe: () => noop },
  on: () => noop,
  emit: () => noop,
  effect: (fn) => { try { const d = fn(); return typeof d === 'function' ? d : noop } catch { return noop } },
  provide: () => noop,
}
window.__ModuleLoader__ = {
  load(spec) {
    const plugin = spec.factory((id) => {
      if (id === 'react') return React
      throw new Error('unsupported require: ' + id)
    })
    window.__dshMqttPlugin = plugin
    plugin.apply(ctx)
  },
}
await import(${JSON.stringify(clientPath)})
const main = registrations.find((item) => item.reg && item.reg.name === 'main')
if (!main) throw new Error('no main registration')
createRoot(document.getElementById('root')).render(React.createElement(main.component))
`
await esbuild.build({
  stdin: { contents: entry, resolveDir: tools, loader: 'js', sourcefile: 'preview-entry.mjs' },
  bundle: true,
  format: 'esm',
  target: ['chrome110'],
  outfile: path.join(tools, 'preview.js'),
  define: { 'process.env.NODE_ENV': '"production"' },
  logLevel: 'error',
})
console.log('bundle ok')

const tokens = {
  dark: `
  --dsw-alias-bg-base:#17181b;--dsw-alias-bg-layer-1:#1f2125;--dsw-alias-bg-layer-2:#26282d;
  --dsw-alias-bg-overlay:#222428;--dsw-alias-border-l1:rgba(255,255,255,.10);--dsw-alias-border-l2:rgba(255,255,255,.18);
  --dsw-alias-brand-primary:#e6e8ea;--dsw-alias-label-primary:#e6e8ea;--dsw-alias-label-secondary:#9aa0a6;
  --dsw-alias-state-error-primary:#f25f5c;--dsw-alias-state-idle-primary:#7b818a;--dsw-alias-state-success-primary:#43b581;
  --dsw-alias-state-warn-primary:#e0a645;--dsw-specific-sidebar-fill:#141518;`,
  light: `
  --dsw-alias-bg-base:#ffffff;--dsw-alias-bg-layer-1:#f5f6f7;--dsw-alias-bg-layer-2:#eceef0;
  --dsw-alias-bg-overlay:#ffffff;--dsw-alias-border-l1:rgba(0,0,0,.10);--dsw-alias-border-l2:rgba(0,0,0,.18);
  --dsw-alias-brand-primary:#111111;--dsw-alias-label-primary:#1d1f22;--dsw-alias-label-secondary:#5f6570;
  --dsw-alias-state-error-primary:#d93025;--dsw-alias-state-idle-primary:#8a9099;--dsw-alias-state-success-primary:#188038;
  --dsw-alias-state-warn-primary:#a06000;--dsw-specific-sidebar-fill:#f2f3f5;`,
}
const html = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>dsh-mqtt-tools preview</title>
<style>:root{${tokens[theme]}}
html,body{margin:0;height:100%;background:var(--dsw-alias-bg-base,#17181b);color:var(--dsw-alias-label-primary,#e6e8ea);font-family:system-ui,'Segoe UI','Microsoft YaHei',sans-serif}
#root{height:100%}</style></head><body><div id="root"></div><script type="module" src="./preview.js"></script></body></html>`
writeFileSync(path.join(tools, 'preview.html'), html, 'utf8')

// ------------------------------------------------------------------ proxy
const server = createServer((req, res) => {
  const url = req.url ?? '/'
  if (url === '/' || url.startsWith('/?') || url === '/preview.js' || url === '/preview.html') {
    const body = url === '/preview.js' ? readFileSync(path.join(tools, 'preview.js')) : readFileSync(path.join(tools, 'preview.html'))
    res.writeHead(200, { 'content-type': url === '/preview.js' ? 'text/javascript; charset=utf-8' : 'text/html; charset=utf-8' })
    res.end(body)
    return
  }
  const target = new URL(url, BASE)
  const upstream = (target.protocol === 'https:' ? httpsRequest : httpRequest)(
    target,
    { method: req.method, headers: { ...req.headers, host: target.host } },
    (upstreamRes) => {
      res.writeHead(upstreamRes.statusCode ?? 502, upstreamRes.headers)
      upstreamRes.pipe(res)
    },
  )
  upstream.on('error', (error) => {
    res.writeHead(502, { 'content-type': 'text/plain' })
    res.end(String(error))
  })
  req.pipe(upstream)
})
await new Promise((resolve, reject) => server.listen(port, '127.0.0.1', resolve).on('error', reject))
console.log(`preview server on http://127.0.0.1:${port}/ (proxy -> ${BASE})`)

// ------------------------------------------------------------------ browser
const edge = 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe'
const browser = await puppeteer.launch({
  executablePath: edge,
  headless: true,
  args: ['--hide-scrollbars', '--disable-gpu', `--user-data-dir=${path.join(tools, 'edge-profile')}`],
  defaultViewport: { width: 1440, height: 900, deviceScaleFactor: 1 },
})
const page = await browser.newPage()
const errors = []
page.on('console', (message) => {
  if (message.type() === 'error') errors.push(message.text())
})
page.on('pageerror', (error) => errors.push(String(error)))
await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: 'domcontentloaded' })
await page.waitForSelector('.mqtt-clients', { timeout: 20000 })
await new Promise((resolve) => setTimeout(resolve, 700))

const tag = theme
await page.screenshot({ path: path.join(shots, `clients-${tag}.png`) })
console.log('shot clients')

// the native <select> popup follows color-scheme; prove the panel publishes the
// right one and that its options are readable in this theme
const schemeFacts = await page.evaluate(() => {
  const root = document.querySelector('.mqtt-root')
  const select = document.querySelector('.mqtt-select')
  const option = select && select.querySelector('option')
  return {
    rootClass: root ? root.className : null,
    rootScheme: root ? getComputedStyle(root).colorScheme : null,
    selectScheme: select ? getComputedStyle(select).colorScheme : null,
    optionBackground: option ? getComputedStyle(option).backgroundColor : null,
    optionColor: option ? getComputedStyle(option).color : null,
    optionLabel: option ? option.textContent : null,
  }
})
console.log('scheme:', JSON.stringify(schemeFacts))

// drag the list/pane divider and prove the width actually changes
const beforeWidth = await page.evaluate(() => Math.round(document.querySelector('.mqtt-list').getBoundingClientRect().width))
const gutterBox = await (await page.$('.mqtt-gutter')).boundingBox()
await page.mouse.move(gutterBox.x + gutterBox.width / 2, gutterBox.y + 160)
await page.mouse.down()
await page.mouse.move(gutterBox.x + gutterBox.width / 2 + 110, gutterBox.y + 160, { steps: 8 })
await page.mouse.up()
await new Promise((resolve) => setTimeout(resolve, 250))
const afterWidth = await page.evaluate(() => Math.round(document.querySelector('.mqtt-list').getBoundingClientRect().width))
console.log(`resize: ${beforeWidth}px -> ${afterWidth}px`)
await page.screenshot({ path: path.join(shots, `clients-resized-${tag}.png`) })

// drag the composer grip and prove the message box grows
const beforeHeight = await page.evaluate(() => Math.round(document.querySelector('.mqtt-composer textarea').getBoundingClientRect().height))
const gripBox = await (await page.$('.mqtt-composer-grip')).boundingBox()
if (gripBox) {
  await page.mouse.move(gripBox.x + gripBox.width / 2, gripBox.y + 2)
  await page.mouse.down()
  await page.mouse.move(gripBox.x + gripBox.width / 2, gripBox.y + 2 - 120, { steps: 8 })
  await page.mouse.up()
  await new Promise((resolve) => setTimeout(resolve, 250))
}
const afterHeight = await page.evaluate(() => Math.round(document.querySelector('.mqtt-composer textarea').getBoundingClientRect().height))
console.log(`composer: ${beforeHeight}px -> ${afterHeight}px`)
await page.screenshot({ path: path.join(shots, `composer-${tag}.png`) })

// collapse the settings drawer to show the message-first layout
const toggled = await page.evaluate(() => {
  const button = [...document.querySelectorAll('.mqtt-clientbar button')].find((item) => /设置/.test(item.textContent || ''))
  if (!button) return false
  button.click()
  return true
})
console.log('settings toggle:', toggled)
await new Promise((resolve) => setTimeout(resolve, 300))
await page.screenshot({ path: path.join(shots, `clients-collapsed-${tag}.png`) })

// switch to the broker tab
await page.evaluate(() => {
  const tab = [...document.querySelectorAll('.mqtt-tab')].find((item) => /服务端/.test(item.textContent || ''))
  if (tab) tab.click()
})
await new Promise((resolve) => setTimeout(resolve, 400))
await page.screenshot({ path: path.join(shots, `broker-${tag}.png`) })
console.log('shot broker')

// tab labels + list width for the report
const facts = await page.evaluate(() => ({
  tabs: [...document.querySelectorAll('.mqtt-tab')].map((item) => item.textContent),
  listWidth: document.querySelector('.mqtt-list')?.getBoundingClientRect().width ?? null,
  root: document.querySelector('.mqtt-root')?.getBoundingClientRect().width ?? null,
  hasGutter: Boolean(document.querySelector('.mqtt-gutter')),
  hasComposer: Boolean(document.querySelector('.mqtt-composer')),
  composerGripRole: document.querySelector('.mqtt-composer-grip')?.getAttribute('role') ?? null,
  composerHeight: document.querySelector('.mqtt-composer textarea')?.getBoundingClientRect().height ?? null,
}))
console.log('facts:', JSON.stringify(facts))
console.log('console errors:', errors.length ? JSON.stringify(errors.slice(0, 6)) : 'none')

await browser.close()
server.closeAllConnections?.()
server.close()
console.log('OK')
process.exit(0)
