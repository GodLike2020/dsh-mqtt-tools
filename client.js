/* global window, document, navigator, EventSource, fetch, setTimeout, clearTimeout, console */
/** dsh-mqtt-tools web half: the MQTT panel (connections + embedded broker). */
window.__ModuleLoader__.load({
  id: 'dsh-mqtt-tools',
  factory(require) {
    const React = require('react')
    const h = React.createElement
    const { useState, useEffect, useMemo, useRef, useCallback } = React

    const API = '/dsh-mqtt-tools/api'
    const PANEL_ORDER = 42
    const MAX_ROWS = 400
    const COMMAND_TIMEOUT_MS = 20000
    const OPEN_TIMEOUT_MS = 8000
    const LOG_FLUSH_MS = 60

    // The shell evaluates this module before it settles the document language,
    // so a language captured once can be wrong (or change later when the user
    // switches locale). Read it lazily and keep the panel subscribed instead.
    const langListeners = new Set()
    let localeLang = ''
    function detectLang() {
      if (localeLang) return localeLang
      try {
        const fromDocument = document.documentElement.lang
        if (fromDocument) return fromDocument
      } catch {
        /* not running in a document */
      }
      try {
        return navigator.language || ''
      } catch {
        return ''
      }
    }
    let LANG = detectLang()
    const isZh = () => /^zh/i.test(LANG)
    function notifyLang() {
      const next = detectLang()
      if (next === LANG) return
      LANG = next
      for (const listener of [...langListeners]) {
        try {
          listener(LANG)
        } catch {
          /* a listener must never break the panel */
        }
      }
    }
    function subscribeLang(listener) {
      langListeners.add(listener)
      return () => {
        langListeners.delete(listener)
      }
    }
    function useLang() {
      const [, setVersion] = useState(0)
      useEffect(() => subscribeLang(() => setVersion((value) => value + 1)), [])
      return LANG
    }
    const t = (en, cn) => (isZh() ? cn : en)

    // Host-side errors are written for tool output: compact and in English. The
    // panel re-states the ones a person hits while clicking around, and keeps the
    // original text after the translated sentence so debugging is still possible.
    const ERROR_RULES = [
      [/^connect first: this connection is not online$/i, () => t('connect this profile first', '请先连接这个连接')],
      [/^a topic is required$/i, () => t('a topic is required', '请填写 topic')],
      [/^a topic filter is required$/i, () => t('a topic filter is required', '请填写订阅主题')],
      [/^a message is required$/i, () => t('a message is required', '请填写消息内容')],
      [/^qos must be 0, 1 or 2$/i, () => t('QoS must be 0, 1 or 2', 'QoS 只能是 0、1 或 2')],
      [/^start the broker first$/i, () => t('start the broker first', '请先启动 broker')],
      [/^broker is not running$/i, () => t('the broker is not running', 'broker 未运行')],
      [/^no such profile/i, () => t('that connection no longer exists', '这个连接已经不存在了')],
      [/^a name is required$/i, () => t('a name is required', '请填写名称')],
      [/^a url is required$/i, () => t('an address is required', '请填写地址')],
    ]
    const TRANSPORT_RULES = [
      [/ECONNREFUSED/i, () => t('connection refused — is the broker listening on that port?', '连接被拒绝——broker 在监听这个端口吗？')],
      [/ENOTFOUND|EAI_AGAIN/i, () => t('host not found — check the address', '找不到主机——检查一下地址')],
      [/ETIMEDOUT|timed out|connack timeout/i, () => t('timed out — the broker did not answer', '等待超时——broker 没有响应')],
      [/not authorized|bad username|bad user name|bad password/i, () => t('the broker rejected the username or password', 'broker 拒绝了用户名或密码')],
      [/anonymous access is off/i, () => t('the broker requires a username and password', 'broker 要求用户名和密码')],
      [/unacceptable protocol version/i, () => t('the broker does not accept this protocol version', 'broker 不接受这个协议版本')],
      [/econnreset/i, () => t('the connection was reset by the broker', '连接被 broker 重置')],
    ]
    function errorText(error) {
      const raw = String((error && error.message) || error || '').trim()
      if (!raw) return ''
      for (const [pattern, render] of ERROR_RULES) if (pattern.test(raw)) return render()
      for (const [pattern, render] of TRANSPORT_RULES) if (pattern.test(raw)) return `${render()} (${raw})`
      return `${t('operation failed', '操作失败')}: ${raw}`
    }

    // A native <select> popup follows `color-scheme`, not our token colours: on
    // the dark theme the popup stayed white while its text inherited the light
    // page colour, so the list was invisible. Track the harness theme instead of
    // guessing from CSS variables, and keep the panel subscribed to changes.
    const schemeListeners = new Set()
    let themeService = null
    let colorScheme = 'light'
    function setColorScheme(next) {
      if (next !== 'dark' && next !== 'light') return
      if (next === colorScheme) return
      colorScheme = next
      for (const listener of [...schemeListeners]) {
        try {
          listener(colorScheme)
        } catch {
          /* a listener must never break the panel */
        }
      }
    }
    function subscribeScheme(listener) {
      schemeListeners.add(listener)
      return () => {
        schemeListeners.delete(listener)
      }
    }
    function useScheme() {
      const [, setVersion] = useState(0)
      useEffect(() => subscribeScheme(() => setVersion((value) => value + 1)), [])
      return colorScheme
    }
    function schemeFromSnapshot(snapshot) {
      const scheme = snapshot && snapshot.active && snapshot.active.colorScheme
      if (scheme === 'dark' || scheme === 'light') setColorScheme(scheme)
    }
    function schemeFromDocument() {
      // Fallback when the theme service is unavailable (or not wired yet):
      // derive the scheme from the surface actually painted behind the panel.
      if (themeService) return
      try {
        const background = getComputedStyle(document.documentElement).backgroundColor
        const parts = /^rgba?\(([^)]+)\)$/.exec(background)
        if (!parts) return
        const [red, green, blue, alpha = 1] = parts[1].split(',').map((value) => Number.parseFloat(value))
        if (!Number.isFinite(red)) return
        if (alpha === 0) return
        setColorScheme(0.299 * red + 0.587 * green + 0.114 * blue < 128 ? 'dark' : 'light')
      } catch {
        /* no document */
      }
    }

    const STATUS_LABEL = () =>
      isZh()
        ? { idle: '未连接', connecting: '连接中', connected: '已连接', reconnecting: '重连中', offline: '离线', disconnected: '已断开', error: '出错', stopped: '已停止', starting: '启动中', running: '运行中', stopping: '停止中' }
        : { idle: 'idle', connecting: 'connecting', connected: 'connected', reconnecting: 'reconnecting', offline: 'offline', disconnected: 'disconnected', error: 'error', stopped: 'stopped', starting: 'starting', running: 'running', stopping: 'stopping' }
    const statusLabel = (status) => STATUS_LABEL()[status] ?? status

    function statusTone(status) {
      if (status === 'connected' || status === 'running') return 'on'
      if (status === 'error') return 'err'
      if (status === 'connecting' || status === 'reconnecting' || status === 'starting' || status === 'stopping') return 'warn'
      return 'off'
    }

    function formatTime(ts) {
      try {
        return new Date(ts).toLocaleTimeString(undefined, { hour12: false })
      } catch {
        return ''
      }
    }

    function formatDuration(startedAt) {
      if (!startedAt) return ''
      const seconds = Math.max(0, Math.round((Date.now() - startedAt) / 1000))
      if (seconds < 60) return `${seconds}s`
      const minutes = Math.floor(seconds / 60)
      if (minutes < 60) return `${minutes}m ${seconds % 60}s`
      return `${Math.floor(minutes / 60)}h ${minutes % 60}m`
    }

    // ---------------------------------------------------------------- styles
    const CSS = `
.mqtt-root{height:100%;width:100%;min-height:0;display:flex;flex-direction:column;background:var(--dsw-alias-bg-base,transparent);color:var(--dsw-alias-label-primary,inherit);font:13px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,"PingFang SC","Microsoft YaHei",sans-serif;overflow:hidden;color-scheme:light}
.mqtt-root.mqtt-scheme-dark{color-scheme:dark}
.mqtt-root.mqtt-scheme-dark .mqtt-input,.mqtt-root.mqtt-scheme-dark .mqtt-select,.mqtt-root.mqtt-scheme-dark .mqtt-textarea{color-scheme:dark}
.mqtt-select option{background:#ffffff;color:#17181b}
.mqtt-root.mqtt-scheme-dark .mqtt-select option{background:#1f2125;color:#e6e8ea}
.mqtt-root.mqtt-scheme-dark .mqtt-btn.primary{color-scheme:light}
.mqtt-head{flex:none;display:flex;align-items:center;gap:10px;padding:12px 16px 10px;border-bottom:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08))}
.mqtt-title{font-size:15px;font-weight:600;letter-spacing:.2px}
.mqtt-back{display:inline-flex;align-items:center;gap:5px;appearance:none;border:1px solid rgba(127,127,127,0.3);border:1px solid color-mix(in srgb,currentColor 22%,transparent);background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 8%,transparent);color:inherit;font:inherit;font-size:12px;line-height:1.4;padding:3px 9px;border-radius:7px;cursor:pointer}
.mqtt-back:hover{background:rgba(127,127,127,0.2);background:color-mix(in srgb,currentColor 14%,transparent)}
.mqtt-sub{color:var(--dsw-alias-label-secondary,#6b6f76);font-size:12px}
.mqtt-grow{flex:1;min-width:0}
.mqtt-live{display:inline-flex;align-items:center;gap:5px;font-size:11px;color:var(--dsw-alias-label-secondary,#6b6f76)}
.mqtt-dot{width:7px;height:7px;border-radius:50%;background:var(--dsw-alias-state-idle-primary,#9aa0a6);flex:none;display:inline-block}
.mqtt-dot.on{background:var(--dsw-alias-state-success-primary,#25a35a)}
.mqtt-dot.warn{background:var(--dsw-alias-state-warn-primary,#e0a020)}
.mqtt-dot.err{background:var(--dsw-alias-state-error-primary,#d64545)}
.mqtt-tabs{flex:none;display:flex;gap:2px;padding:6px 12px 0;border-bottom:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08))}
.mqtt-tab{appearance:none;border:none;background:transparent;color:var(--dsw-alias-label-secondary,#6b6f76);font:inherit;padding:7px 12px;border-radius:7px 7px 0 0;cursor:pointer;border-bottom:2px solid transparent}
.mqtt-tab:hover{background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 9%,transparent)}
.mqtt-tab.active{color:var(--dsw-alias-label-primary,#17181b);border-bottom-color:var(--dsw-alias-brand-primary,#2f6feb);font-weight:600}
.mqtt-body{flex:1;min-height:0;display:flex;overflow:hidden}
.mqtt-list{width:252px;flex:none;border-right:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08));display:flex;flex-direction:column;min-height:0;background:var(--dsw-specific-sidebar-fill,transparent)}
.mqtt-list-head{flex:none;display:flex;align-items:center;gap:6px;padding:10px 10px 6px}
.mqtt-list-title{font-size:11px;text-transform:uppercase;letter-spacing:.6px;color:var(--dsw-alias-label-secondary,#6b6f76);flex:1}
.mqtt-list-rows{flex:1;min-height:0;overflow:auto;padding:0 8px 10px}
.mqtt-item{display:flex;align-items:center;gap:8px;padding:8px 9px;border-radius:8px;cursor:pointer;border:1px solid transparent;margin-bottom:2px}
.mqtt-item:hover{background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 7%,transparent)}
.mqtt-item.active{background:rgba(127,127,127,0.2);background:color-mix(in srgb,currentColor 11%,transparent);border-color:rgba(127,127,127,0.4);border-color:color-mix(in srgb,currentColor 26%,transparent)}
.mqtt-item-text{flex:1;min-width:0}
.mqtt-item-name{font-weight:550;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mqtt-item-url{font-size:11px;color:var(--dsw-alias-label-secondary,#6b6f76);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
.mqtt-pane{flex:1;min-width:0;min-height:0;display:flex;flex-direction:column;overflow:hidden}
.mqtt-scroll{flex:1;min-height:0;overflow:auto;padding:14px 16px 18px}
.mqtt-section{margin-bottom:18px}
.mqtt-section-title{font-size:12px;font-weight:600;color:var(--dsw-alias-label-secondary,#6b6f76);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;display:flex;align-items:center;gap:8px}
.mqtt-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px 14px}
.mqtt-field{display:flex;flex-direction:column;gap:4px;min-width:0}
.mqtt-field.wide{grid-column:1 / -1}
.mqtt-label{font-size:12px;color:var(--dsw-alias-label-secondary,#6b6f76)}
.mqtt-hint{font-size:11px;color:var(--dsw-alias-label-secondary,#8a8f96)}
.mqtt-input,.mqtt-select,.mqtt-textarea{width:100%;box-sizing:border-box;border:1px solid rgba(127,127,127,0.4);border:1px solid color-mix(in srgb,currentColor 24%,transparent);background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 6%,transparent);color:inherit;border-radius:7px;padding:6px 9px;font:inherit;outline:none}
.mqtt-input:focus,.mqtt-select:focus,.mqtt-textarea:focus{border-color:var(--dsw-alias-brand-primary,#2f6feb);box-shadow:0 0 0 2px rgba(127,127,127,0.3);box-shadow:0 0 0 2px color-mix(in srgb,var(--dsw-alias-brand-primary,#2f6feb) 18%,transparent)}
.mqtt-textarea{resize:vertical;min-height:64px;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:12px}
.mqtt-row{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.mqtt-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:10px}
.mqtt-btn{appearance:none;border:1px solid rgba(127,127,127,0.4);border:1px solid color-mix(in srgb,currentColor 24%,transparent);background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 8%,transparent);color:inherit;font:inherit;padding:5px 12px;border-radius:7px;cursor:pointer;line-height:1.5;white-space:nowrap}
.mqtt-btn:hover{background:rgba(127,127,127,0.2);background:color-mix(in srgb,currentColor 14%,transparent);border-color:rgba(127,127,127,0.5);border-color:color-mix(in srgb,currentColor 34%,transparent)}
.mqtt-btn:disabled{opacity:.45;cursor:not-allowed}
.mqtt-btn.primary{background:#2f6feb;border-color:transparent;color:#fff}
.mqtt-btn.primary:hover{background:#2563d6;border-color:transparent}
.mqtt-btn.danger{color:var(--dsw-alias-state-error-primary,#d64545);border-color:rgba(127,127,127,0.6);border-color:color-mix(in srgb,var(--dsw-alias-state-error-primary,#d64545) 40%,transparent)}
.mqtt-btn.ghost{background:transparent;border-color:transparent;padding:3px 7px;color:var(--dsw-alias-label-secondary,#6b6f76)}
.mqtt-btn.ghost:hover{background:rgba(127,127,127,0.2);background:color-mix(in srgb,currentColor 10%,transparent);color:inherit}
.mqtt-check{display:inline-flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;user-select:none}
.mqtt-check input{accent-color:var(--dsw-alias-brand-primary,#2f6feb)}
.mqtt-badge{display:inline-flex;align-items:center;gap:5px;font-size:11px;padding:2px 8px;border-radius:999px;background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 9%,transparent);color:var(--dsw-alias-label-secondary,#6b6f76);white-space:nowrap}
.mqtt-badge.on{color:var(--dsw-alias-state-success-primary,#1f8a4c);background:rgba(127,127,127,0.2);background:color-mix(in srgb,var(--dsw-alias-state-success-primary,#25a35a) 12%,transparent)}
.mqtt-badge.err{color:var(--dsw-alias-state-error-primary,#d64545);background:rgba(127,127,127,0.2);background:color-mix(in srgb,var(--dsw-alias-state-error-primary,#d64545) 12%,transparent)}
.mqtt-badge.warn{color:var(--dsw-alias-state-warn-primary,#9a6b00);background:rgba(127,127,127,0.2);background:color-mix(in srgb,var(--dsw-alias-state-warn-primary,#e0a020) 14%,transparent)}
.mqtt-empty{color:var(--dsw-alias-label-secondary,#8a8f96);font-size:12px;padding:22px 6px;text-align:center}
.mqtt-list-empty{color:var(--dsw-alias-label-secondary,#8a8f96);font-size:12px;padding:14px 8px;text-align:center}
.mqtt-sub-row{display:flex;align-items:center;gap:8px;padding:5px 8px;border:1px solid rgba(127,127,127,0.3);border:1px solid color-mix(in srgb,currentColor 16%,transparent);border-radius:7px;margin-bottom:5px;background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 5%,transparent)}
.mqtt-sub-topic{flex:1;min-width:0;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mqtt-log{flex:none;height:38%;min-height:170px;display:flex;flex-direction:column;border-top:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08));min-width:0}
.mqtt-log-bar{flex:none;display:flex;align-items:center;gap:8px;padding:7px 12px;border-bottom:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.06))}
.mqtt-log-bar .mqtt-input{height:26px;padding:2px 8px;font-size:12px;max-width:230px}
.mqtt-log-bar .mqtt-select{height:26px;padding:2px 6px;font-size:12px;width:auto}
.mqtt-log-count{font-size:11px;color:var(--dsw-alias-label-secondary,#8a8f96);margin-left:auto;white-space:nowrap}
.mqtt-log-rows{flex:1;min-height:0;overflow:auto;padding:4px 8px 8px}
.mqtt-log-row{display:flex;align-items:baseline;gap:8px;padding:3px 6px;border-radius:5px;font-size:12px;line-height:1.5}
.mqtt-log-row:hover{background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 7%,transparent)}
.mqtt-log-time{color:var(--dsw-alias-label-secondary,#8a8f96);font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:11px;flex:none}
.mqtt-log-dir{flex:none;width:14px;text-align:center;font-weight:700}
.mqtt-log-dir.in{color:var(--dsw-alias-state-success-primary,#25a35a)}
.mqtt-log-dir.out{color:var(--dsw-alias-brand-primary,#2f6feb)}
.mqtt-log-topic{flex:none;max-width:34%;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:550}
.mqtt-log-payload{flex:1;min-width:0;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--dsw-alias-label-secondary,#4a4e55)}
.mqtt-log-meta{flex:none;color:var(--dsw-alias-label-secondary,#8a8f96);font-size:11px}
.mqtt-split{display:grid;grid-template-columns:minmax(0,1.15fr) minmax(0,1fr);gap:16px}
.mqtt-card{border:1px solid rgba(127,127,127,0.3);border:1px solid color-mix(in srgb,currentColor 16%,transparent);border-radius:10px;padding:12px 14px;background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 5%,transparent);margin-bottom:14px}
.mqtt-kv{display:grid;grid-template-columns:auto 1fr;gap:4px 14px;font-size:12px}
.mqtt-kv dt{color:var(--dsw-alias-label-secondary,#6b6f76)}
.mqtt-kv dd{margin:0;font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
.mqtt-table{width:100%;border-collapse:collapse;font-size:12px}
.mqtt-table th{text-align:left;font-weight:600;color:var(--dsw-alias-label-secondary,#6b6f76);font-size:11px;text-transform:uppercase;letter-spacing:.4px;padding:4px 6px;border-bottom:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08))}
.mqtt-table td{padding:5px 6px;border-bottom:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.05));vertical-align:top}
.mqtt-table td.mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
.mqtt-toast{position:absolute;left:50%;bottom:18px;transform:translateX(-50%);background:var(--dsw-alias-bg-overlay,#22262b);color:#fff;color:var(--dsw-alias-label-primary,#fff);padding:7px 14px;border-radius:8px;font-size:12px;box-shadow:0 6px 20px rgba(0,0,0,.25);max-width:60%;z-index:5}
.mqtt-wrap{position:relative;height:100%;display:flex;flex-direction:column;min-height:0}
.mqtt-banner{display:flex;gap:8px;align-items:center;padding:7px 12px;border-radius:8px;background:rgba(127,127,127,0.2);background:color-mix(in srgb,var(--dsw-alias-state-warn-primary,#e0a020) 14%,transparent);color:var(--dsw-alias-label-primary,#5a4200);font-size:12px;margin-bottom:12px}
.mqtt-banner.err{background:rgba(127,127,127,0.2);background:color-mix(in srgb,var(--dsw-alias-state-error-primary,#d64545) 12%,transparent);color:var(--dsw-alias-state-error-primary,#d64545)}
.mqtt-error{white-space:pre-wrap;word-break:break-word;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:11px;line-height:1.5;max-height:320px;overflow:auto;margin:0}
/* message cards */
.mqtt-log-rows{display:flex;flex-direction:column;padding:10px 12px 12px}
.mqtt-log-row.mqtt-msg{display:flex;flex-direction:column;align-items:stretch;gap:4px;max-width:min(80%,720px);margin:0 0 10px;padding:8px 10px 7px;border:1px solid rgba(127,127,127,0.28);border:1px solid color-mix(in srgb,currentColor 12%,transparent);border-radius:10px;background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 4%,transparent);font-size:12px;line-height:1.5}
.mqtt-log-row.mqtt-msg.in{align-self:flex-start;border-left:2px solid var(--dsw-alias-state-success-primary,#25a35a);margin-right:auto}
.mqtt-log-row.mqtt-msg.out{align-self:flex-end;border-right:2px solid #2f6feb;background:rgba(47,111,235,0.10);border-color:rgba(47,111,235,0.35);margin-left:auto}
.mqtt-msg-head{display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.mqtt-msg-topic{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-weight:600;overflow-wrap:anywhere}
.mqtt-msg-badge{font-size:10px;line-height:1.35;padding:0 5px;border-radius:999px;border:1px solid rgba(127,127,127,0.35);border:1px solid color-mix(in srgb,currentColor 18%,transparent);color:var(--dsw-alias-label-secondary,#6b6f76);letter-spacing:.2px}
.mqtt-msg-badge.retain{background:#e8eefc;border-color:rgba(30,86,201,.45);color:#1e56c9}
.mqtt-root.mqtt-scheme-dark .mqtt-msg-badge.retain{background:#1c2740;border-color:rgba(107,157,245,.55);color:#9dbdf7}
.mqtt-msg-src{font-size:10px;color:var(--dsw-alias-label-secondary,#8a8f96)}
.mqtt-msg-time{margin-left:auto;font-size:10px;color:var(--dsw-alias-label-secondary,#8a8f96);font-variant-numeric:tabular-nums}
.mqtt-msg-body{margin:0;white-space:pre-wrap;overflow-wrap:anywhere;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:11.5px;line-height:1.5;max-height:240px;overflow:auto}
.mqtt-msg-foot{font-size:10px;color:var(--dsw-alias-label-secondary,#8a8f96)}
/* subscription chips */
.mqtt-subs{flex:none;display:flex;align-items:center;gap:6px;padding:7px 12px;border-bottom:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.06));flex-wrap:wrap}
.mqtt-subs-label{font-size:10px;text-transform:uppercase;letter-spacing:.6px;color:var(--dsw-alias-label-secondary,#8a8f96);margin-right:2px}
.mqtt-chip{display:inline-flex;align-items:center;gap:5px;border:1px solid rgba(127,127,127,0.45);border:1px solid color-mix(in srgb,currentColor 16%,transparent);border-radius:999px;padding:1px 3px 1px 8px;font-size:11px;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;background:rgba(127,127,127,0.1);background:color-mix(in srgb,currentColor 5%,transparent);max-width:100%}
.mqtt-chip-topic{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:260px}
.mqtt-chip-qos{opacity:.65;font-size:10px}
.mqtt-chip-x{appearance:none;border:none;background:transparent;color:inherit;cursor:pointer;font:inherit;font-size:11px;line-height:1;padding:2px 5px;border-radius:999px;opacity:.6}
.mqtt-chip-x:hover{background:rgba(127,127,127,0.2);background:color-mix(in srgb,currentColor 14%,transparent);opacity:1}
.mqtt-chip.add{cursor:pointer;padding:1px 9px;color:var(--dsw-alias-label-secondary,#6b6f76);background:transparent;border-style:dashed;font-family:inherit}
.mqtt-chip.add:hover{color:inherit}
.mqtt-subform{flex:none;display:flex;gap:6px;align-items:center;padding:0 12px 8px;flex-wrap:wrap}
.mqtt-subform>input.mqtt-input{flex:1;min-width:150px;height:28px;font-size:12px}
.mqtt-subform .mqtt-select{height:28px;font-size:12px;padding:2px 6px}
/* connection list search */
.mqtt-list-search{flex:none;display:flex;padding:0 8px 6px}
.mqtt-list-search>input.mqtt-input{width:100%;height:28px;font-size:12px;padding:2px 9px;border-radius:7px}
.mqtt-list-count{flex:none;font-size:10px;color:var(--dsw-alias-label-secondary,#8a8f96);padding:0 10px 4px}
/* composer card */
.mqtt-composer{flex:none;display:flex;flex-direction:column;align-items:stretch;gap:6px;padding:2px 12px 11px;border-top:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08));background:rgba(127,127,127,0.06);background:color-mix(in srgb,currentColor 2.5%,transparent)}
.mqtt-composer-topic{display:flex;align-items:center;gap:8px;min-width:0}
.mqtt-composer-label{flex:none;font-size:10px;text-transform:uppercase;letter-spacing:.6px;color:var(--dsw-alias-label-secondary,#8a8f96)}
.mqtt-composer-topic>input.mqtt-input{flex:1;min-width:0;width:auto;height:28px;padding:3px 9px;font-size:12px;font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
.mqtt-composer>textarea.mqtt-input{width:100%;box-sizing:border-box;flex:none;resize:none;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:12px;line-height:1.6;padding:7px 10px;border-radius:9px}
.mqtt-composer>textarea.mqtt-input::placeholder{font-family:inherit}
.mqtt-composer-foot{display:flex;align-items:center;gap:8px;min-width:0}
.mqtt-composer-opts{display:flex;align-items:center;gap:8px;flex:1;min-width:0;flex-wrap:wrap}
.mqtt-composer-opts>.mqtt-select{width:auto;height:26px;padding:1px 6px;font-size:11px}
.mqtt-composer-opts>.mqtt-check{font-size:11px;white-space:nowrap}
.mqtt-composer-opts>.mqtt-hint{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mqtt-composer-foot>.mqtt-btn{flex:none}
.mqtt-clients{flex:1;min-height:0;display:grid;grid-template-columns:var(--mqtt-list-width,252px) 6px minmax(0,1fr);overflow:hidden}
.mqtt-clients>.mqtt-list{width:auto;border-right:none}
.mqtt-gutter{position:relative;cursor:col-resize;background:transparent;border-right:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08))}
.mqtt-gutter::after{content:"";position:absolute;top:50%;left:1px;width:3px;height:34px;margin-top:-17px;border-radius:2px;background:rgba(127,127,127,0.35);background:color-mix(in srgb,currentColor 26%,transparent);opacity:0;transition:opacity .15s}
.mqtt-clients:hover .mqtt-gutter::after,.mqtt-gutter:hover::after{opacity:1}
.mqtt-gutter:hover{background:rgba(127,127,127,0.18);background:color-mix(in srgb,currentColor 12%,transparent)}
.mqtt-clientpane{flex:1;min-width:0;min-height:0;display:flex;flex-direction:column;overflow:hidden}
.mqtt-clientbar{flex:none;display:flex;align-items:center;gap:8px;padding:8px 12px;border-bottom:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08));min-width:0}
.mqtt-clientname{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:26%}
/* A narrow pane must fold the URL with an ellipsis instead of running text
   under the action buttons. */
.mqtt-clientbar>.mqtt-sub{flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mqtt-clientbar>.mqtt-btn{flex:none;white-space:nowrap}
.mqtt-settings{flex:0 1 auto;max-height:52%;min-height:0;overflow:auto;padding:12px 14px 14px;border-bottom:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08));background:rgba(127,127,127,0.06);background:color-mix(in srgb,currentColor 4%,transparent)}
.mqtt-settings .mqtt-section{margin-bottom:12px}
/* A folded-out drawer can outgrow a short panel; keep the edge readable so the
   cut never looks like missing content. */
.mqtt-settings{scrollbar-width:thin;scrollbar-color:rgba(127,127,127,.38) transparent}
.mqtt-settings::-webkit-scrollbar{width:8px;height:8px}
.mqtt-settings::-webkit-scrollbar-thumb{background:rgba(127,127,127,.38);border-radius:4px}
.mqtt-settings::-webkit-scrollbar-track{background:transparent}
.mqtt-clientpane>.mqtt-log{flex:1 1 auto;height:auto;min-height:110px;border-top:none}
/* The broker pane mirrors a connection: status bar, foldable settings drawer,
   the message log and a send card at the bottom. */
.mqtt-pane>.mqtt-log{flex:1 1 0;height:auto;min-height:110px;border-top:none}
.mqtt-pane>.mqtt-settings{flex:0 1 auto;max-height:none;min-height:96px;overscroll-behavior:contain;margin:8px 10px 0;border:1px solid var(--dsw-alias-border-l1,rgba(0,0,0,.08));border-radius:10px}
.mqtt-pane>.mqtt-composer{margin-top:7px}
.mqtt-composer.mqtt-composer-flat{padding:9px 12px 11px}
@media (max-width:640px){.mqtt-pane>.mqtt-settings .mqtt-split{grid-template-columns:minmax(0,1fr)}}
.mqtt-composer-grip{height:9px;margin:-4px 0 -1px;display:flex;align-items:center;justify-content:center;cursor:ns-resize;touch-action:none;outline:none}
.mqtt-composer-grip::after{content:'';width:46px;height:3px;border-radius:2px;background:currentColor;opacity:.16;transition:opacity .15s}
.mqtt-composer:hover .mqtt-composer-grip::after,.mqtt-composer-grip:focus-visible::after{opacity:.45}
.mqtt-composer-grip:focus-visible::after{background:var(--dsw-alias-brand-primary,#2f6feb);opacity:.9}
/* On a narrow pane the options drop to their own line instead of squeezing the
   send button off the edge. */
@media (max-width:520px){.mqtt-composer-foot{flex-wrap:wrap}.mqtt-composer-foot>.mqtt-btn{width:100%}}
`

    // ------------------------------------------------------------- api client
    function useMqttApi() {
      const [state, setState] = useState(null)
      const [messages, setMessages] = useState([])
      const [events, setEvents] = useState([])
      const [live, setLive] = useState(false)
      const [fatal, setFatal] = useState(null)

      useEffect(() => {
        let source = null
        let disposed = false
        let openTimer = null
        // Burst batching: a busy broker can emit hundreds of frames per second.
        // Buffer them and flush at most every LOG_FLUSH_MS so the panel renders
        // once per batch instead of once per message.
        const pending = { messages: [], events: [] }
        let flushTimer = null
        const trim = (list) => (list.length > MAX_ROWS ? list.slice(list.length - MAX_ROWS) : list)
        const flush = () => {
          flushTimer = null
          if (pending.messages.length > 0) {
            const batch = pending.messages.splice(0, pending.messages.length)
            setMessages((current) => trim(current.concat(batch)))
          }
          if (pending.events.length > 0) {
            const batch = pending.events.splice(0, pending.events.length)
            setEvents((current) => trim(current.concat(batch)))
          }
        }
        const schedule = () => {
          if (flushTimer === null) flushTimer = setTimeout(flush, LOG_FLUSH_MS)
        }
        const queue = (kind, event) => {
          try {
            pending[kind].push(JSON.parse(event.data))
          } catch {
            return
          }
          schedule()
        }
        // Clear one ring (or a slice of it): a scoped clear must not throw away
        // frames the other log already queued for the next flush.
        const dropPending = (kind = 'both', scope = {}) => {
          if (kind !== 'events') {
            pending.messages = pending.messages.filter((entry) => {
              if (scope.scope === 'broker') return entry.kind !== 'broker'
              if (typeof scope.clientId === 'string') return entry.clientId !== scope.clientId
              return false
            })
          }
          if (kind !== 'messages') pending.events.length = 0
          if (flushTimer !== null && pending.messages.length === 0 && pending.events.length === 0) {
            clearTimeout(flushTimer)
            flushTimer = null
          }
        }
        try {
          source = new EventSource(`${API}/events`)
        } catch (error) {
          setFatal(errorText(error))
          return undefined
        }
        openTimer = setTimeout(() => {
          // A page that never receives the first frame would otherwise sit on
          // "loading" forever without telling the user anything.
          if (!disposed) {
            setFatal(
              t(
                'no answer from the MQTT service after 8 seconds — check the DSH log or reload the page',
                'MQTT 服务 8 秒内没有响应 — 请查看 DSH 日志或刷新页面',
              ),
            )
          }
        }, OPEN_TIMEOUT_MS)
        source.addEventListener('hello', (event) => {
          try {
            const data = JSON.parse(event.data)
            dropPending()
            setState(data.state ?? null)
            setMessages(Array.isArray(data.messages) ? data.messages : [])
            setEvents(Array.isArray(data.events) ? data.events : [])
            setLive(true)
            if (openTimer) {
              clearTimeout(openTimer)
              openTimer = null
            }
            setFatal(null)
          } catch (error) {
            setFatal(errorText(error))
          }
        })
        source.addEventListener('state', (event) => {
          try {
            setState(JSON.parse(event.data))
            setLive(true)
          } catch {
            /* ignore one malformed frame */
          }
        })
        source.addEventListener('message', (event) => queue('messages', event))
        source.addEventListener('broker-event', (event) => queue('events', event))
        source.addEventListener('cleared', (event) => {
          let kind = 'both'
          let clientId = ''
          let scope = ''
          try {
            const data = JSON.parse(event.data || '{}')
            if (data?.kind === 'messages' || data?.kind === 'events') kind = data.kind
            if (typeof data?.clientId === 'string') clientId = data.clientId
            if (data?.scope === 'broker') scope = 'broker'
          } catch {
            /* a host that predates scoped clears sends an empty frame: wipe both */
          }
          dropPending(kind, { clientId, scope })
          if (kind !== 'events') {
            // The message ring is shared: only the rows this clear named go away.
            if (scope === 'broker') setMessages((current) => current.filter((entry) => entry.kind !== 'broker'))
            else if (clientId !== '') setMessages((current) => current.filter((entry) => entry.clientId !== clientId))
            else setMessages([])
          }
          if (kind !== 'messages') setEvents([])
        })
        source.onopen = () => setLive(true)
        source.onerror = () => {
          if (!disposed) setLive(false)
        }
        return () => {
          disposed = true
          if (openTimer) clearTimeout(openTimer)
          dropPending()
          source.close()
        }
      }, [])

      const run = useCallback(async (cmd, params = {}) => {
        const controller = typeof AbortController === 'function' ? new AbortController() : null
        const timer = controller ? setTimeout(() => controller.abort(), COMMAND_TIMEOUT_MS) : null
        let response
        try {
          response = await fetch(`${API}/command`, {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'x-dsh-mqtt-tools': '1' },
            body: JSON.stringify({ cmd, ...params }),
            signal: controller ? controller.signal : undefined,
          })
        } catch (error) {
          if (error && (error.name === 'AbortError' || error.name === 'TimeoutError')) {
            throw new Error(t('the command timed out', '命令超时'))
          }
          throw error
        } finally {
          if (timer) clearTimeout(timer)
        }
        let data
        try {
          data = await response.json()
        } catch {
          throw new Error(`HTTP ${response.status}`)
        }
        if (data.state) setState(data.state)
        if (!data.ok) throw new Error(data.error || `HTTP ${response.status}`)
        return data.result
      }, [])

      return { state, messages, events, live, fatal, run }
    }

    // ------------------------------------------------------------ primitives
    function TextInput({ value, onChange, placeholder, type = 'text', disabled = false, min, max, step }) {
      return h('input', {
        className: 'mqtt-input',
        type,
        value: value ?? '',
        placeholder: placeholder ?? '',
        disabled,
        min,
        max,
        step,
        onChange: (event) => onChange(event.target.value),
      })
    }

    function NumberInput({ value, onChange, min, max, step = 1, disabled = false }) {
      return h('input', {
        className: 'mqtt-input',
        type: 'number',
        value: value ?? '',
        min,
        max,
        step,
        disabled,
        onChange: (event) => onChange(event.target.value === '' ? '' : Number(event.target.value)),
      })
    }

    function Select({ value, options, onChange, disabled = false }) {
      return h(
        'select',
        { className: 'mqtt-select', value: String(value), disabled, onChange: (event) => onChange(event.target.value) },
        options.map((option) => h('option', { key: String(option.value), value: String(option.value) }, option.label)),
      )
    }

    function Field({ label, hint, wide = false, children }) {
      return h(
        'div',
        { className: wide ? 'mqtt-field wide' : 'mqtt-field' },
        h('div', { className: 'mqtt-label' }, label),
        children,
        hint ? h('div', { className: 'mqtt-hint' }, hint) : null,
      )
    }

    function Check({ checked, onChange, label }) {
      return h(
        'label',
        { className: 'mqtt-check' },
        h('input', { type: 'checkbox', checked: Boolean(checked), onChange: (event) => onChange(event.target.checked) }),
        h('span', null, label),
      )
    }

    function Button({ children, onClick, variant, disabled = false, title }) {
      return h(
        'button',
        { type: 'button', className: variant ? `mqtt-btn ${variant}` : 'mqtt-btn', onClick, disabled, title },
        children,
      )
    }

    function StatusBadge({ status, error }) {
      return h(
        'span',
        { className: `mqtt-badge ${statusTone(status)}`, title: error || '' },
        h('span', { className: `mqtt-dot ${statusTone(status)}` }),
        statusLabel(status),
      )
    }

    function useFlash() {
      const [message, setMessage] = useState(null)
      const timer = useRef(null)
      const flash = useCallback((text) => {
        setMessage(text)
        if (timer.current) clearTimeout(timer.current)
        timer.current = setTimeout(() => setMessage(null), 2600)
      }, [])
      useEffect(() => () => { if (timer.current) clearTimeout(timer.current) }, [])
      return [message, flash]
    }

    // Shared "run one host command" helper: every view that talks to the API
    // needs the same disable-controls / flash-the-outcome dance. The fifth
    // value is the raw setter for the occasional two-step action.
    function useBusyAction() {
      const [busy, setBusy] = useState(false)
      const [message, flash] = useFlash()
      const act = useCallback(async (label, action) => {
        setBusy(true)
        try {
          await action()
          if (label) flash(label)
        } catch (error) {
          flash(errorText(error))
        } finally {
          setBusy(false)
        }
      }, [flash])
      return [busy, act, message, flash, setBusy]
    }

    // --------------------------------------------------------- message log
    const PAGE_SIZE = 100
    function MessageLog({ entries, onClear, emptyText, showSource, storageKey }) {
      // The pane is unmounted when the other tab is shown; the reader's filter,
      // direction and pause switch should not reset with it.
      const remembered = storageKey ? logViewState.get(storageKey) : undefined
      const [filter, setFilter] = useState(remembered?.filter ?? '')
      const [direction, setDirection] = useState(remembered?.direction ?? 'all')
      const [paused, setPaused] = useState(remembered?.paused ?? false)
      const [limit, setLimit] = useState(PAGE_SIZE)
      const boxRef = useRef(null)
      const pausedLength = useRef(null)
      useEffect(() => {
        if (storageKey) logViewState.set(storageKey, { filter, direction, paused })
      }, [storageKey, filter, direction, paused])

      const shown = useMemo(() => {
        const needle = filter.trim().toLowerCase()
        return entries.filter((entry) => {
          if (direction !== 'all' && entry.direction !== direction) return false
          if (!needle) return true
          return (
            String(entry.topic ?? '').toLowerCase().includes(needle) ||
            String(entry.payload ?? '').toLowerCase().includes(needle) ||
            String(entry.name ?? '').toLowerCase().includes(needle)
          )
        })
      }, [entries, filter, direction])

      useEffect(() => {
        setLimit(PAGE_SIZE)
      }, [filter, direction])

      // The newest page stays on screen; older rows are pulled in on demand so a
      // busy broker never turns the panel into one endless scroll. While paused
      // the window is pinned: new rows must not slide the rows the reader is
      // looking at out of the page.
      if (!paused) pausedLength.current = null
      else if (pausedLength.current === null) pausedLength.current = shown.length
      const visible = pausedLength.current === null ? shown : shown.slice(0, pausedLength.current)
      const page = visible.length > limit ? visible.slice(visible.length - limit) : visible
      // Follow the newest row, not the page length: once the page is pinned at
      // its limit the length stops changing and the log would silently stop
      // following new messages.
      const lastSeq = page.length > 0 ? page[page.length - 1].seq : 0
      const pausedAtSeq = useRef(0)
      // Count only what the current filter would actually reveal after resuming.
      const unseen = paused ? shown.filter((entry) => entry.seq > pausedAtSeq.current).length : 0
      useEffect(() => {
        if (!paused) pausedAtSeq.current = lastSeq
      }, [paused, lastSeq])

      useEffect(() => {
        if (paused) return
        const box = boxRef.current
        if (box) box.scrollTop = box.scrollHeight
      }, [lastSeq, paused])

      const onScroll = () => {
        const box = boxRef.current
        if (!box) return
        const nearBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 48
        setPaused(!nearBottom)
      }

      return h(
        'div',
        { className: 'mqtt-log' },
        h(
          'div',
          { className: 'mqtt-log-bar' },
          h('input', {
            className: 'mqtt-input',
            placeholder: t('filter topic or payload', '过滤 topic 或内容'),
            'aria-label': t('filter messages', '过滤消息'),
            value: filter,
            onChange: (event) => setFilter(event.target.value),
          }),
          h(Select, {
            value: direction,
            onChange: setDirection,
            options: [
              { value: 'all', label: t('all', '全部') },
              { value: 'in', label: t('incoming', '接收') },
              { value: 'out', label: t('outgoing', '发送') },
            ],
          }),
          h(
            Button,
            {
              variant: 'ghost',
              onClick: () => setPaused((value) => !value),
              title: t('pause auto-scroll', '暂停自动滚动'),
            },
            paused ? (unseen > 0 ? `${t('resume', '继续')} (${unseen})` : t('resume', '继续')) : t('pause', '暂停'),
          ),
          h(Button, { variant: 'ghost', onClick: onClear, title: t('clear log', '清空日志') }, t('clear', '清空')),
          shown.length > page.length
            ? h(
                Button,
                { variant: 'ghost', onClick: () => setLimit((value) => value + PAGE_SIZE), title: t('load older messages', '加载更早的消息') },
                `${t('older', '更早')} (${shown.length - page.length})`,
              )
            : null,
          h(
            'span',
            { className: 'mqtt-log-count' },
            `${page.length} / ${shown.length}${entries.length > shown.length ? ` · ${entries.length} ${t('total', '总计')}` : ''}`,
          ),
        ),
        h(
          'div',
          {
            className: 'mqtt-log-rows',
            ref: boxRef,
            onScroll,
            role: 'log',
            'aria-label': t('MQTT messages', 'MQTT 消息'),
          },
          shown.length === 0
            ? h('div', { className: 'mqtt-empty' }, emptyText ?? t('no messages yet', '还没有消息'))
            : page.map((entry) => h(LogRow, { key: entry.seq, entry, showSource })),
        ),
      )
    }

    // A single log line. Rows are immutable once appended, so memoisation
    // keeps a busy broker from re-rendering the whole page on every frame.
    function prettyPayload(entry) {
      const payload = String(entry.payload ?? '')
      if (entry.encoding === 'base64') return 'base64:' + payload
      if (payload.length > 4096) return payload
      const trimmed = payload.trim()
      if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return payload
      try {
        return JSON.stringify(JSON.parse(trimmed), null, 2)
      } catch {
        return payload
      }
    }

    const LogRow = React.memo(function LogRow({ entry, showSource }) {
      const outgoing = entry.direction === 'out'
      return h(
        'div',
        { className: `mqtt-log-row mqtt-msg ${outgoing ? 'out' : 'in'}` },
        h(
          'div',
          { className: 'mqtt-msg-head' },
          h('span', { className: 'mqtt-msg-topic', title: entry.topic }, entry.topic),
          h('span', { className: 'mqtt-msg-badge' }, `QoS ${entry.qos}`),
          entry.retain ? h('span', { className: 'mqtt-msg-badge retain' }, 'RETAIN') : null,
          showSource && entry.name ? h('span', { className: 'mqtt-msg-src' }, entry.name) : null,
          h('span', { className: 'mqtt-msg-time' }, formatTime(entry.ts)),
        ),
        h('pre', { className: 'mqtt-msg-body' }, prettyPayload(entry) || ' '),
        h('div', { className: 'mqtt-msg-foot' }, `${entry.bytes} B`),
      )
    })

    // Broker event line (start / connect / subscribe / error), same idea.
    const EventRow = React.memo(function EventRow({ entry }) {
      return h(
        'div',
        { className: 'mqtt-log-row mqtt-event-row' },
        h('span', { className: 'mqtt-log-time' }, formatTime(entry.ts)),
        h('span', { className: `mqtt-log-dir ${entry.action === 'error' ? 'out' : 'in'}` }, entry.action === 'error' ? '!' : '•'),
        h('span', { className: 'mqtt-log-topic' }, entry.action),
        h('span', { className: 'mqtt-log-payload', title: entry.text || '' }, entry.clientId ? `${entry.clientId} — ${entry.text || ''}` : entry.text || ''),
      )
    })

    // -------------------------------------------------------- publish composer
    // The broker's send bar is the same card as the clients' composer: the topic
    // line on top, the message box under it, options and the send button along
    // the bottom — and the same resize grip, so the message box can be dragged
    // taller when a payload needs the room.
    function Publisher({ onPublish, disabledReason }) {
      // The broker card keeps its unsent draft across tab switches, like the
      // clients' composers do.
      const [topic, setTopic] = useState(brokerPublisherDraft.topic)
      const [payload, setPayload] = useState(brokerPublisherDraft.payload)
      const [qos, setQos] = useState('0')
      const [retain, setRetain] = useState(false)
      const [busy, setBusy] = useState(false)
      const [message, flash] = useFlash()
      const [height, setHeight] = useState(publisherHeight)
      const areaRef = useRef(null)
      const topicRef = useRef(null)
      const cardRef = useRef(null)
      const dragStop = useRef(null)

      // The message box never eats the pane: the bar, a couple of log rows and
      // the drawer (which may scroll) keep their room whatever the user drags to.
      const PUBLISHER_LOG_KEEP = 110
      const PUBLISHER_SETTINGS_KEEP = 180
      const PUBLISHER_CHROME_KEEP = 8
      const heightCap = () => {
        const root = cardRef.current
        const pane = root?.parentElement
        const field = areaRef.current
        const room = pane?.clientHeight ?? 0
        // Detached or pre-layout (jsdom, first paint): the static limit applies.
        if (!root || !pane || !field || room <= 0) return PUBLISHER_MAX
        let taken = 0
        for (const child of pane.children) {
          if (child === root) continue
          const childHeight = child.getBoundingClientRect().height || 0
          if (child.classList.contains('mqtt-log')) continue
          taken += child.classList.contains('mqtt-settings')
            ? Math.min(childHeight, PUBLISHER_SETTINGS_KEEP)
            : childHeight
        }
        const chrome = Math.max(0, root.getBoundingClientRect().height - field.getBoundingClientRect().height)
        const room4field = room - taken - PUBLISHER_LOG_KEEP - chrome - PUBLISHER_CHROME_KEEP
        return Math.max(PUBLISHER_MIN, Math.min(PUBLISHER_MAX, Math.round(room4field)))
      }

      // Height the box picks for itself: it grows with the text and falls back
      // to the starting size, never below it.
      const setAutoHeight = (next) => {
        const value = Math.max(PUBLISHER_MIN, Math.min(Math.round(next), heightCap()))
        publisherHeight = value
        setHeight(value)
      }

      // Grow with the text, shrink back to the starting size: measure the text
      // on a collapsed box first, a fixed-height box always reports its own height.
      const growToFit = () => {
        const field = areaRef.current
        // A height the user chose by dragging stays put; the text scrolls.
        if (!field || !publisherHeightAuto) return
        const border = Math.max(0, field.offsetHeight - field.clientHeight)
        const inline = field.style.height
        field.style.height = '0px'
        const content = field.scrollHeight
        field.style.height = inline
        const limit = Math.min(PUBLISHER_GROW_LIMIT, heightCap())
        const needed = Math.min(limit, Math.max(PUBLISHER_START, Math.ceil(content + border)))
        if (needed !== height) setAutoHeight(needed)
      }

      const applyHeight = (next) => {
        const value = Math.min(heightCap(), Math.max(PUBLISHER_MIN, Math.round(next)))
        publisherHeight = value
        // Once the user has picked a height by hand, stop auto-growing.
        publisherHeightAuto = false
        setHeight(value)
      }

      const startResize = (event) => {
        if (typeof event.button === 'number' && event.button !== 0) return
        event.preventDefault()
        publisherHeightAuto = false
        const startY = event.clientY
        const startHeight = height
        const move = (moveEvent) => applyHeight(startHeight + (startY - moveEvent.clientY))
        const finish = () => {
          window.removeEventListener('mousemove', move)
          window.removeEventListener('mouseup', finish)
          window.removeEventListener('blur', finish)
          dragStop.current = null
        }
        dragStop.current = finish
        window.addEventListener('mousemove', move)
        window.addEventListener('mouseup', finish)
        window.addEventListener('blur', finish)
      }

      const onGripKeyDown = (event) => {
        if (event.key === 'ArrowUp') {
          event.preventDefault()
          applyHeight(height + 16)
        } else if (event.key === 'ArrowDown') {
          event.preventDefault()
          applyHeight(height - 16)
        }
      }

      // A drag in flight must not keep calling setState on an unmounted card
      // (switching tabs unmounts this pane).
      useEffect(() => () => dragStop.current?.(), [])

      const send = async () => {
        if (!topic.trim()) {
          flash(t('a topic is required', '请填写 topic'))
          topicRef.current?.focus()
          return
        }
        setBusy(true)
        try {
          await onPublish({ topic: topic.trim(), payload, qos: Number(qos), retain })
          flash(t('published', '已发布'))
          brokerPublisherDraft.payload = ''
          setPayload('')
          // A height the user dragged stays where they left it; the auto-grown
          // one falls back, so a short next message does not keep the tall box.
          if (publisherHeightAuto) {
            publisherHeight = PUBLISHER_START
            setHeight(PUBLISHER_START)
            if (areaRef.current) areaRef.current.style.height = PUBLISHER_START + 'px'
          }
          // The next message is typed without reaching for the mouse.
          areaRef.current?.focus()
        } catch (error) {
          flash(errorText(error))
        } finally {
          setBusy(false)
        }
      }

      // Enter publishes, Shift+Enter breaks the line, a composing Enter belongs
      // to the IME; the topic card sends the same way as the clients' composer.
      // A stopped broker has nothing to send, so the key must stay a newline
      // instead of being swallowed by a dead handler.
      const onKeyDown = (event) => {
        if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent?.isComposing) return
        if (disabledReason || busy) return
        event.preventDefault()
        send()
      }

      const onTopicKeyDown = (event) => {
        if (event.key === 'Enter' || event.key === 'ArrowDown') {
          event.preventDefault()
          areaRef.current?.focus()
        }
      }

      return h(
        'div',
        { className: 'mqtt-composer mqtt-composer-flat', ref: cardRef },
        h('div', {
          className: 'mqtt-composer-grip',
          role: 'separator',
          'aria-orientation': 'horizontal',
          'aria-label': t('resize the message box', '拖动调整消息框高度'),
          'aria-valuenow': height,
          'aria-valuemin': PUBLISHER_MIN,
          'aria-valuemax': PUBLISHER_MAX,
          tabIndex: 0,
          title: t('drag to resize · arrow keys work too', '拖动调整高度 · 也支持方向键'),
          onMouseDown: startResize,
          onKeyDown: onGripKeyDown,
        }),
        h(
          'div',
          { className: 'mqtt-composer-topic' },
          h('span', { className: 'mqtt-composer-label' }, t('topic', '主题')),
          h('input', {
            className: 'mqtt-input',
            ref: topicRef,
            placeholder: 'sensors/room-1/temperature',
            value: topic,
            onChange: (event) => {
              brokerPublisherDraft.topic = event.target.value
              setTopic(event.target.value)
            },
            onKeyDown: onTopicKeyDown,
            spellCheck: false,
            autoComplete: 'off',
            'aria-label': t('topic', '主题'),
          }),
        ),
        h('textarea', {
          className: 'mqtt-input',
          ref: areaRef,
          rows: 1,
          style: { height: height + 'px' },
          placeholder: t('text or JSON', '文本或 JSON'),
          value: payload,
          onChange: (event) => {
            brokerPublisherDraft.payload = event.target.value
            setPayload(event.target.value)
            growToFit()
          },
          onKeyDown,
          spellCheck: false,
          'aria-label': t('payload', '消息内容'),
        }),
        h(
          'div',
          { className: 'mqtt-composer-foot' },
          h(
            'div',
            { className: 'mqtt-composer-opts' },
            h(Select, {
              value: qos,
              onChange: setQos,
              options: [
                { value: '0', label: t('QoS 0 · at most once', 'QoS 0 · 至多一次') },
                { value: '1', label: t('QoS 1 · at least once', 'QoS 1 · 至少一次') },
                { value: '2', label: t('QoS 2 · exactly once', 'QoS 2 · 恰好一次') },
              ],
            }),
            h(Check, { checked: retain, onChange: setRetain, label: t('retain', '保留消息 (retain)') }),
            disabledReason ? h('span', { className: 'mqtt-hint' }, disabledReason) : null,
            message ? h('span', { className: 'mqtt-hint' }, message) : null,
          ),
          h(
            Button,
            {
              variant: 'primary',
              onClick: send,
              // No topic yet means nothing can be sent: disable the control instead
              // of letting the click flash "a topic is required".
              disabled: busy || Boolean(disabledReason) || !topic.trim(),
              title: disabledReason
                ? disabledReason
                : topic.trim()
                  ? t('publish (Enter)', '发布（回车）')
                  : t('a topic is required', '请填写 topic'),
            },
            busy ? t('sending…', '发送中…') : t('publish', '发布'),
          ),
        ),
      )
    }

    // ----------------------------------------------------------- connections
    function draftOf(profile) {
      return {
        id: profile.id,
        name: profile.name,
        url: profile.url,
        clientId: profile.clientId,
        username: profile.username,
        password: '',
        keepalive: profile.keepalive,
        clean: profile.clean,
        protocolVersion: profile.protocolVersion,
        connectTimeout: profile.connectTimeout,
        reconnectPeriod: profile.reconnectPeriod,
        rejectUnauthorized: profile.rejectUnauthorized,
        autoConnect: profile.autoConnect,
      }
    }

    // Settings the drawer shows that differ from what the host already saved.
    // The password is write-only, so only a typed value can make it dirty.
    function draftDiffers(profile, draft) {
      const saved = draftOf(profile)
      return Object.keys(saved).some((key) => (key === 'password' ? draft.password !== '' : draft[key] !== saved[key]))
    }

    // One saved connection's settings, rendered inside the collapsible drawer
    // of the clients pane. The draft lives in ClientPane: folding the drawer
    // away must not throw away half-typed settings, and the connect button has
    // to be able to persist them before it dials out.
    function ClientSettings({ profile, draft, update, reset, run }) {
      const [confirming, setConfirming] = useState(false)
      const [busy, act, message] = useBusyAction()
      const online = profile.status === 'connected' || profile.status === 'connecting' || profile.status === 'reconnecting'

      const save = () =>
        act(t('saved', '已保存'), async () => {
          await run('client.save', { profile: draft })
          // The password is write-only: clear it so the stored one stays in place.
          update({ password: '' })
        })
      const remove = () => act(t('deleted', '已删除'), () => run('client.delete', { id: profile.id }))

      return h(
        'div',
        { className: 'mqtt-section' },
        h('div', { className: 'mqtt-section-title' }, t('connection', '连接设置')),
        h(
          'div',
          { className: 'mqtt-grid' },
          h(Field, { label: t('name', '名称') }, h(TextInput, { value: draft.name, onChange: (value) => update({ name: value }) })),
          h(Field, { label: t('broker url', 'Broker 地址'), hint: 'mqtt:// mqtts:// ws:// wss://' }, h(TextInput, { value: draft.url, onChange: (value) => update({ url: value }), placeholder: 'mqtt://127.0.0.1:1883' })),
          h(Field, { label: t('client id', '客户端 ID'), hint: t('empty = auto', '留空自动生成') }, h(TextInput, { value: draft.clientId, onChange: (value) => update({ clientId: value }), placeholder: 'auto' })),
          h(Field, { label: t('username', '用户名') }, h(TextInput, { value: draft.username, onChange: (value) => update({ username: value }) })),
          h(Field, { label: t('password', '密码') }, h(TextInput, { value: draft.password, onChange: (value) => update({ password: value }), type: 'password', placeholder: profile.hasPassword ? t('saved — leave empty to keep', '已保存 — 留空保持不变') : '' })),
          h(Field, { label: t('keep alive (s)', '心跳 keepalive (秒)') }, h(NumberInput, { value: draft.keepalive, onChange: (value) => update({ keepalive: value }), min: 0, max: 65535 })),
          h(Field, { label: t('protocol', '协议版本') }, h(Select, { value: draft.protocolVersion, onChange: (value) => update({ protocolVersion: Number(value) }), options: [{ value: '3', label: 'MQTT 3.1' }, { value: '4', label: 'MQTT 3.1.1' }, { value: '5', label: 'MQTT 5.0' }] })),
          h(Field, { label: t('reconnect period (ms)', '重连间隔 (毫秒)'), hint: '0 = off' }, h(NumberInput, { value: draft.reconnectPeriod, onChange: (value) => update({ reconnectPeriod: value }), min: 0, max: 600000 })),
          h(Field, { label: t('connect timeout (ms)', '连接超时 (毫秒)') }, h(NumberInput, { value: draft.connectTimeout, onChange: (value) => update({ connectTimeout: value }), min: 1000, max: 300000 })),
          h(
            Field,
            { label: t('options', '选项'), wide: true },
            h(
              'div',
              { className: 'mqtt-row' },
              h(Check, { checked: draft.clean, onChange: (value) => update({ clean: value }), label: t('clean session', 'clean session') }),
              h(Check, { checked: draft.rejectUnauthorized, onChange: (value) => update({ rejectUnauthorized: value }), label: t('verify TLS certificate', '校验 TLS 证书') }),
              h(Check, { checked: draft.autoConnect, onChange: (value) => update({ autoConnect: value }), label: t('connect at startup', '启动时自动连接') }),
            ),
          ),
        ),
        h(
          'div',
          { className: 'mqtt-actions' },
          h(Button, { variant: 'primary', onClick: save, disabled: busy }, t('save', '保存')),
          h(Button, { onClick: reset }, t('revert', '还原')),
          confirming
            ? h(Button, { variant: 'danger', onClick: remove, disabled: busy }, t('confirm delete', '确认删除'))
            : h(Button, { variant: 'ghost', onClick: () => setConfirming(true) }, t('delete', '删除')),
          message ? h('span', { className: 'mqtt-hint', style: { marginLeft: 'auto' } }, message) : null,
        ),
      )
    }

    // Chat-style composer pinned to the bottom of the clients pane. One card:
    // the topic line on top, the message box under it, the send action in the
    // bottom-right corner (where a chat's send button lives) and the option
    // chips along the bottom-left. The topic is remembered per connection so
    // the second message is a single keystroke.
    function Composer({ profile, run, online }) {
      const [topic, setTopic] = useState(() => composerTopics.get(profile.id) ?? '')
      const [payload, setPayload] = useState(() => composerDrafts.get(profile.id) ?? '')
      const [qos, setQos] = useState(() => composerQos.get(profile.id) ?? '0')
      const [retain, setRetain] = useState(() => composerRetain.get(profile.id) ?? false)
      const [busy, setBusy] = useState(false)
      const [message, flash] = useFlash()
      const [height, setHeight] = useState(() => composerHeight)
      const dragStop = useRef(null)
      const composerRoot = useRef(null)
      const payloadField = useRef(null)
      const topicField = useRef(null)

      useEffect(() => {
        setTopic(composerTopics.get(profile.id) ?? '')
        setPayload(composerDrafts.get(profile.id) ?? '')
        setQos(composerQos.get(profile.id) ?? '0')
        setRetain(composerRetain.get(profile.id) ?? false)
        // eslint-disable-next-line react-hooks/exhaustive-deps
      }, [profile.id])

      useEffect(() => () => {
        if (dragStop.current) dragStop.current()
      }, [])

      // The cursor belongs in the message box: the topic is already filled in.
      // Opening a connection should not steal the focus the user just placed in
      // another field, so only take it when nothing else is focused.
      useEffect(() => {
        if (!online || !payloadField.current) return
        try {
          const active = document.activeElement
          const tag = active && active.tagName
          if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || (active && active.isContentEditable)) return
        } catch {
          /* no document */
        }
        payloadField.current.focus()
        // Only when the connection comes up, not on every render.
        // eslint-disable-next-line react-hooks/exhaustive-deps
      }, [online])

      // A shrinking window must not leave a remembered height that pushes the
      // send button below the pane.
      useEffect(() => {
        const clamp = () => {
          const cap = heightCap()
          if (height > cap) {
            composerHeight = cap
            setHeight(cap)
          }
        }
        clamp()
        window.addEventListener('resize', clamp)
        return () => window.removeEventListener('resize', clamp)
        // eslint-disable-next-line react-hooks/exhaustive-deps
      }, [height, online])

      const send = async () => {
        const value = topic.trim()
        if (!value) {
          flash(t('a topic is required', '请填写 topic'))
          topicField.current?.focus()
          return
        }
        setBusy(true)
        try {
          await run('client.publish', { id: profile.id, topic: value, payload, qos: Number(qos), retain })
          composerTopics.set(profile.id, value)
          composerQos.set(profile.id, qos)
          composerRetain.set(profile.id, retain)
          composerDrafts.delete(profile.id)
          setPayload('')
          // An empty box should be its starting size again.
          setAutoHeight(COMPOSER_START)
          // Keep the cursor where the next message is typed.
          payloadField.current?.focus()
        } catch (error) {
          flash(errorText(error))
        } finally {
          setBusy(false)
        }
      }

      // Enter sends, Shift+Enter breaks the line, but an IME candidate must not
      // swallow the key: composing Enter belongs to the input method. Offline the
      // key is not swallowed either — there is no send to stand in for it.
      const onKeyDown = (event) => {
        if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent?.isComposing) return
        if (!online || busy) return
        event.preventDefault()
        // Even without a topic: send() points the user at the topic field, so
        // the key is never a silent no-op.
        send()
      }

      // ArrowUp from the first line jumps to the topic so the target can be
      // changed without reaching for the mouse. Read the caret live: a tracked
      // value drifts once the box is cleared and refocused.
      const onPayloadKeyDown = (event) => {
        const start = event.target?.selectionStart
        const end = event.target?.selectionEnd
        if (event.key === 'ArrowUp' && !event.shiftKey && start === 0 && end === 0) {
          event.preventDefault()
          topicField.current?.focus()
          topicField.current?.select()
          return
        }
        onKeyDown(event)
      }

      const onTopicKeyDown = (event) => {
        if (event.key === 'Enter') {
          event.preventDefault()
          payloadField.current?.focus()
        } else if (event.key === 'ArrowDown') {
          event.preventDefault()
          payloadField.current?.focus()
        }
      }

      const GROW_LIMIT = 220
      // The message box never eats the pane: the client bar, the subscription
      // chips and a couple of log rows stay visible whatever the user drags to.
      const LOG_KEEP = 110
      const SETTINGS_KEEP = 180
      const CHROME_KEEP = 8
      const heightCap = () => {
        const root = composerRoot.current
        const pane = root?.parentElement
        const field = payloadField.current
        const room = pane?.clientHeight ?? 0
        // Detached or pre-layout (jsdom, first paint): the static limit applies.
        if (!root || !pane || !field || room <= 0) return COMPOSER_MAX
        let taken = 0
        for (const child of pane.children) {
          if (child === root) continue
          const height = child.getBoundingClientRect().height || 0
          // The log keeps only its floor; a settings drawer may scroll.
          if (child.classList.contains('mqtt-log')) continue
          taken += child.classList.contains('mqtt-settings') ? Math.min(height, SETTINGS_KEEP) : height
        }
        // Everything in the composer except the message box itself.
        const chrome = Math.max(0, root.getBoundingClientRect().height - field.getBoundingClientRect().height)
        const room4field = room - taken - LOG_KEEP - chrome - CHROME_KEEP
        return Math.max(COMPOSER_MIN, Math.min(COMPOSER_MAX, Math.round(room4field)))
      }

      // Height the box picks for itself: it grows with the text and falls back
      // to the starting size, never below it.
      const setAutoHeight = (next) => {
        const value = Math.max(COMPOSER_MIN, Math.min(Math.round(next), heightCap()))
        composerHeight = value
        setHeight(value)
      }

      const growToFit = () => {
        const field = payloadField.current
        // A height the user chose by dragging stays put; the text scrolls.
        if (!field || !composerHeightAuto) return
        const border = Math.max(0, field.offsetHeight - field.clientHeight)
        const inline = field.style.height
        // Measure the text on its own: a fixed-height box always reports
        // scrollHeight >= clientHeight, so collapse it first — otherwise the
        // box can only ever grow and deleting the text never gives the room back.
        field.style.height = '0px'
        const content = field.scrollHeight
        field.style.height = inline
        const limit = Math.min(GROW_LIMIT, heightCap())
        const needed = Math.min(limit, Math.max(COMPOSER_START, Math.ceil(content + border)))
        if (needed !== height) setAutoHeight(needed)
      }

      const applyHeight = (next) => {
        const value = Math.min(heightCap(), Math.max(COMPOSER_MIN, Math.round(next)))
        composerHeight = value
        // Once the user has picked a height by hand, stop auto-growing.
        composerHeightAuto = false
        setHeight(value)
      }

      const startResize = (event) => {
        if (typeof event.button === 'number' && event.button !== 0) return
        event.preventDefault()
        composerHeightAuto = false
        const startY = event.clientY
        const startHeight = height
        const move = (moveEvent) => applyHeight(startHeight + (startY - moveEvent.clientY))
        const finish = () => {
          window.removeEventListener("mousemove", move)
          window.removeEventListener("mouseup", finish)
          window.removeEventListener("blur", finish)
          dragStop.current = null
        }
        dragStop.current = finish
        window.addEventListener("mousemove", move)
        window.addEventListener("mouseup", finish)
        window.addEventListener("blur", finish)
      }

      const onGripKeyDown = (event) => {
        if (event.key === 'ArrowUp') {
          event.preventDefault()
          applyHeight(height + 16)
        } else if (event.key === 'ArrowDown') {
          event.preventDefault()
          applyHeight(height - 16)
        }
      }

      const disabledReason = online ? null : t('connect first', '请先连接')

      return h(
        'div',
        { className: 'mqtt-composer', ref: composerRoot },
        h('div', {
          className: 'mqtt-composer-grip',
          role: 'separator',
          'aria-orientation': 'horizontal',
          'aria-label': t('resize the message box', '拖动调整消息框高度'),
          'aria-valuenow': height,
          'aria-valuemin': COMPOSER_MIN,
          'aria-valuemax': COMPOSER_MAX,
          tabIndex: 0,
          title: t('drag to resize · arrow keys work too', '拖动调整高度 · 也支持方向键'),
          onMouseDown: startResize,
          onKeyDown: onGripKeyDown,
        }),
        h(
          'div',
          { className: 'mqtt-composer-topic' },
          h('span', { className: 'mqtt-composer-label' }, t('topic', '主题')),
          h('input', {
            className: 'mqtt-input',
            ref: topicField,
            placeholder: t('sensors/room-1/temp', 'sensors/room-1/temp'),
            value: topic,
            onChange: (event) => setTopic(event.target.value),
            onKeyDown: onTopicKeyDown,
            spellCheck: false,
            autoComplete: 'off',
            'aria-label': t('topic', '主题'),
          }),
        ),
        h('textarea', {
          className: 'mqtt-input',
          ref: payloadField,
          rows: 1,
          style: { height: height + 'px' },
          placeholder: online
            ? t('message — Enter to send, Shift+Enter for a new line', '消息 — 回车发送，Shift+回车换行')
            : disabledReason,
          value: payload,
          onChange: (event) => {
            composerDrafts.set(profile.id, event.target.value)
            setPayload(event.target.value)
            growToFit()
          },
          onKeyDown: onPayloadKeyDown,
          spellCheck: false,
          'aria-label': t('message', '消息'),
        }),
        h(
          'div',
          { className: 'mqtt-composer-foot' },
          h(
            'div',
            { className: 'mqtt-composer-opts' },
            h(Select, {
              value: qos,
              onChange: setQos,
              options: [
                { value: '0', label: t('QoS 0 · at most once', 'QoS 0 · 至多一次') },
                { value: '1', label: t('QoS 1 · at least once', 'QoS 1 · 至少一次') },
                { value: '2', label: t('QoS 2 · exactly once', 'QoS 2 · 恰好一次') },
              ],
            }),
            h(Check, { checked: retain, onChange: setRetain, label: t('retain', '保留') }),
            message ? h('span', { className: 'mqtt-hint' }, message) : null,
          ),
          h(
            Button,
            {
              variant: 'primary',
              onClick: send,
              // The native tooltip stays silent unless there is something to say:
              // a disabled button with an empty title is its own explanation.
              title: online
                ? topic.trim()
                  ? t('send (Enter)', '发送（回车）')
                  : t('a topic is required', '请填写 topic')
                : disabledReason,
              disabled: busy || !online || !topic.trim(),
            },
            busy ? t('sending…', '发送中…') : t('send', '发送'),
          ),
        ),
      )
    }

    function ClientPane({ profile, messages, run }) {
      const [busy, act, message, flash, setBusy] = useBusyAction()
      const [settingsOpen, setSettingsOpen] = useState(() =>
        typeof settingsOpenByClient[profile.id] === 'boolean' ? settingsOpenByClient[profile.id] : profile.status !== 'connected',
      )
      // The settings draft lives here, not in the drawer: folding the drawer away
      // must not drop half-typed values, and the connect button needs it.
      const [draft, setDraft] = useState(() => draftOf(profile))
      useEffect(() => {
        setDraft(draftOf(profile))
        // The draft belongs to one profile; switching selection re-seeds it.
        // eslint-disable-next-line react-hooks/exhaustive-deps
      }, [profile.id])
      const update = (patch) => setDraft((current) => ({ ...current, ...patch }))
      const reset = () => setDraft(draftOf(profile))
      const [subOpen, setSubOpen] = useState(false)
      const [subTopic, setSubTopic] = useState('')
      const [subQos, setSubQos] = useState('0')
      const online = profile.status === 'connected' || profile.status === 'connecting' || profile.status === 'reconnecting'
      const subscriptions = profile.subscriptions ?? []

      const rememberSettings = (next) => {
        settingsOpenByClient[profile.id] = next
        setSettingsOpen(next)
      }

      const toggleSettings = () => rememberSettings(!settingsOpen)

      const connect = () =>
        act(t('connecting…', '正在连接…'), async () => {
          // Connect with what the form shows: persist the draft first so a URL or
          // a credential the user just typed is what the socket actually uses.
          let latest = profile
          if (draftDiffers(profile, draft)) {
            const saved = await run('client.save', { profile: draft })
            // Only an actual profile view re-seeds the form; a host that replies
            // with an empty object must not blank the fields the user just typed.
            if (saved !== null && typeof saved === 'object' && typeof saved.id === 'string') latest = saved
          }
          await run('client.connect', { id: profile.id })
          // Re-seed from the saved profile, not from this render's closure: the
          // effect above only fires when the selection changes, so resetting from
          // `profile` would put the old URL back in the form and the next connect
          // would save it over the new one.
          setDraft(draftOf(latest))
          // Once connected the messages matter, not the form: fold the settings away.
          rememberSettings(false)
        })
      const disconnect = () => act(t('disconnected', '已断开'), () => run('client.disconnect', { id: profile.id }))

      const subscribe = async () => {
        const filter = subTopic.trim()
        if (!filter) {
          flash(t('a topic filter is required', '请填写 topic 过滤规则'))
          return
        }
        setBusy(true)
        try {
          await run('client.subscribe', { id: profile.id, topic: filter, qos: Number(subQos) })
          setSubTopic('')
          setSubOpen(false)
          flash(online ? t('subscribed', '已订阅') : t('saved for the next connect', '已保存，连接后自动订阅'))
        } catch (error) {
          flash(errorText(error))
        } finally {
          setBusy(false)
        }
      }

      const unsubscribe = (topic) =>
        run('client.unsubscribe', { id: profile.id, topic }).catch((error) => flash(errorText(error)))

      return h(
        'div',
        { className: 'mqtt-clientpane' },
        h(
          'div',
          { className: 'mqtt-clientbar' },
          h('span', { className: 'mqtt-dot ' + statusTone(profile.status) }),
          h('span', { className: 'mqtt-clientname', title: profile.name }, profile.name),
          h('span', { className: 'mqtt-sub mqtt-grow' }, profile.url),
          message ? h('span', { className: 'mqtt-hint' }, message) : null,
          h(StatusBadge, { status: profile.status, error: profile.error }),
          h(
            Button,
            { variant: 'ghost', onClick: toggleSettings, title: t('connection settings', '连接设置') },
            settingsOpen ? t('hide settings', '收起设置') : t('settings', '设置'),
          ),
          online
            ? h(Button, { onClick: disconnect, disabled: busy }, t('disconnect', '断开'))
            : h(Button, { variant: 'primary', onClick: connect, disabled: busy }, t('connect', '连接')),
        ),
        h(
          'div',
          { className: 'mqtt-subs' },
          h('span', { className: 'mqtt-subs-label' }, t('subscriptions', '订阅')),
          subscriptions.length === 0
            ? h('span', { className: 'mqtt-hint' }, t('no subscriptions yet', '还没有订阅'))
            : subscriptions.map((subscription) =>
                h(
                  'span',
                  { className: 'mqtt-chip', key: subscription.topic, title: subscription.topic },
                  h('span', { className: 'mqtt-chip-topic' }, subscription.topic),
                  h('span', { className: 'mqtt-chip-qos' }, 'q' + subscription.qos),
                  h(
                    'button',
                    {
                      type: 'button',
                      className: 'mqtt-chip-x',
                      title: t('unsubscribe', '取消订阅'),
                      'aria-label': `${t('unsubscribe', '取消订阅')} ${subscription.topic}`,
                      onClick: () => unsubscribe(subscription.topic),
                    },
                    '✕',
                  ),
                ),
              ),
          h(
            'button',
            {
              type: 'button',
              className: 'mqtt-chip add',
              onClick: () => setSubOpen((value) => !value),
              'aria-expanded': subOpen,
              title: t('subscribe to a topic', '订阅一个 topic'),
            },
            `＋ ${t('subscribe', '订阅')}`,
          ),
        ),
        subOpen
          ? h(
              'div',
              { className: 'mqtt-subform' },
              h('input', {
                className: 'mqtt-input',
                placeholder: 'sensors/#',
                value: subTopic,
                onChange: (event) => setSubTopic(event.target.value),
                'aria-label': t('topic filter (supports + and #)', 'topic 过滤规则（支持 + 和 #）'),
              }),
              h(Select, { value: subQos, onChange: setSubQos, options: [{ value: '0', label: 'q0' }, { value: '1', label: 'q1' }, { value: '2', label: 'q2' }] }),
              h(Button, { onClick: subscribe, disabled: busy }, t('add', '添加')),
              h(Button, { variant: 'ghost', onClick: () => setSubOpen(false) }, t('cancel', '取消')),
            )
          : null,
        profile.error ? h('div', { className: 'mqtt-banner err', style: { margin: '8px 12px 0' } }, h('span', null, errorText(profile.error))) : null,
        settingsOpen ? h('div', { className: 'mqtt-settings' }, h(ClientSettings, { profile, draft, update, reset, run })) : null,
        h(MessageLog, {
          entries: messages.filter((entry) => entry.kind === 'client' && entry.clientId === profile.id),
          // The message ring is shared by every connection, so clearing must name
          // the rows to drop: one connection's clear used to empty all of them.
          onClear: () => run('log.clear', { kind: 'messages', clientId: profile.id }).catch(() => {}),
          emptyText: t('messages on this connection will appear here', '这个连接收发的消息会显示在这里'),
          storageKey: `client:${profile.id}`,
        }),
        h(Composer, { profile, run, online }),
      )
    }

    function ConnectionsTab({ state, messages, run, selectedId, setSelectedId }) {
      const profiles = state.clients ?? []
      const selected = profiles.find((profile) => profile.id === selectedId) ?? null
      const [creating, setCreating] = useState(false)
      // A failed "new connection" used to be a silent no-op: the ＋ button flashed
      // nothing and the list stayed empty, so the click looked broken.
      const [createError, setCreateError] = useState(null)
      const [query, setQuery] = useState('')
      const [width, setWidth] = useState(clientListWidth)
      const dragCleanup = useRef(null)
      const needle = query.trim().toLowerCase()

      useEffect(
        () => () => {
          if (dragCleanup.current) dragCleanup.current()
        },
        [],
      )

      const add = async () => {
        setCreating(true)
        setCreateError(null)
        try {
          const created = await run('client.save', { profile: { name: t('New connection', '新连接') } })
          if (created?.id) setSelectedId(created.id)
        } catch (error) {
          setCreateError(errorText(error))
        } finally {
          setCreating(false)
        }
      }

      const startResize = (event) => {
        if (typeof event.button === 'number' && event.button !== 0) return
        event.preventDefault()
        const startX = event.clientX
        const startWidth = width
        const move = (moveEvent) => {
          const next = Math.min(520, Math.max(180, startWidth + moveEvent.clientX - startX))
          clientListWidth = next
          setWidth(next)
        }
        const finish = () => {
          if (dragCleanup.current === finish) dragCleanup.current = null
          document.removeEventListener('mousemove', move)
          document.removeEventListener('mouseup', finish)
          window.removeEventListener('blur', finish)
          document.body.style.cursor = ''
        }
        document.addEventListener('mousemove', move)
        document.addEventListener('mouseup', finish)
        // A mouseup outside the window never reaches the document, so the drag would
        // stay stuck; losing focus ends it as well.
        window.addEventListener('blur', finish)
        document.body.style.cursor = 'col-resize'
        dragCleanup.current = finish
      }

      const resizeBy = (delta) => {
        const next = Math.min(520, Math.max(180, width + delta))
        clientListWidth = next
        setWidth(next)
      }

      return h(
        'div',
        { className: 'mqtt-clients', style: { '--mqtt-list-width': width + 'px' } },
        h(
          'div',
          { className: 'mqtt-list' },
          h(
            'div',
            { className: 'mqtt-list-head' },
            h('span', { className: 'mqtt-list-title' }, t('clients', '客户端')),
            h(Button, { variant: 'ghost', onClick: add, disabled: creating, title: t('new connection', '新建连接') }, '＋'),
          ),
          h(
            'div',
            { className: 'mqtt-list-search' },
            h('input', {
              className: 'mqtt-input',
              placeholder: t('search connections', '搜索连接'),
              value: query,
              onChange: (event) => setQuery(event.target.value),
              'aria-label': t('search connections', '搜索连接'),
            }),
          ),
          profiles.length > 0
            ? h(
                'div',
                { className: 'mqtt-list-count' },
                needle
                  ? `${profiles.filter((profile) => `${profile.name} ${profile.url}`.toLowerCase().includes(needle)).length} / ${profiles.length}`
                  : String(profiles.length),
              )
            : null,
          createError ? h('div', { className: 'mqtt-banner err', style: { margin: '6px 10px 0' } }, h('span', null, errorText(createError))) : null,
          h(
            'div',
            { className: 'mqtt-list-rows' },
            profiles.length === 0
              ? h('div', { className: 'mqtt-list-empty' }, t('no connections yet', '还没有连接'))
              : profiles
                  .filter((profile) => !needle || `${profile.name} ${profile.url}`.toLowerCase().includes(needle))
                  .map((profile) =>
                  h(
                    'div',
                    {
                      key: profile.id,
                      className: profile.id === selectedId ? 'mqtt-item active' : 'mqtt-item',
                      onClick: () => setSelectedId(profile.id),
                    },
                    h('span', { className: 'mqtt-dot ' + statusTone(profile.status) }),
                    h(
                      'div',
                      { className: 'mqtt-item-text' },
                      h('div', { className: 'mqtt-item-name' }, profile.name),
                      h('div', { className: 'mqtt-item-url' }, profile.url),
                    ),
                  ),
                ),
          ),
        ),
        h('div', {
          className: 'mqtt-gutter',
          role: 'separator',
          'aria-orientation': 'vertical',
          'aria-valuenow': width,
          'aria-valuemin': 180,
          'aria-valuemax': 520,
          'aria-label': t('resize the connection list', '调整连接列表宽度'),
          tabIndex: 0,
          title: t('drag to resize (arrow keys also work)', '拖动调整宽度（方向键也可以）'),
          onMouseDown: startResize,
          onKeyDown: (event) => {
            if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return
            event.preventDefault()
            resizeBy(event.key === 'ArrowLeft' ? -16 : 16)
          },
        }),
        selected
          ? h(ClientPane, { key: selected.id, profile: selected, messages, run })
          : h(
              'div',
              { className: 'mqtt-pane' },
              h(
                'div',
                { className: 'mqtt-scroll' },
                h('div', { className: 'mqtt-empty' }, profiles.length === 0 ? t('create a connection to an MQTT broker to start.', '新建一个到 MQTT broker 的连接开始使用。') : t('select a connection', '选择一个连接')),
              ),
            ),
      )
    }

    // --------------------------------------------------------------- broker
    function BrokerTab({ state, messages, events, run, showSystem }) {
      const broker = state.broker ?? {}
      const [draft, setDraft] = useState(() => ({
        host: broker.host ?? '0.0.0.0',
        port: broker.port ?? 1883,
        autoStart: Boolean(broker.autoStart),
        allowAnonymous: broker.allowAnonymous !== false,
        maxConnections: Number.isFinite(broker.maxConnections) ? broker.maxConnections : 200,
        connectTimeoutMs: Number.isFinite(broker.connectTimeoutMs) ? broker.connectTimeoutMs : 10000,
        maxPayloadBytes: Number.isFinite(broker.maxPayloadBytes) ? broker.maxPayloadBytes : 1048576,
        maxRetainedMessages: Number.isFinite(broker.maxRetainedMessages) ? broker.maxRetainedMessages : 1000,
        protocols:
          Array.isArray(broker.protocols) && broker.protocols.length > 0 ? [...broker.protocols] : ['3.1', '3.1.1', '5.0'],
        users: (broker.users ?? []).map((user) => ({ username: user.username, password: '' })),
      }))
      const [busy, act, message, flash, setBusy] = useBusyAction()

      useEffect(() => {
        setDraft({
          host: broker.host ?? '0.0.0.0',
          port: broker.port ?? 1883,
          autoStart: Boolean(broker.autoStart),
          allowAnonymous: broker.allowAnonymous !== false,
          maxConnections: Number.isFinite(broker.maxConnections) ? broker.maxConnections : 200,
          connectTimeoutMs: Number.isFinite(broker.connectTimeoutMs) ? broker.connectTimeoutMs : 10000,
          maxPayloadBytes: Number.isFinite(broker.maxPayloadBytes) ? broker.maxPayloadBytes : 1048576,
          maxRetainedMessages: Number.isFinite(broker.maxRetainedMessages) ? broker.maxRetainedMessages : 1000,
          protocols:
            Array.isArray(broker.protocols) && broker.protocols.length > 0 ? [...broker.protocols] : ['3.1', '3.1.1', '5.0'],
          users: (broker.users ?? []).map((user) => ({ username: user.username, password: '' })),
        })
        // Re-seed when the host reports different persisted settings.
        // eslint-disable-next-line react-hooks/exhaustive-deps
      }, [broker.host, broker.port, broker.autoStart, broker.allowAnonymous, JSON.stringify(broker.protocols ?? []), JSON.stringify(broker.users ?? []),
        broker.maxConnections,
        broker.connectTimeoutMs,
        broker.maxPayloadBytes,
        broker.maxRetainedMessages,
      ])

      const update = (patch) => setDraft((current) => ({ ...current, ...patch }))
      const toggleProtocol = (value, enabled) => {
        const next = enabled
          ? Array.from(new Set(draft.protocols.concat(value)))
          : draft.protocols.filter((item) => item !== value)
        // Never let the last version be switched off; the broker has to speak one.
        if (next.length > 0) update({ protocols: next })
      }
      const running = broker.status === 'running' || broker.status === 'starting'
      const saving = broker.status === 'running'
      // A running server starts folded so its messages and the send card have the
      // room; the choice is remembered for the session like the clients' drawer.
      const [settingsOpen, setSettingsOpen] = useState(() =>
        typeof brokerSettingsOpen === 'boolean' ? brokerSettingsOpen : broker.status !== 'running',
      )
      const rememberSettings = (next) => {
        brokerSettingsOpen = next
        setSettingsOpen(next)
      }
      const toggleSettings = () => rememberSettings(!settingsOpen)

      const savePayload = () => ({
        broker: {
          host: draft.host,
          port: Number(draft.port),
          autoStart: draft.autoStart,
          allowAnonymous: draft.allowAnonymous,
          protocols: draft.protocols,
          maxConnections: Number(draft.maxConnections),
          connectTimeoutMs: Number(draft.connectTimeoutMs),
          maxPayloadBytes: Number(draft.maxPayloadBytes),
          maxRetainedMessages: Number(draft.maxRetainedMessages),
          users: draft.users,
        },
      })
      const save = () => act(t('saved — restart the broker to apply', '已保存 — 重启 broker 后生效'), () => run('broker.save', savePayload()))
      // "Start" runs what the drawer shows: save the draft first, then bind the
      // port. Starting on the stored settings would ignore every edit made here.
      const start = () =>
        act(t('broker started', 'broker 已启动'), async () => {
          await run('broker.save', savePayload())
          await run('broker.start')
          // Started: the configuration is out of the way, messages matter now.
          rememberSettings(false)
        })
      const stop = () => act(t('broker stopped', 'broker 已停止'), () => run('broker.stop'))
      // Clearing is one MQTT empty retained publish per topic; the host does the
      // loop so a user does not have to remember which topics hold a value.
      const clearRetained = () =>
        act(null, async () => {
          const result = await run('broker.clearRetained')
          const cleared = Number(result?.cleared ?? 0)
          flash(cleared === 0 ? t('no retained messages to clear', '没有需要清除的保留消息') : t('retained messages cleared', '已清除保留消息'))
        })

      const setUser = (index, patch) =>
        update({ users: draft.users.map((user, position) => (position === index ? { ...user, ...patch } : user)) })

      return h(
        'div',
        { className: 'mqtt-pane' },
        h(
          'div',
          { className: 'mqtt-clientbar' },
          h('span', { className: 'mqtt-dot ' + statusTone(broker.status) }),
          h('span', { className: 'mqtt-clientname' }, t('Embedded broker', '内嵌 Broker')),
          h(
            'span',
            { className: 'mqtt-sub mqtt-grow' },
            running
              ? `${broker.host}:${broker.boundPort ?? broker.port} · ${t('clients', '客户端')} ${broker.clientCount ?? 0} · ${formatDuration(broker.startedAt)}`
              : t('the local MQTT server is stopped', '本地 MQTT 服务端当前未启动'),
          ),
          message ? h('span', { className: 'mqtt-hint' }, message) : null,
          h(StatusBadge, { status: broker.status, error: broker.error }),
          h(
            Button,
            { variant: 'ghost', onClick: toggleSettings, title: t('server settings', '服务器设置') },
            settingsOpen ? t('hide settings', '收起设置') : t('settings', '设置'),
          ),
          running
            ? h(Button, { onClick: stop, disabled: busy }, t('stop', '停止'))
            : h(Button, { variant: 'primary', onClick: start, disabled: busy }, t('start', '启动')),
        ),
        broker.error ? h('div', { className: 'mqtt-banner err', style: { margin: '10px 12px 0' } }, h('span', null, errorText(broker.error))) : null,
        broker.restartHint && saving
          ? h(
              'div',
              { className: 'mqtt-banner', style: { margin: '10px 12px 0' } },
              h('span', null, t('settings changed — restart the broker for them to take effect', '设置已修改 — 重启 broker 后生效')),
            )
          : null,
        settingsOpen
          ? h(
              'div',
              { className: 'mqtt-settings' },
              h(
                'div',
                { className: 'mqtt-split' },
            h(
              'div',
              null,
              h(
                'div',
                { className: 'mqtt-section' },
                h('div', { className: 'mqtt-section-title' }, t('listener', '监听设置')),
                h(
                  'div',
                  { className: 'mqtt-grid' },
                  h(Field, { label: t('bind host', '监听地址') }, h(TextInput, { value: draft.host, onChange: (value) => update({ host: value }) })),
                  h(Field, { label: t('port', '端口') }, h(NumberInput, { value: draft.port, onChange: (value) => update({ port: value }), min: 1, max: 65535 })),
                  h(
                    Field,
                    { label: t('options', '选项'), wide: true },
                    h(
                      'div',
                      { className: 'mqtt-row' },
                      h(Check, { checked: draft.allowAnonymous, onChange: (value) => update({ allowAnonymous: value }), label: t('allow anonymous clients', '允许匿名连接') }),
                      h(Check, { checked: draft.autoStart, onChange: (value) => update({ autoStart: value }), label: t('start with DSH', '随 DSH 启动') }),
                    ),
                  ),
                ),
              ),
              h(
                'div',
                { className: 'mqtt-section' },
                h('div', { className: 'mqtt-section-title' }, t('protocol versions', '协议版本')),
                h(
                  'div',
                  { className: 'mqtt-row' },
                  [
                    { value: '3.1', label: 'MQTT 3.1' },
                    { value: '3.1.1', label: 'MQTT 3.1.1' },
                    { value: '5.0', label: 'MQTT 5.0' },
                  ].map((choice) =>
                    h(Check, {
                      key: choice.value,
                      checked: draft.protocols.includes(choice.value),
                      onChange: (value) => toggleProtocol(choice.value, value),
                      label: choice.label,
                    }),
                  ),
                ),
                h(
                  'div',
                  { className: 'mqtt-hint' },
                  t(
                    'MQTT 5.0 clients are translated to 3.1.1 sessions internally; a version you uncheck is refused with a CONNECT error. Restart the broker to apply.',
                    'MQTT 5.0 客户端在内部会转换为 3.1.1 会话；取消勾选的版本会收到 CONNECT 拒绝。修改后需重启 broker。',
                  ),
                ),
              ),
              h(
                'div',
                { className: 'mqtt-section' },
                h('div', { className: 'mqtt-section-title' }, t('resource limits', '资源上限')),
                h(
                  'div',
                  { className: 'mqtt-grid' },
                  h(
                    Field,
                    { label: t('max connections (0 = unlimited)', '最大连接数（0 = 不限）') },
                    h(NumberInput, { value: draft.maxConnections, onChange: (value) => update({ maxConnections: value }), min: 0, max: 5000 }),
                  ),
                  h(
                    Field,
                    { label: t('CONNECT timeout (ms)', 'CONNECT 超时（毫秒）') },
                    h(NumberInput, { value: draft.connectTimeoutMs, onChange: (value) => update({ connectTimeoutMs: value }), min: 500, max: 120000, step: 500 }),
                  ),
                  h(
                    Field,
                    { label: t('max payload (bytes)', '单条消息上限（字节）') },
                    h(NumberInput, { value: draft.maxPayloadBytes, onChange: (value) => update({ maxPayloadBytes: value }), min: 1024, max: 16777216, step: 1024 }),
                  ),
                  h(
                    Field,
                    { label: t('max retained messages (0 = unlimited)', '保留消息上限（0 = 不限）') },
                    h(NumberInput, {
                      value: draft.maxRetainedMessages,
                      onChange: (value) => update({ maxRetainedMessages: value }),
                      min: 0,
                      max: 100000,
                    }),
                  ),
                ),
                h(
                  'div',
                  { className: 'mqtt-hint' },
                  t(
                    'Limits keep a runaway client from exhausting DSH: over-budget publishes are refused and oversized clients are disconnected with a broker log entry. Connection and CONNECT-timeout changes apply after a broker restart; payload and retained limits apply live.',
                    '这些上限防止异常客户端拖垮 DSH：超限的发布会收到拒绝，超大的客户端会被断开并写入 Broker 日志。连接数与 CONNECT 超时修改后需重启 Broker；消息大小与保留消息上限实时生效。',
                  ),
                ),
              ),
              h(
                'div',
                { className: 'mqtt-actions' },
                h(Button, { variant: 'primary', onClick: save, disabled: busy }, t('save settings', '保存设置')),
                h(Check, { checked: showSystem, onChange: (value) => run('settings.save', { showSystem: value }).catch(() => {}), label: t('show $SYS messages', '显示 $SYS 消息') }),
                broker.status === 'running' && Number(broker.retainedCount ?? 0) > 0
                  ? h(
                      Button,
                      { variant: 'ghost', onClick: clearRetained, disabled: busy, title: t('drop every retained message', '清空所有保留消息') },
                      t('retained', '保留消息') + ` (${broker.retainedCount})`,
                    )
                  : null,
              ),
            ),
            h(
              'div',
              null,
              h(
                'div',
                { className: 'mqtt-section' },
                h('div', { className: 'mqtt-section-title' }, `${t('connected clients', '在线客户端')} (${broker.clientCount ?? 0})`),
                (broker.clients ?? []).length === 0
                  ? h('div', { className: 'mqtt-empty' }, t('no clients connected', '暂无客户端连接'))
                  : h(
                      'table',
                      { className: 'mqtt-table' },
                      h('thead', null, h('tr', null, h('th', null, t('client', '客户端')), h('th', null, t('remote', '来源')), h('th', null, t('subscriptions', '订阅')))),
                      h(
                        'tbody',
                        null,
                        (broker.clients ?? []).map((client) =>
                          h(
                            'tr',
                            { key: client.id },
                            h('td', { className: 'mono' }, client.id, client.clean ? h('span', { className: 'mqtt-badge' }, 'clean') : null),
                            h('td', { className: 'mono' }, client.remote || '—'),
                            h('td', { className: 'mono' }, (client.subscriptions ?? []).join(', ') || '—'),
                          ),
                        ),
                      ),
                    ),
              ),
              h(
                'div',
                { className: 'mqtt-section' },
                h('div', { className: 'mqtt-section-title' }, t('users', '用户')),
                draft.users.length === 0
                  ? h('div', { className: 'mqtt-hint' }, t('no users — anonymous access only', '没有用户 — 仅允许匿名连接'))
                  : draft.users.map((user, index) => {
                      // Match the saved row by username, not by position: deleting a
                      // row shifts every later index and the password placeholder
                      // would claim someone else's saved password.
                      const savedUser = (broker.users ?? []).find((saved) => saved.username === user.username)
                      return h(
                        'div',
                        { className: 'mqtt-sub-row', key: index },
                        h('input', {
                          className: 'mqtt-input',
                          style: { flex: 1, minWidth: '90px' },
                          placeholder: t('username', '用户名'),
                          value: user.username,
                          onChange: (event) => setUser(index, { username: event.target.value }),
                        }),
                        h('input', {
                          className: 'mqtt-input',
                          style: { flex: 1, minWidth: '90px' },
                          type: 'password',
                          placeholder: savedUser?.hasPassword ? t('saved — empty keeps it', '已保存 — 留空保持不变') : t('password', '密码'),
                          value: user.password,
                          onChange: (event) => setUser(index, { password: event.target.value }),
                        }),
                        h(Button, { variant: 'ghost', onClick: () => update({ users: draft.users.filter((_, position) => position !== index) }) }, '✕'),
                      )
                    }),
                h(Button, { variant: 'ghost', onClick: () => update({ users: draft.users.concat({ username: '', password: '' }) }) }, t('＋ add user', '＋ 添加用户')),
                draft.users.length > 0
                  ? h(
                      'div',
                      { className: 'mqtt-hint' },
                      t('an empty password keeps the saved one; rows without a username are ignored when saving', '密码留空表示沿用已保存的密码；没有用户名的行在保存时会被忽略'),
                    )
                  : null,
                // What makes the list look dead: anonymous access accepts
                // everyone, so a running broker never asks for a name.
                h(
                  'div',
                  { className: 'mqtt-hint' },
                  draft.allowAnonymous
                    ? t(
                        'anonymous access is on, so this list is not checked: turn "allow anonymous clients" off, then save and restart the broker to require a username and password',
                        '允许匿名连接已开启，因此这份用户名单不会被校验：关闭「允许匿名连接」后保存并重启 broker，连接才需要用户名和密码',
                      )
                    : t(
                        'the list is checked on CONNECT: save and restart the broker to apply changes',
                        '用户名单在 CONNECT 时校验：保存后重启 broker 才生效',
                      ),
                ),
              ),
              h(
                'div',
                { className: 'mqtt-section' },
                h(
                  'div',
                  { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px' } },
                  h('div', { className: 'mqtt-section-title' }, t('broker events', 'Broker 事件')),
                  (events ?? []).length > 0
                    ? h(
                        Button,
                        {
                          variant: 'ghost',
                          onClick: () => run('log.clear', { kind: 'events' }).catch(() => {}),
                          title: t('clear broker events', '清空 Broker 事件'),
                        },
                        t('clear', '清空'),
                      )
                    : null,
                ),
                (events ?? []).length === 0
                  ? h('div', { className: 'mqtt-empty' }, t('connection and subscription events appear here', '连接与订阅事件会显示在这里'))
                  : h(
                      'div',
                      { style: { maxHeight: '220px', overflow: 'auto' } },
                      (events ?? []).slice().reverse().map((entry) => h(EventRow, { key: entry.seq, entry })),
                    ),
              ),
            ),
              ),
            )
          : null,
        h(MessageLog, {
          entries: messages.filter((entry) => entry.kind === 'broker'),
          onClear: () => run('log.clear', { kind: 'messages', scope: 'broker' }).catch(() => {}),
          emptyText: t('messages published through the broker will appear here', '经过 broker 的消息会显示在这里'),
          storageKey: 'broker',
        }),
        // The send bar sits below the log, exactly where the clients put theirs.
        h(Publisher, { onPublish: (input) => run('broker.publish', input), disabledReason: running ? null : t('start the broker first', '请先启动 broker') }),
      )
    }

    // ---------------------------------------------------------------- panel
    // The slot can remount this view when the harness rebuilds; remembering the
    // tab here keeps the user where they were instead of snapping back.
    let lastTab = 'clients'
    let clientListWidth = 252
    const composerTopics = new Map()
    // Per-connection send options: the second message to the same endpoint
    // usually wants the same QoS and retain as the first one.
    const composerQos = new Map()
    const composerRetain = new Map()
    // Half-typed messages survive a tab switch: the pane is unmounted when the
    // other tab is shown, and losing the draft there is a data-loss surprise.
    const composerDrafts = new Map()
    const brokerPublisherDraft = { topic: '', payload: '' }
    // Filter, direction and the pause switch survive a tab round-trip the same
    // way the draft does; only the pane, not the user's settings, goes away.
    const logViewState = new Map()
    const COMPOSER_MIN = 44
    const COMPOSER_START = 64
    const COMPOSER_MAX = 360
    // The height the user picked for the message box. Shared across
    // connections and remembered for the session, like the list width.
    let composerHeight = COMPOSER_START
    // Auto-grow only until the user drags the grip; after that the drag wins
    // and the text scrolls inside the box they chose.
    let composerHeightAuto = true
    const settingsOpenByClient = {}
    // The broker pane keeps its configuration in the same kind of foldable
    // drawer; null means "not chosen yet" so a running server starts collapsed.
    let brokerSettingsOpen = null
    const PUBLISHER_MIN = 64
    const PUBLISHER_START = 64
    // Auto-grow stops here; a drag may go further, but never eats the log.
    const PUBLISHER_GROW_LIMIT = 220
    const PUBLISHER_MAX = 320
    // Same contract as the clients' composer: the height the user picked for the
    // broker's message box survives a tab switch for the session.
    let publisherHeight = PUBLISHER_START
    let publisherHeightAuto = true

    // Navigation back to the Conversation lives in the client layout service.
    // Resolve it lazily: the service registry is ready before this module is
    // evaluated in practice, but a plugin reload must never leave the button
    // dead, so the lookup is retried on click as well.
    let pluginContext = null
    let layoutService = null
    function resolveLayout() {
      if (layoutService) return layoutService
      try {
        if (pluginContext && typeof pluginContext.get === 'function') {
          const layout = pluginContext.get('layout')
          if (layout && typeof layout.selectPanel === 'function') layoutService = layout
        }
      } catch {
        /* the layout service is optional */
      }
      return layoutService
    }
    function backToConversation() {
      const layout = resolveLayout()
      if (!layout) return
      try {
        layout.selectPanel(null)
      } catch {
        /* a released panel must not break the click handler */
      }
    }

    class PanelBoundary extends React.Component {
      constructor(props) {
        super(props)
        this.state = { error: null }
      }

      static getDerivedStateFromError(error) {
        return { error }
      }

      componentDidCatch(error, info) {
        console.error('[dsh-mqtt-tools] panel failed to render', error, info)
      }

      render() {
        const error = this.state.error
        if (error) {
          return h(
            'div',
            { className: 'mqtt-pane' },
            h(
              'div',
              { className: 'mqtt-scroll' },
              h('div', { className: 'mqtt-banner err' }, h('span', null, t('the MQTT panel failed to render', 'MQTT 面板渲染出错'))),
              h('pre', { className: 'mqtt-error' }, String((error && error.stack) || error)),
            ),
          )
        }
        return this.props.children
      }
    }

    function Panel() {
      useLang()
      const scheme = useScheme()
      const { state, messages, events, live, fatal, run } = useMqttApi()
      const [tab, setTabState] = useState(lastTab)
      const setTab = (value) => {
        lastTab = value
        setTabState(value)
      }
      const [selectedId, setSelectedId] = useState(null)

      useEffect(() => {
        if (state === null) return
        const profiles = state.clients ?? []
        if (profiles.length === 0) {
          if (selectedId !== null) setSelectedId(null)
          return
        }
        if (!profiles.some((profile) => profile.id === selectedId)) setSelectedId(profiles[0].id)
        // eslint-disable-next-line react-hooks/exhaustive-deps
      }, [state])

      const broker = state?.broker ?? null
      // Only a real import failure is worth a warning: the host loads both
      // packages in the background, so an unloaded flag just means "not yet".
      const missing = state !== null && Boolean(state.library?.error)

      if (state === null) {
        return h(
          'div',
          { className: 'mqtt-wrap', 'data-dsh-plugin': 'mqtt' },
          h('style', null, CSS),
          h('div', { className: 'mqtt-empty', style: { marginTop: '40px' } }, fatal ? fatal : t('loading MQTT…', '正在加载 MQTT…')),
        )
      }

      const connected = (state.clients ?? []).filter((profile) => profile.status === 'connected').length

      return h(
        'div',
        { className: 'mqtt-wrap', 'data-dsh-plugin': 'mqtt' },
        h('style', null, CSS),
        h(
          'div',
          { className: scheme === 'dark' ? 'mqtt-root mqtt-scheme-dark' : 'mqtt-root' },
          h(
            'div',
            { className: 'mqtt-head' },
            h(
              'button',
              {
                type: 'button',
                className: 'mqtt-back',
                title: t('Back to the conversation', '返回会话'),
                'aria-label': t('Back to the conversation', '返回会话'),
                onClick: backToConversation,
              },
              h('span', null, t('< Back to chat', '<返回会话')),
            ),
            h('div', { className: 'mqtt-title' }, 'MQTT'),
            h(
              'div',
              { className: 'mqtt-grow mqtt-row' },
              h('span', { className: `mqtt-badge ${connected > 0 ? 'on' : ''}` }, h('span', { className: `mqtt-dot ${connected > 0 ? 'on' : 'off'}` }), t('connected', '已连接'), ' ', connected),
              h(
                'span',
                {
                  className: `mqtt-badge ${statusTone(broker?.status)}`,
                  title: broker?.error ? errorText(broker.error) : undefined,
                },
                h('span', { className: `mqtt-dot ${statusTone(broker?.status)}` }),
                'Broker',
                broker?.status === 'error' || broker?.error
                  ? t('error', '出错')
                  : broker?.status === 'running'
                    ? ` :${broker.boundPort ?? broker.port}`
                    : broker?.status === 'starting'
                      ? t('starting…', '启动中…')
                      : t('stopped', '已停止'),
              ),
            ),
            h(
              'span',
              { className: 'mqtt-live', title: state.stateFile || '' },
              h('span', { className: `mqtt-dot ${live ? 'on' : 'warn'}` }),
              live ? t('live', '实时') : t('reconnecting', '重连中'),
            ),
          ),
          missing
            ? h(
                'div',
                { className: 'mqtt-banner err', style: { margin: '8px 16px 0' } },
                h(
                  'span',
                  null,
                  t('the MQTT library could not be loaded', 'MQTT 依赖库加载失败'),
                  ': ',
                  String(state.library?.error ?? ''),
                  ' ',
                  t('— run npm install in the plugin directory.', '— 请在插件目录执行 npm install。'),
                ),
              )
            : null,
          h(
            'div',
            { className: 'mqtt-tabs', role: 'tablist' },
            h(
              'button',
              {
                type: 'button',
                role: 'tab',
                'aria-selected': tab === 'clients',
                className: tab === 'clients' ? 'mqtt-tab active' : 'mqtt-tab',
                onClick: () => setTab('clients'),
              },
              t('Clients', '客户端'),
            ),
            h(
              'button',
              {
                type: 'button',
                role: 'tab',
                'aria-selected': tab === 'broker',
                className: tab === 'broker' ? 'mqtt-tab active' : 'mqtt-tab',
                onClick: () => setTab('broker'),
              },
              t('Broker', '服务端'),
            ),
          ),
          h(
            PanelBoundary,
            // A fresh boundary per tab: a failed render must not stick around
            // after the user switches to the other view.
            { key: tab },
            tab === 'clients'
              ? h(ConnectionsTab, { state, messages, run, selectedId, setSelectedId })
              : h(BrokerTab, { state, messages, events, run, showSystem: Boolean(state.settings?.showSystem) }),
          ),
        ),
      )
    }

    // --------------------------------------------------------------- sidebar
    function PanelIcon({ size }) {
      return h(
        'svg',
        {
          'data-dsh-panel-entry': 'mqtt',
          viewBox: '0 0 16 16',
          width: size,
          height: size,
          fill: 'none',
          stroke: 'currentColor',
          strokeWidth: '1.5',
          strokeLinecap: 'round',
          strokeLinejoin: 'round',
          'aria-hidden': 'true',
        },
        h('circle', { cx: '8', cy: '8', r: '1.7', fill: 'currentColor', stroke: 'none' }),
        h('path', { d: 'M5.1 10.9a4.1 4.1 0 0 1 0-5.8' }),
        h('path', { d: 'M10.9 5.1a4.1 4.1 0 0 1 0 5.8' }),
        h('path', { d: 'M2.6 13.4a7.6 7.6 0 0 1 0-10.8' }),
        h('path', { d: 'M13.4 2.6a7.6 7.6 0 0 1 0 10.8' }),
      )
    }

    // ---------------------------------------------------------------- plugin
    const plugin = {
      inject: ['slots'],
      apply(ctx) {
        const slots = ctx.slots
        const disposers = []
        pluginContext = ctx
        resolveLayout()
        // Follow the harness locale: it is authoritative, and it may be set
        // after this module is evaluated.
        try {
          const locale = typeof ctx.get === 'function' ? ctx.get('locale') : null
          if (locale && typeof locale.getLocale === 'function') {
            const current = locale.getLocale()
            if (typeof current === 'string' && current) localeLang = current
          }
          if (locale && typeof locale.subscribe === 'function') {
            const disposeLocale = locale.subscribe(() => {
              try {
                if (typeof locale.getLocale === 'function') {
                  const current = locale.getLocale()
                  if (typeof current === 'string') localeLang = current
                }
              } catch {
                /* fall back to the document language */
              }
              notifyLang()
            })
            if (typeof disposeLocale === 'function') disposers.push(disposeLocale)
          }
        } catch {
          /* the locale service is optional */
        }
        try {
          const theme = typeof ctx.get === 'function' ? ctx.get('theme') : null
          if (theme && typeof theme.getTheme === 'function') {
            themeService = theme
            const syncScheme = () => {
              try {
                schemeFromSnapshot(theme.getTheme())
              } catch {
                /* the theme service may be mid-switch */
              }
            }
            syncScheme()
            if (typeof ctx.on === 'function') {
              const disposeTheme = ctx.on('theme/change', syncScheme)
              if (typeof disposeTheme === 'function') disposers.push(disposeTheme)
            }
          } else {
            schemeFromDocument()
          }
        } catch {
          schemeFromDocument()
        }
        try {
          if (typeof MutationObserver === 'function' && typeof document !== 'undefined' && document.documentElement) {
            const observer = new MutationObserver(() => {
              notifyLang()
              schemeFromDocument()
            })
            observer.observe(document.documentElement, {
              attributes: true,
              attributeFilter: ['lang', 'data-locale', 'data-lang', 'class', 'data-theme', 'style'],
            })
            disposers.push(() => observer.disconnect())
          }
        } catch {
          /* no DOM mutation observer available */
        }
        notifyLang()
        disposers.push(
          slots.inject('sidebar.panellist', () =>
            slots.register({ name: 'sidebar.panellist', id: 'mqtt', order: PANEL_ORDER, label: () => 'MQTT' }, PanelIcon),
          ),
        )
        disposers.push(slots.inject('main', () => slots.register({ name: 'main', key: 'mqtt' }, Panel)))
        ctx.effect(
          () => () => {
            for (const dispose of disposers.splice(0)) {
              try {
                dispose()
              } catch {
                /* ignore a disposal failure */
              }
            }
          },
          'dsh-mqtt-tools: panel',
        )
      },
    }
    // Test-only surface used by scripts/render-check.mjs; the loader only
    // reads `inject` and `apply`, and this key is absent in normal loads.
    if (globalThis.__DSH_MQTT_TEST__ === true) {
      plugin.__components = { Panel, ConnectionsTab, ClientPane, BrokerTab, MessageLog, Publisher, PanelIcon }
    }
    return plugin
  },
})
