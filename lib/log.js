/** Bounded, sequence-numbered logs shared by the client and broker halves. */
import { isUtf8 } from 'node:buffer'

/** How much of one payload is decoded, and how many characters are kept, for display. */
const PREVIEW_BYTES = 64 * 1024
const PREVIEW_CHARS = 8192

/**
 * Re-materialise a prefix as a fresh, flat string. `String.prototype.slice` returns
 * a V8 SlicedString that keeps its whole parent alive, so slicing a multi-hundred-
 * kilobyte payload would pin the entire decoded payload in the ring log forever.
 */
function flatten(text, length) {
  return Buffer.from(text.slice(0, length), 'utf8').toString('utf8')
}

/** Render one MQTT payload as text the page can show, never trusting it. */
export function describePayload(payload, maxChars = PREVIEW_CHARS) {
  const buffer = Buffer.isBuffer(payload)
    ? payload
    : payload instanceof Uint8Array
      ? Buffer.from(payload)
      : Buffer.from(payload === undefined || payload === null ? '' : String(payload), 'utf8')
  const bytes = buffer.byteLength
  if (bytes === 0) return { payload: '', encoding: 'utf8', bytes: 0 }
  const limit = Number.isFinite(maxChars) && maxChars > 0 ? Math.trunc(maxChars) : PREVIEW_CHARS
  // Decode only a bounded prefix: a 512 KiB binary payload would otherwise build a
  // ~700 KiB base64 string that the ring keeps alive and the page never shows.
  const capped = bytes > PREVIEW_BYTES ? buffer.subarray(0, PREVIEW_BYTES) : buffer
  const binary = !isUtf8(capped)
  const encoding = binary ? 'base64' : 'utf8'
  let text = capped.toString(encoding)
  if (text.length > limit) {
    // Keep base64 alignable: a partial group would decode to garbage for anyone
    // who copies the preview out.
    const cut = binary ? Math.max(4, 4 * Math.floor(limit / 4)) : limit
    text = `${flatten(text, cut)}…`
  }
  return { payload: text, encoding, bytes }
}

/** Turn an arbitrary value into one bounded line of text. */
export function oneLine(value, max = 300) {
  let text
  try {
    text = typeof value === 'string' ? value : String(value?.message ?? value ?? '')
  } catch {
    text = ''
  }
  text = text.replace(/\s+/g, ' ').trim()
  return text.length > max ? `${text.slice(0, max)}…` : text
}

/** A bounded ring of entries, each carrying a monotonically increasing `seq`. */
export class RingLog {
  constructor(limit = 800) {
    this.limit = limit
    this.items = []
    this.seq = 0
  }

  push(entry) {
    this.seq += 1
    const item = { seq: this.seq, ts: Date.now(), ...entry }
    this.items.push(item)
    const overflow = this.items.length - this.limit
    if (overflow > 0) this.items.splice(0, overflow)
    return item
  }

  tail(count = 200) {
    return this.items.slice(Math.max(0, this.items.length - count))
  }

  /** Entries newer than `seq`, at most `count` of them (the newest). */
  since(seq, count = 300) {
    const newer = this.items.filter((item) => item.seq > seq)
    return newer.length > count ? newer.slice(newer.length - count) : newer
  }

  /** Drop every entry, or only the ones `match` selects. */
  clear(match = null) {
    this.items = match === null ? [] : this.items.filter((item) => !match(item))
  }
}
