// Runner: executes every scripts/suite/*.test.mjs sequentially and prints one table.
//
//   node scripts/suite/run-all.mjs                 # everything
//   node scripts/suite/run-all.mjs protocol ui     # suites whose filename contains one of the words
//
// Each suite prints its own log and a last line `SUITE-RESULT {json}`; the runner
// parses that line so a crash (missing result) is also visible as a failure.
import { spawn } from 'node:child_process';
import { readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const PLUGIN_DIR = dirname(dirname(HERE));
const filters = process.argv.slice(2).filter((arg) => !arg.startsWith('-'));
const verbose = !process.argv.includes('--quiet');

const files = readdirSync(HERE)
  .filter((file) => file.endsWith('.test.mjs'))
  .filter((file) => filters.length === 0 || filters.some((filter) => file.includes(filter)))
  .sort();

if (files.length === 0) {
  console.error(`no suite files matched${filters.length ? ` [${filters.join(', ')}]` : ''}`);
  process.exit(2);
}

function run(file) {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const child = spawn(process.execPath, [join(HERE, file)], {
      cwd: PLUGIN_DIR,
      env: { ...process.env },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '';
    const onData = (chunk) => {
      const text = String(chunk);
      output += text;
      if (verbose) process.stdout.write(text);
    };
    child.stdout.on('data', onData);
    child.stderr.on('data', onData);
    child.on('close', (code) => {
      const line = output
        .split(/\r?\n/)
        .reverse()
        .find((row) => row.startsWith('SUITE-RESULT '));
      let result = null;
      if (line) {
        try {
          result = JSON.parse(line.slice('SUITE-RESULT '.length));
        } catch {
          result = null;
        }
      }
      resolve({ file, code, result, ms: Date.now() - startedAt, output });
    });
  });
}

const results = [];
for (const file of files) {
  console.log(`\n########## ${file}`);
  results.push(await run(file));
}

let failed = 0;
let skipped = 0;
let passed = 0;
console.log('\n================ SUMMARY');
for (const { file, code, result, ms } of results) {
  const name = file.replace(/\.test\.mjs$/, '');
  if (!result) {
    failed += 1;
    console.log(`  CRASH ${name} (exit ${code}, ${(ms / 1000).toFixed(1)} s) — no SUITE-RESULT line`);
    continue;
  }
  passed += result.passed;
  failed += result.failed;
  skipped += result.skipped;
  const status = result.failed > 0 ? 'FAIL' : 'PASS';
  console.log(`  ${status} ${name}: ${result.passed} passed, ${result.failed} failed, ${result.skipped} skipped (${(ms / 1000).toFixed(1)} s)`);
  for (const failure of result.failures) console.log(`         - ${failure.name}: ${failure.message}`);
  for (const skip of result.skips) console.log(`         ~ ${skip.name}: ${skip.reason}`);
}
console.log(`\nTOTAL ${passed} passed, ${failed} failed, ${skipped} skipped across ${results.length} suites`);
process.exitCode = failed > 0 ? 1 : 0;
