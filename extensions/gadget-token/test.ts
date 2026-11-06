// Tests for the gadget-token extension. Run with:
//   bun extensions/gadget-token/test.ts
// Safe by design: uses temp credential directories and a mock fetch.
// It never touches the real credential directory and never hits the
// real Muse API, and it never prints a token value.

import { mkdtemp, writeFile, readFile, stat, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import {
  MuseAccount,
  loadCredentials,
  saveCredentials,
  unpair,
  validateSDKToken,
  SDK_TOKEN_URL,
} from './src/index.js';

let passed = 0;
let failed = 0;
function check(name: string, cond: boolean) {
  if (cond) { passed++; console.log(`PASS ${name}`); }
  else { failed++; console.log(`FAIL ${name}`); }
}

const dir = await mkdtemp(join(tmpdir(), 'gadget-token-test-'));
try {
  // --- validateSDKToken ---
  check('sdk-token: url is the browser token page', SDK_TOKEN_URL === 'https://gadgets.muse.ai/settings/sdk-tokens');
  const good = 'mgst_' + 'A'.repeat(42) + 'A';
  check('sdk-token: valid format accepted', validateSDKToken(good) === good);
  check('sdk-token: valid format with whitespace trimmed', validateSDKToken(`  ${good}\n`) === good);
  for (const bad of ['not-a-token', 'mgst_short', '', 'mgst_' + 'A'.repeat(41) + 'A', 'mgst_' + 'A'.repeat(43) + 'A']) {
    let threw = false;
    try { validateSDKToken(bad); } catch { threw = true; }
    check(`sdk-token: rejects ${JSON.stringify(bad.slice(0, 20))}`, threw);
  }
  // Charset: the last char is constrained to [AEIMQUYcgkosw048] and the
  // middle to [A-Za-z0-9_-]; a valid-length token with a bad last char
  // or a bad middle char must be rejected.
  check('sdk-token: rejects valid-length token with bad last char', (() => {
    try { validateSDKToken('mgst_' + 'A'.repeat(42) + 'B'); return false; } catch { return true; }
  })());
  check('sdk-token: rejects token with invalid middle char', (() => {
    try { validateSDKToken('mgst_' + 'A'.repeat(20) + '!' + 'A'.repeat(21) + 'A'); return false; } catch { return true; }
  })());

  // --- credentials load/save round-trip in a temp dir ---
  await writeFile(join(dir, 'identity.json'), JSON.stringify({ mac: 'aa:bb:cc:dd:ee:ff' }), { mode: 0o600 });
  await writeFile(join(dir, 'pairing.json'), JSON.stringify({
    access_token: 'access-1', refresh_token: 'refresh-1', token_type: 'device',
    api_url_v2: '', noise_host: '', access_token_saved_at: 1000, sdk_token_hint: 'mgst_ABCDEFG',
  }), { mode: 0o600 });
  await writeFile(join(dir, 'sdk_token'), good + '\n', { mode: 0o600 });
  const loaded = await loadCredentials(dir);
  check('credentials: load reads tokens + device id', loaded.accessToken === 'access-1' && loaded.refreshToken === 'refresh-1' && loaded.deviceId === 'homelink-ddeeff');
  check('credentials: load reads sdk token + hint', loaded.sdkToken === good && loaded.sdkTokenHint === 'mgst_ABCDEFG');
  check('credentials: load reads savedAt', loaded.savedAt === 1000);
  check('credentials: pairing.json written 0600', ((await stat(join(dir, 'pairing.json'))).mode & 0o777) === 0o600);
  check('credentials: directory is 0700', ((await stat(dir)).mode & 0o777) === 0o700);

  // --- error paths: invalid MAC, missing files ---
  const badMacDir = await mkdtemp(join(tmpdir(), 'gadget-token-badmac-'));
  try {
    await writeFile(join(badMacDir, 'identity.json'), JSON.stringify({ mac: 'not-a-mac' }));
    await writeFile(join(badMacDir, 'pairing.json'), JSON.stringify({ access_token: 'a', refresh_token: 'r' }));
    let threw = false;
    try { await loadCredentials(badMacDir); } catch { threw = true; }
    check('credentials: rejects invalid MAC', threw);
  } finally { await rm(badMacDir, { recursive: true, force: true }); }
  const emptyDir = await mkdtemp(join(tmpdir(), 'gadget-token-empty-'));
  try {
    let threw = false;
    try { await loadCredentials(emptyDir); } catch { threw = true; }
    check('credentials: missing pairing.json/identity.json throws', threw);
  } finally { await rm(emptyDir, { recursive: true, force: true }); }

  // --- unpair ---
  check('unpair: removes pairing.json, returns true', await unpair(dir) === true);
  check('unpair: pairing.json gone', await (async () => { try { await readFile(join(dir, 'pairing.json')); return false; } catch { return true; } })());
  check('unpair: second call returns false', await unpair(dir) === false);
  // Restore pairing.json for the refresh tests below.
  await writeFile(join(dir, 'pairing.json'), JSON.stringify({
    access_token: 'access-1', refresh_token: 'refresh-1', token_type: 'device',
    api_url_v2: '', noise_host: '', access_token_saved_at: 1000, sdk_token_hint: 'mgst_ABCDEFG',
  }), { mode: 0o600 });
  const loaded2 = await loadCredentials(dir);

  // --- refresh with a mock fetch (no real API, no real credentials) ---
  const seen: { url: string; method: string; auth: string; apiVersion: string; userAgent: string; contentType: string; body: string; hasSignal: boolean }[] = [];
  const mockFetch = (async (url: URL, init: { method?: string; headers: Record<string, string>; body?: string; signal?: unknown }) => {
    seen.push({
      url: String(url), method: init.method ?? 'GET', auth: init.headers.Authorization,
      apiVersion: init.headers['X-API-Version'], userAgent: init.headers['User-Agent'],
      contentType: init.headers['Content-Type'], body: init.body ?? '', hasSignal: init.signal !== undefined,
    });
    return { ok: true, json: async () => ({ access_token: 'access-2', refresh_token: 'refresh-2' }) };
  }) as unknown as typeof globalThis.fetch;
  let saved: unknown = null;
  const account = new MuseAccount({
    credentials: loaded2,
    onCredentials: async (next) => { saved = next; await saveCredentials(next, dir); },
    fetch: mockFetch,
  });
  const rotated = await account.refresh();
  check('refresh: returns new tokens', rotated.accessToken === 'access-2' && rotated.refreshToken === 'refresh-2');
  check('refresh: hits /device_token/refresh', seen.length === 1 && seen[0].url === 'https://api.muse.ai/device_token/refresh');
  check('refresh: POST with API version + user agent + JSON content type',
    seen[0].method === 'POST' && seen[0].apiVersion === '1.0.0'
    && seen[0].userAgent === 'muse-client/0.1.0' && seen[0].contentType === 'application/json');
  check('refresh: request carries an abort signal (timeout)', seen[0].hasSignal === true);
  check('refresh: Bearer uses hatch_refresh:<refresh after last colon>', seen[0].auth === 'Bearer hatch_refresh:refresh-1');
  const body = JSON.parse(seen[0].body);
  check('refresh: body carries device_id + sdk_token', body.device_id === 'homelink-ddeeff' && body.sdk_token === good);
  check('refresh: onCredentials fired and saved to disk', saved !== null && (saved as { accessToken: string }).accessToken === 'access-2');
  const reloaded = JSON.parse(await readFile(join(dir, 'pairing.json'), 'utf8'));
  check('refresh: pairing.json on disk has rotated tokens', reloaded.access_token === 'access-2' && reloaded.refresh_token === 'refresh-2');
  check('refresh: saved pairing.json stays 0600', ((await stat(join(dir, 'pairing.json'))).mode & 0o777) === 0o600);

  // --- refresh shares one flight for concurrent callers ---
  seen.length = 0;
  const account2 = new MuseAccount({ credentials: rotated, fetch: mockFetch });
  const [r1, r2] = await Promise.all([account2.refresh(), account2.refresh()]);
  check('refresh: concurrent refresh shares one request', seen.length === 1 && r1.accessToken === r2.accessToken);

  // --- envelope response: rotate() parses result.payload ?? result ---
  seen.length = 0;
  const envelopeFetch = (async () => ({
    ok: true,
    json: async () => ({ payload: { access_token: 'access-3', refresh_token: 'refresh-3' } }),
  })) as unknown as typeof globalThis.fetch;
  const account3 = new MuseAccount({ credentials: rotated, fetch: envelopeFetch });
  const rotated3 = await account3.refresh();
  check('refresh: envelope {payload} response parses', rotated3.accessToken === 'access-3' && rotated3.refreshToken === 'refresh-3');

  // --- sdkToken absent: the body must not carry sdk_token ---
  seen.length = 0;
  const noSdk = { ...rotated, sdkToken: undefined };
  const account4 = new MuseAccount({ credentials: noSdk, fetch: mockFetch });
  await account4.refresh();
  check('refresh: body omits sdk_token when no SDK token', !('sdk_token' in JSON.parse(seen[0].body)));

  // --- onWarning fires on SDK-token hint mismatch ---
  let warned = '';
  const mismatch = { ...rotated, sdkTokenHint: 'mgst_XXXXXXXX', sdkToken: good };
  new MuseAccount({ credentials: mismatch, onWarning: (m) => { warned = m; } });
  check('account: hint mismatch warns', warned.includes('SDK token has changed'));

  // --- error paths: HTTP !ok, malformed JSON, missing refresh token ---
  const failingFetch = (async () => ({ ok: false, status: 401, json: async () => ({}) })) as unknown as typeof globalThis.fetch;
  const account5 = new MuseAccount({ credentials: rotated, fetch: failingFetch });
  let httpThrew = false;
  try { await account5.refresh(); } catch (e) { httpThrew = (e as Error).name === 'MuseHttpError'; }
  check('refresh: HTTP !ok throws MuseHttpError', httpThrew);
  const badJsonFetch = (async () => ({ ok: true, json: async () => { throw new Error('bad json'); } })) as unknown as typeof globalThis.fetch;
  const account6 = new MuseAccount({ credentials: rotated, fetch: badJsonFetch });
  let jsonThrew = false;
  try { await account6.refresh(); } catch { jsonThrew = true; }
  check('refresh: malformed JSON throws', jsonThrew);
  let missingRefreshThrew = false;
  try { new MuseAccount({ credentials: { ...rotated, refreshToken: '' }, fetch: mockFetch }); } catch { missingRefreshThrew = true; }
  // The constructor does not require refreshToken; rotate() does.
  if (!missingRefreshThrew) {
    const account7 = new MuseAccount({ credentials: { ...rotated, refreshToken: '' }, fetch: mockFetch });
    try { await account7.refresh(); } catch { missingRefreshThrew = true; }
  }
  check('refresh: missing refresh token throws', missingRefreshThrew);

  // --- CLI smoke test: status against the temp dir prints metadata, no token values ---
  const cliOut = await new Promise<string>((resolve, reject) => {
    const proc = spawn(process.execPath, [join('extensions', 'gadget-token', 'cli.ts'), 'status', '--credentials', dir], { cwd: process.cwd() });
    let out = '';
    proc.stdout.on('data', (d) => { out += d; });
    proc.stderr.on('data', (d) => { out += d; });
    proc.on('error', reject);
    proc.on('close', () => resolve(out));
  }).catch(() => '');
  // bun runs .ts directly; if the spawn path above is unavailable, fall back to bun.
  const cliText = cliOut || await new Promise<string>((resolve) => {
    const proc = spawn('/opt/hatch-image/bin/bun', ['extensions/gadget-token/cli.ts', 'status', '--credentials', dir], { cwd: process.cwd() });
    let out = '';
    proc.stdout.on('data', (d) => { out += d; });
    proc.stderr.on('data', (d) => { out += d; });
    proc.on('close', () => resolve(out));
  });
  check('cli: status prints device id, no token values',
    cliText.includes('homelink-ddeeff') && !cliText.includes('access-2') && !cliText.includes('refresh-2') && !cliText.includes(good));
} finally {
  await rm(dir, { recursive: true, force: true });
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
