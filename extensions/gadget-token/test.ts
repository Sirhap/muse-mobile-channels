// Tests for the gadget-token extension. Run with:
//   bun extensions/gadget-token/test.ts
// Safe by design: uses temp credential directories and a mock fetch.
// It never touches the real credential directory and never hits the
// real Muse API, and it never prints a token value.

import { mkdtemp, writeFile, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  MuseAccount,
  loadCredentials,
  saveCredentials,
  validateSDKToken,
  SDK_TOKEN_URL,
} from './src/index.js';

let passed = 0;
let failed = 0;
function check(name: string, cond: boolean, detail = '') {
  if (cond) { passed++; console.log(`PASS ${name}`); }
  else { failed++; console.log(`FAIL ${name} ${detail}`); }
}

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

// --- credentials load/save round-trip in a temp dir ---
const dir = await mkdtemp(join(tmpdir(), 'gadget-token-test-'));
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

// --- refresh with a mock fetch (no real API, no real credentials) ---
const seen: { url: string; auth: string; body: string }[] = [];
const mockFetch = (async (url: URL, init: { headers: Record<string, string>; body?: string }) => {
  seen.push({ url: String(url), auth: init.headers.Authorization, body: init.body ?? '' });
  return {
    ok: true,
    json: async () => ({ access_token: 'access-2', refresh_token: 'refresh-2' }),
  };
}) as unknown as typeof globalThis.fetch;
let saved: unknown = null;
const account = new MuseAccount({
  credentials: loaded,
  onCredentials: async (next) => { saved = next; await saveCredentials(next, dir); },
  fetch: mockFetch,
});
const rotated = await account.refresh();
check('refresh: returns new tokens', rotated.accessToken === 'access-2' && rotated.refreshToken === 'refresh-2');
check('refresh: hits /device_token/refresh', seen.length === 1 && seen[0].url === 'https://api.muse.ai/device_token/refresh');
check('refresh: Bearer uses hatch_refresh:<refresh after last colon>', seen[0].auth === 'Bearer hatch_refresh:refresh-1');
const body = JSON.parse(seen[0].body);
check('refresh: body carries device_id + sdk_token', body.device_id === 'homelink-ddeeff' && body.sdk_token === good);
check('refresh: onCredentials fired and saved to disk', saved !== null && (saved as { accessToken: string }).accessToken === 'access-2');
const reloaded = JSON.parse(await readFile(join(dir, 'pairing.json'), 'utf8'));
check('refresh: pairing.json on disk has rotated tokens', reloaded.access_token === 'access-2' && reloaded.refresh_token === 'refresh-2');

// --- refresh shares one flight for concurrent callers ---
seen.length = 0;
const account2 = new MuseAccount({ credentials: rotated, fetch: mockFetch });
const [r1, r2] = await Promise.all([account2.refresh(), account2.refresh()]);
check('refresh: concurrent refresh shares one request', seen.length === 1 && r1.accessToken === r2.accessToken);

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
