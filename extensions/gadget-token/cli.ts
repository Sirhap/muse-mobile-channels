#!/usr/bin/env bun
// Gadget token CLI: refresh, status, pair, unpair.
// Run with bun (the project's Gadget runtime):
//   bun extensions/gadget-token/cli.ts <command> [--credentials <dir>]
//
// Never pass a token on the command line: the SDK token is read from
// a file (--sdk-token-file) or stdin, never from argv. No command
// prints a token value; `status` shows metadata only (device id,
// token age, SDK-token hint).

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import {
  MuseAccount,
  defaultCredentialsDirectory,
  loadCredentials,
  saveCredentials,
  unpair,
  validateSDKToken,
  SDK_TOKEN_URL,
} from './src/index.js';

function usage(): never {
  console.error(`Usage: bun cli.ts <command> [options]
Commands:
  refresh [--credentials <dir>]   Rotate the device access token with the stored refresh token
  status  [--credentials <dir>]   Show token metadata (never prints token values)
  pair    [--credentials <dir>] [--sdk-token-file <file>]
                                  Validate an SDK token and explain pairing (macOS + Bluetooth only)
  unpair  [--credentials <dir>]   Remove pairing.json (keeps identity + sdk_token)

Options:
  --credentials <dir>   Credentials directory. Default: ${defaultCredentialsDirectory()}
                        This project keeps Gadget credentials at ~/.config/MuseGadgetPair/
  --sdk-token-file <file>  File holding the SDK token (for pair). Or pipe it on stdin.

Get an SDK token in a browser: ${SDK_TOKEN_URL}`);
  process.exit(2);
}

function flag(args: string[], name: string): string | undefined {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : undefined;
}

async function readSdkToken(args: string[]): Promise<string> {
  const file = flag(args, '--sdk-token-file');
  if (file) return validateSDKToken(await readFile(file, 'utf8'));
  if (!process.stdin.isTTY) {
    let data = '';
    for await (const chunk of process.stdin) data += chunk;
    if (data.trim()) return validateSDKToken(data);
  }
  throw new Error(`No SDK token given. Put it in a file and pass --sdk-token-file <file>, or pipe it on stdin. Get one at ${SDK_TOKEN_URL}`);
}

function ageText(savedAt?: number): string {
  if (!savedAt) return 'unknown age';
  const secs = Math.floor(Date.now() / 1000) - savedAt;
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${(secs / 3600).toFixed(1)}h ago`;
  return `${(secs / 86400).toFixed(1)}d ago`;
}

async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2);
  if (!command) usage();
  const dir = flag(args, '--credentials') ?? defaultCredentialsDirectory();

  if (command === 'refresh') {
    const creds = await loadCredentials(dir);
    const account = new MuseAccount({
      credentials: creds,
      onCredentials: (next) => saveCredentials(next, dir),
      onWarning: (m) => console.error(`warning: ${m}`),
    });
    const next = await account.refresh();
    console.log(`refreshed device ${next.deviceId}; new access token saved at ${new Date((next.savedAt ?? 0) * 1000).toISOString()} (saved to ${join(dir, 'pairing.json')})`);
    return;
  }

  if (command === 'status') {
    const creds = await loadCredentials(dir);
    console.log(`directory: ${dir}`);
    console.log(`device: ${creds.deviceId}`);
    console.log(`access token: present, saved ${ageText(creds.savedAt)}`);
    console.log(`refresh token: present`);
    console.log(`sdk token: ${creds.sdkToken ? 'present' : 'absent'}${creds.sdkTokenHint ? ` (hint ${creds.sdkTokenHint})` : ''}`);
    console.log(`api url: ${creds.apiUrl ?? '(default https://api.muse.ai)'}`);
    console.log(`noise host: ${creds.noiseHost ?? '(default)'}`);
    return;
  }

  if (command === 'pair') {
    const token = await readSdkToken(args);
    console.log(`SDK token format OK (hint ${token.slice(0, 12)}).`);
    if (process.platform !== 'darwin') {
      console.log('Pairing needs macOS + Bluetooth and cannot run on this machine.');
      console.log('Do it once on a Mac:');
      console.log(`  1. Open ${SDK_TOKEN_URL} in a browser and copy the SDK token.`);
      console.log('  2. On the Mac, run the full client pairing (reference/muse-client):');
      console.log('       bun run cli pair');
      console.log('     Paste the SDK token when prompted and allow Bluetooth access.');
      console.log('  3. Copy the resulting credential files to this machine:');
      console.log(`       pairing.json, identity.json, sdk_token  ->  ${dir}/`);
      console.log('     Keep them 0600, directory 0700. Afterwards `refresh` and `status` work here.');
      return;
    }
    console.log('This is macOS. This extension does not reimplement Bluetooth pairing.');
    console.log('Pair with the full client (reference/muse-client, vendored in this repo):');
    console.log('  bun run cli pair --credentials ' + dir);
    console.log('Paste the SDK token when prompted, allow Bluetooth, open Muse > Settings > Devices > Add Device (Developer mode enabled), and wait for `Paired successfully`.');
    return;
  }

  if (command === 'unpair') {
    const removed = await unpair(dir);
    console.log(removed ? `removed ${join(dir, 'pairing.json')} (identity + sdk_token kept)` : 'no pairing exists');
    return;
  }

  usage();
}

main().catch((error) => {
  console.error(`error: ${(error as Error).message}`);
  process.exit(1);
});
