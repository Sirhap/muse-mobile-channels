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
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
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

/** This project's Gadget credential directory. */
export const PROJECT_CREDENTIALS_DIR = join(homedir(), '.config', 'MuseGadgetPair');

/** Resolve the credentials directory: --credentials flag, then the
 * MUSE_GADGET_CREDENTIALS env var, then this project's directory when
 * it exists, then the upstream default. */
export function resolveCredentialsDir(args: string[]): string {
  const fromFlag = flag(args, '--credentials');
  if (fromFlag) return fromFlag;
  if (process.env.MUSE_GADGET_CREDENTIALS) return process.env.MUSE_GADGET_CREDENTIALS;
  if (existsSync(PROJECT_CREDENTIALS_DIR)) return PROJECT_CREDENTIALS_DIR;
  return defaultCredentialsDirectory();
}

function usage(): never {
  console.error(`Usage: bun cli.ts <command> [options]
Commands:
  refresh [--credentials <dir>]   Rotate the device access token with the stored refresh token
  status  [--credentials <dir>]   Show token metadata (never prints token values)
  pair    [--credentials <dir>] [--sdk-token-file <file>]
                                  Validate an SDK token and explain pairing (macOS + Bluetooth only)
  unpair  [--credentials <dir>]   Remove pairing.json (keeps identity + sdk_token)

Options:
  --credentials <dir>   Credentials directory. Default: MUSE_GADGET_CREDENTIALS
                        env var, else ~/.config/MuseGadgetPair when it exists,
                        else ${defaultCredentialsDirectory()}
  --sdk-token-file <file>  File holding the SDK token (for pair). Or pipe it on stdin,
                        or keep it in <credentials>/sdk_token.

Get an SDK token in a browser: ${SDK_TOKEN_URL}`);
  process.exit(2);
}

function flag(args: string[], name: string): string | undefined {
  const i = args.indexOf(name);
  if (i < 0) return undefined;
  const value = args[i + 1];
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${name} needs a value`);
  }
  return value;
}

async function readSdkToken(args: string[], dir: string): Promise<string> {
  const file = flag(args, '--sdk-token-file');
  if (file) return validateSDKToken(await readFile(file, 'utf8'));
  if (!process.stdin.isTTY) {
    let data = '';
    for await (const chunk of process.stdin) data += chunk;
    if (data.trim()) return validateSDKToken(data);
  }
  try {
    return validateSDKToken(await readFile(join(dir, 'sdk_token'), 'utf8'));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  throw new Error(`No SDK token given. Put it in a file and pass --sdk-token-file <file>, pipe it on stdin, or keep it in ${join(dir, 'sdk_token')}. Get one at ${SDK_TOKEN_URL}`);
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
  const dir = resolveCredentialsDir(args);

  if (command === 'refresh') {
    const creds = await loadCredentials(dir);
    const account = new MuseAccount({
      credentials: creds,
      onCredentials: (next) => saveCredentials(next, dir),
      onWarning: (m) => console.error(`warning: ${m}`),
    });
    let next;
    try {
      next = await account.refresh();
    } catch (error) {
      // The tokens may already be rotated server-side while the disk
      // save failed: the stored refresh token may no longer work.
      // Say so loudly instead of a bare error.
      console.error('WARNING: token rotation may have succeeded server-side but the new tokens were NOT saved to disk. The stored refresh token may no longer work; re-pair if refresh keeps failing.');
      throw error;
    }
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
    const token = await readSdkToken(args, dir);
    console.log(`SDK token format OK (hint ${token.slice(0, 12)}).`);
    if (process.platform !== 'darwin') {
      console.log('Pairing needs macOS + Bluetooth and cannot run on this machine.');
      console.log('Do it once on a Mac:');
      console.log(`  1. Open ${SDK_TOKEN_URL} in a browser and copy the SDK token.`);
      console.log('  2. On the Mac, run the full client pairing (reference/muse-client):');
      console.log('       cd reference/muse-client && bun run cli pair');
      console.log('     Paste the SDK token when prompted and allow Bluetooth access.');
      console.log('  3. Copy the resulting credential files to this machine:');
      console.log(`       pairing.json, identity.json, sdk_token  ->  ${dir}/`);
      console.log('     Keep them 0600, directory 0700. Afterwards `refresh` and `status` work here.');
      return;
    }
    console.log('This is macOS. This extension does not reimplement Bluetooth pairing.');
    console.log('Pair with the full client (reference/muse-client, vendored in this repo):');
    console.log('  cd reference/muse-client && bun run cli pair --credentials ' + dir);
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
