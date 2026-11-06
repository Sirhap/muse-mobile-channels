// Device token account: refresh a Gadget device access token.
// Source: wong2/muse-client src/api.ts (Apache-2.0), reduced to the
// token flow only. The VM lookup (listVMs) and chat paths are NOT
// included here; this module does one thing: keep the device token
// alive by rotating it with the stored refresh token.
//
// Flow: pairing (macOS, Bluetooth) yields pairing.json with an
// access_token + refresh_token. Afterwards this class rotates them:
// POST /device_token/refresh with Bearer `hatch_refresh:<refresh>`
// returns a fresh access_token + refresh_token. Callers must durably
// save the rotated credentials (onCredentials) before resolving,
// or the next process will keep using the old refresh token.

import { MuseError, MuseHttpError, MuseProtocolError } from './errors.js';

export interface DeviceCredentials {
  accessToken: string;
  refreshToken: string;
  /** The node_id used during pairing, e.g. homelink-123abc. */
  deviceId: string;
  sdkToken?: string;
  /** First 12 characters of the SDK token used when this device paired. */
  sdkTokenHint?: string;
  apiUrl?: string;
  noiseHost?: string;
  savedAt?: number;
}

export interface AccountOptions {
  credentials: DeviceCredentials;
  /** Must durably save rotated tokens before resolving. */
  onCredentials?: (credentials: DeviceCredentials) => Promise<void> | void;
  /** Non-fatal configuration warnings; never contains a full token. */
  onWarning?: (message: string) => void;
  fetch?: typeof globalThis.fetch;
}

export function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new MuseProtocolError('Expected JSON object');
  return value as Record<string, unknown>;
}

export function requiredString(value: unknown, label: string): string {
  if (typeof value !== 'string' || !value) throw new MuseProtocolError(`Missing ${label}`);
  return value;
}

export class MuseAccount {
  private current: DeviceCredentials;
  private refreshing?: Promise<DeviceCredentials>;

  constructor(private readonly options: AccountOptions) {
    this.current = { ...options.credentials };
    requiredString(this.current.accessToken, 'access token');
    requiredString(this.current.deviceId, 'device ID');
    if (this.current.sdkTokenHint && this.current.sdkToken
        && this.current.sdkTokenHint !== this.current.sdkToken.trim().slice(0, 12)) {
      options.onWarning?.('The SDK token has changed since pairing. Refreshing device credentials does not change the SDK token associated with this device. Run cli unpair, then cli pair with the new token.');
    }
  }

  get credentials(): DeviceCredentials { return { ...this.current }; }

  private async call(path: string, token: string, body?: unknown): Promise<unknown> {
    const root = new URL(this.current.apiUrl || 'https://api.muse.ai');
    if (root.protocol !== 'https:' || root.username || root.password) throw new MuseError('Muse API requires HTTPS');
    const response = await (this.options.fetch ?? globalThis.fetch)(new URL(path, root), {
      method: body === undefined ? 'GET' : 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'X-API-Version': '1.0.0',
        'User-Agent': 'muse-client/0.1.0',
        'Content-Type': 'application/json',
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      // 60s, not the upstream 15s: the egress proxy here is slow and a
      // 15s REST timeout killed a live Gadget turn before connect
      // (2026-11-06, 15008ms). Kept from the project's local patch to
      // the vendored reference/muse-client.
      signal: AbortSignal.timeout(60000),
      redirect: 'error',
    });
    if (!response.ok) throw new MuseHttpError(response.status, path);
    return response.json();
  }

  /** Rotate the device token once; concurrent callers share one flight. */
  refresh(): Promise<DeviceCredentials> {
    if (this.refreshing) return this.refreshing;
    this.refreshing = this.rotate().finally(() => { this.refreshing = undefined; });
    return this.refreshing;
  }

  private async rotate(): Promise<DeviceCredentials> {
    const raw = requiredString(this.current.refreshToken, 'refresh token').split(':').at(-1)!;
    const result = object(await this.call('/device_token/refresh', `hatch_refresh:${raw}`, {
      device_id: this.current.deviceId,
      ...(this.current.sdkToken ? { sdk_token: this.current.sdkToken } : {}),
    }));
    const data = object(result.payload ?? result);
    const next = {
      ...this.current,
      accessToken: requiredString(data.access_token, 'refreshed access token'),
      refreshToken: requiredString(data.refresh_token, 'refreshed refresh token'),
      savedAt: Math.floor(Date.now() / 1000),
    };
    // Keep rotated tokens in memory even if disk persistence fails.
    this.current = next;
    await this.options.onCredentials?.({ ...next });
    return { ...next };
  }
}
