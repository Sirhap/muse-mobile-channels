// SDK token: the one-time token a user takes from the Muse website
// and hands to pairing. Source: wong2/muse-client src/pairing.ts
// (Apache-2.0), `validateSDKToken` copied verbatim.
//
// Get one in a browser:
//   https://gadgets.muse.ai/settings/sdk-tokens
// One token per device. The token is associated with the device WHEN
// IT PAIRS: after changing that token, or revoking the token used for
// pairing, unpair and pair again with the new token. Refreshing device
// credentials does not change the SDK token associated with the device.
//
// Pairing itself needs macOS + Bluetooth (see cli.ts `pair`); this
// module only validates the token format, it never performs pairing.

import { MuseError } from './errors.js';

/** Where a user takes an SDK token: open in a browser, copy the token. */
export const SDK_TOKEN_URL = 'https://gadgets.muse.ai/settings/sdk-tokens';

/** Validate an SDK token's format; returns the trimmed token. */
export function validateSDKToken(token: string): string {
  const value = token.trim();
  if (!/^mgst_[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$/.test(value)) throw new MuseError('Invalid SDK token format');
  return value;
}
