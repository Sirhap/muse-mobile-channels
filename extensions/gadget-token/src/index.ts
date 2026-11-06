// Gadget token extension: only the code that gets and keeps a device token.
// Re-exports the whole public surface. Chat, Noise transport, VM lookup
// and the Bluetooth pairing protocol are intentionally NOT here; see
// README.md for what this extension does and does not do.

export { MuseError, MuseHttpError, MuseProtocolError } from './errors.js';
export { MuseAccount } from './account.js';
export type { DeviceCredentials, AccountOptions } from './account.js';
export {
  defaultCredentialsDirectory,
  loadCredentials,
  saveCredentials,
  unpair,
} from './credentials.js';
export { SDK_TOKEN_URL, validateSDKToken } from './sdk-token.js';
