// Error types for the Gadget token extension.
// Source: wong2/muse-client src/errors.ts (Apache-2.0), copied verbatim.
// Only the errors the token flow needs; the chat/Noise errors live in
// the full client and are intentionally not included here.

export class MuseError extends Error {
  override name = 'MuseError';
}

export class MuseHttpError extends MuseError {
  override name = 'MuseHttpError';
  constructor(public readonly status: number, public readonly endpoint: string) {
    super(`Muse ${endpoint} returned HTTP ${status}`);
  }
}

export class MuseProtocolError extends MuseError {
  override name = 'MuseProtocolError';
}
