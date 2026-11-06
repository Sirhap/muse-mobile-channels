// Bun tests for mobile_channels/gadget_worker/turn.ts helpers and the
// send-failure branches (items 1 + 4 of the executor fix batch).
//
// - mergeBoundReplies / extractFileMedia: a turn's answer may span
//   several assistant messages; all of them merge into one text +
//   media list, and [[FILE:...]] markers never survive into text.
// - sendMessage throwing (or returning an empty ack id) settles
//   'unknown', never 'failed': the message may have reached the server,
//   and a 'failed' without a message id would make DualTrack re-send
//   on the Native track and double-deliver.
// - A failure before the send was attempted (connect) is a determinable
//   'failed': nothing could have been delivered.
//
// Mocks for the reference client are registered at the top of this file
// BEFORE turn.ts is imported (dynamic import inside the tests), and the
// mocked send/connect behaviour is switched per test through mutable
// variables the mocks close over.
//
// Run via: bun test tests/gadget_turn.test.ts
// (tests/test_core_fix_exec_turn_ts.py drives this from unittest.)
import { describe, expect, mock, test } from 'bun:test';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const REF = join(import.meta.dir, '..', 'reference', 'muse-client', 'src');

// Per-test behaviour switches, read by the mocks at call time.
let sendImpl: (text: string) => unknown = () => ({ messageId: 'm-1', raw: {} });
let connectImpl: () => unknown = () => fakeClient;

const fakeClient = {
  vm: { id: 'vm-test' },
  sendMessage: (...args: unknown[]) => (sendImpl as (...a: unknown[]) => unknown)(...args),
  subscribe: async () => {
    throw new Error('subscribe must not be reached in these branches');
  },
  close: () => {},
};

mock.module(join(REF, 'client.js'), () => ({
  MuseClient: class {
    static async connect() {
      return connectImpl();
    }
  },
}));
mock.module(join(REF, 'credentials.js'), () => ({
  loadCredentials: async () => ({}),
  saveCredentials: async () => {},
}));
mock.module(join(REF, 'turn.js'), () => ({
  MuseTurn: class {
    replies = new Map();
    acknowledge() {}
    feed() {
      return [];
    }
    complete() {
      return true;
    }
    completionDelay() {
      return 0;
    }
  },
}));

async function loadTurn() {
  return import('../mobile_channels/gadget_worker/turn.ts');
}

describe('extractFileMedia', () => {
  test('parses a video marker into media and strips it from text', async () => {
    const { extractFileMedia } = await loadTurn();
    const { text, media } = extractFileMedia('第一段\n[[FILE:/workspace/clip.mp4]]\nhttps://example.invalid/v');
    expect(text).toBe('第一段\nhttps://example.invalid/v');
    expect(media).toEqual([{ kind: 'video', path: '/workspace/clip.mp4', filename: 'clip.mp4' }]);
  });

  test('classifies image and file kinds by extension', async () => {
    const { extractFileMedia } = await loadTurn();
    const { media } = extractFileMedia('[[FILE:/workspace/a.png]] [[FILE:/workspace/b.pdf]]');
    expect(media).toEqual([
      { kind: 'image', path: '/workspace/a.png', filename: 'a.png' },
      { kind: 'file', path: '/workspace/b.pdf', filename: 'b.pdf' },
    ]);
  });

  test('relative paths produce no media item and leave no marker', async () => {
    const { extractFileMedia } = await loadTurn();
    const { text, media } = extractFileMedia('see [[FILE:relative/x.png]] now');
    expect(text).not.toContain('[[FILE:');
    expect(media).toEqual([]);
  });

  test('duplicate paths collapse to one media item', async () => {
    const { extractFileMedia } = await loadTurn();
    const { media } = extractFileMedia('[[FILE:/workspace/a.png]] [[FILE:/workspace/a.png]]');
    expect(media).toHaveLength(1);
  });
});

describe('mergeBoundReplies', () => {
  test('joins every bound reply and merges media', async () => {
    const { mergeBoundReplies } = await loadTurn();
    const { text, media } = mergeBoundReplies([
      '第一段',
      '[[FILE:/workspace/clip.mp4]]\nhttps://example.invalid/v',
    ]);
    expect(text).toBe('第一段\nhttps://example.invalid/v');
    expect(media).toEqual([{ kind: 'video', path: '/workspace/clip.mp4', filename: 'clip.mp4' }]);
  });

  test('skips empty replies', async () => {
    const { mergeBoundReplies } = await loadTurn();
    expect(mergeBoundReplies(['', '  ', 'ok']).text).toBe('ok');
  });
});

describe('validateAckMessageId', () => {
  test('empty or missing id is null, never a usable id', async () => {
    const { validateAckMessageId } = await loadTurn();
    expect(validateAckMessageId({ messageId: '' })).toBeNull();
    expect(validateAckMessageId({})).toBeNull();
    expect(validateAckMessageId({ messageId: '  ' })).toBeNull();
    expect(validateAckMessageId({ messageId: 'm-1' })).toBe('m-1');
  });
});

async function runWorkerMain() {
  const { main } = await loadTurn();
  const dir = mkdtempSync(join(tmpdir(), 'gadget-turn-test-'));
  const messageFile = join(dir, 'msg.txt');
  const outFile = join(dir, 'result.json');
  writeFileSync(messageFile, 'hello gadget', 'utf8');
  const savedArgv = process.argv;
  process.argv = ['bun', 'turn.ts', '--session', 'sess-1', '--message-file', messageFile,
    '--out', outFile, '--timeout', '30', '--credentials', join(dir, 'creds')];
  try {
    const code = await main();
    const result = JSON.parse(readFileSync(outFile, 'utf8'));
    return { code, result };
  } finally {
    process.argv = savedArgv;
  }
}

describe('send failure handling in main()', () => {
  test('sendMessage throwing settles unknown, never failed', async () => {
    sendImpl = () => {
      throw new Error('stream reset after accept');
    };
    connectImpl = () => fakeClient;
    const { code, result } = await runWorkerMain();
    expect(code).toBe(0);
    expect(result.state).toBe('unknown');
    expect(result.evidence).toContain('send_unconfirmed');
    expect(result.message_id).toBeUndefined();
  });

  test('empty ack message id settles unknown', async () => {
    sendImpl = () => ({ messageId: '', raw: {} });
    connectImpl = () => fakeClient;
    const { code, result } = await runWorkerMain();
    expect(code).toBe(0);
    expect(result.state).toBe('unknown');
    expect(result.evidence).toBe('ack_missing_message_id');
  });

  test('connect failure before any send settles failed', async () => {
    sendImpl = () => ({ messageId: 'm-1', raw: {} });
    connectImpl = () => {
      throw new Error('no vm available');
    };
    const { code, result } = await runWorkerMain();
    expect(code).toBe(0);
    expect(result.state).toBe('failed');
  });
});
