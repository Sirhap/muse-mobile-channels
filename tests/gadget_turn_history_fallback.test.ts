// Repro for the Gadget 200s+ stall (2026-11-07), two parts:
//
// (1) Worker timeout path (this file): the worker waits on the live
// /chat/subscribe stream for a task.status terminal event, and MuseTurn
// stays active (completionDelay Infinity) while any task is still marked
// running. When that terminal event is late, the worker must at least
// settle 'unknown' (turn_timeout) at its deadline and exit. Before the
// iterator-close fix it never settled at all: `await iterator.return?.()`
// on an idle async generator waits for its pending await to settle, so
// a timed-out worker hung forever and never wrote a result.
// (2) History fallback (tests/test_gadget_executor.py,
// TestGadgetHistoryFallback): the actual fix for the 200s wait. Gadget
// Noise /chat/history is 403 (endpoint not in the device-token ACL),
// but the Cookie bridge reads a Gadget-created session's history fine
// (live probe 2026-11-07: completed '收到' on the first poll), so
// GadgetExecutor.query settles from history when the worker is still
// running on a late terminal.
//
// Real MuseTurn is used (turn.js is NOT mocked); only the transport
// (client.js / credentials.js) is mocked. No network, no production
// files, no real credentials: everything runs in a temp dir.
// Run via: bun test tests/gadget_turn_history_fallback.test.ts
import { describe, expect, mock, test } from 'bun:test';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const REF = join(import.meta.dir, '..', 'reference', 'muse-client', 'src');

type ChatEvent = { type: 'event'; event: string; seq?: number; payload: Record<string, unknown>; raw: Record<string, unknown> };

// Switch: whether the mocked subscription ever delivers the terminal event.
let deliverTerminal = false;

function event(name: string, payload: Record<string, unknown>, seq: number): ChatEvent {
  return { type: 'event', event: name, seq, payload, raw: { ...payload } };
}

async function* stalledSubscription(): AsyncGenerator<ChatEvent> {
  yield event('task.status', { session_id: 'sess-1', status: 'running', task_id: 'task-1' }, 1);
  yield event('delta.text_append', {
    session_id: 'sess-1', message_id: 'a-1', reply_to_message_id: 'u-1', text: '收到',
  }, 2);
  yield event('delta.message_done', {
    session_id: 'sess-1', message_id: 'a-1', reply_to_message_id: 'u-1', display_text: '收到',
  }, 3);
  if (deliverTerminal) {
    yield event('task.status', { session_id: 'sess-1', status: 'completed', task_id: 'task-1' }, 4);
    return;
  }
  // Terminal never arrives and the stream never ends: the production stall.
  await new Promise<void>(() => {});
}

const fakeClient = {
  vm: { id: 'vm-test' },
  sendMessage: () => ({
    messageId: 'u-1',
    raw: { result: { session_id: 'sess-1', is_thread: true } },
  }),
  subscribe: async () => ({
    close: () => {},
    [Symbol.asyncIterator]: () => stalledSubscription(),
  }),
  close: () => {},
};

mock.module(join(REF, 'client.js'), () => ({
  MuseClient: class {
    static async connect() {
      return fakeClient;
    }
  },
}));
mock.module(join(REF, 'credentials.js'), () => ({
  loadCredentials: async () => ({}),
  saveCredentials: async () => {},
}));
// NOTE: turn.js is deliberately NOT mocked here: the stall lives in the
// real MuseTurn completion rules.

async function runWorkerMain(timeoutSeconds: number) {
  const { main } = await import('../mobile_channels/gadget_worker/turn.ts');
  const dir = mkdtempSync(join(tmpdir(), 'gadget-stall-repro-'));
  const messageFile = join(dir, 'msg.txt');
  const outFile = join(dir, 'result.json');
  writeFileSync(messageFile, 'sandbox stall repro: please reply 收到', 'utf8');
  const savedArgv = process.argv;
  process.argv = ['bun', 'turn.ts', '--session', 'sess-1', '--message-file', messageFile,
    '--out', outFile, '--timeout', String(timeoutSeconds), '--credentials', join(dir, 'creds')];
  try {
    const started = Date.now();
    const code = await main();
    const elapsedMs = Date.now() - started;
    const result = JSON.parse(readFileSync(outFile, 'utf8'));
    return { code, result, elapsedMs };
  } finally {
    process.argv = savedArgv;
  }
}

describe('Gadget subscription-terminal stall (repro)', () => {
  test('stalled terminal: worker settles unknown at its deadline and exits', async () => {
    deliverTerminal = false;
    const { code, result, elapsedMs } = await runWorkerMain(5);
    expect(code).toBe(0);
    // Before the iterator-close fix this never returned: the worker hung
    // in `await iterator.return?.()` past the deadline and the test
    // itself timed out. After the fix it settles promptly.
    expect(result.state).toBe('unknown');
    expect(result.evidence).toContain('turn_timeout');
    expect(elapsedMs).toBeLessThan(15000);
  }, 30000);

  test('control: terminal event present completes promptly', async () => {
    deliverTerminal = true;
    const { code, result, elapsedMs } = await runWorkerMain(5);
    expect(code).toBe(0);
    expect(result.state).toBe('succeeded');
    expect(result.text).toBe('收到');
    expect(elapsedMs).toBeLessThan(5000);
  }, 30000);
});
