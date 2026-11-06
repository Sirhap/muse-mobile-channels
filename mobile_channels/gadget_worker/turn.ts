// One Gadget turn, driven as a short-lived worker process.
//
// The bridge executor cannot await a reply inside a tick: it submits once
// and polls. This worker owns the whole turn -- connect, send (which creates
// the side chat on first use), subscribe, follow the bound reply to
// completion -- and records the outcome in a result file the executor polls.
// The executor spawns one worker per attempt and kills it on cancel.
//
// Protocol facts (proven live 2026-11-06):
// - A fresh side chat must be created by sending its first message with
//   sendMessage({ sessionId }); subscribing to a not-yet-created side chat
//   fails with /chat/subscribe HTTP 404. So this worker sends first, then
//   subscribes -- the reverse of the library's chat() order.
// - Sending to the main chat (no session id) fails with /chat/stream HTTP
//   500 on this account; side chats work. This worker only ever drives
//   side chats.
// - The reply is bound through MuseTurn: with ownsChat the dedicated
//   session's assistant messages are bound; without it the reply's parent
//   must be the acknowledged user message id.
//
// The worker never prints the message text or any credential value. stderr
// carries lifecycle events only (connection, ack id, terminal state).
import { readFileSync, renameSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { MuseClient } from '../../reference/muse-client/src/client.js';
import { loadCredentials } from '../../reference/muse-client/src/credentials.js';
import { MuseTurn } from '../../reference/muse-client/src/turn.js';

const TERMINAL_TASK = new Set(['completed', 'failed', 'errored', 'cancelled', 'canceled']);

interface Args {
  session: string;
  messageFile: string;
  out: string;
  timeoutSeconds: number;
  credentialsDir: string;
  vmId?: string;
}

function parseArgs(argv: string[]): Args {
  const values = new Map<string, string>();
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    if (!arg.startsWith('--')) throw new Error(`unexpected argument: ${arg}`);
    const key = arg.slice(2);
    const value = argv[++i];
    if (value === undefined || value.startsWith('--')) throw new Error(`missing value for --${key}`);
    values.set(key, value);
  }
  const session = values.get('session');
  const messageFile = values.get('message-file');
  const out = values.get('out');
  if (!session || !messageFile || !out) {
    throw new Error('usage: turn --session <uuid> --message-file <path> --out <path> [--timeout <seconds>] [--credentials <dir>] [--vm <id>]');
  }
  return {
    session,
    messageFile,
    out,
    timeoutSeconds: Number(values.get('timeout') ?? 900),
    credentialsDir: values.get('credentials') ?? join(homedir(), '.config', 'MuseGadgetPair'),
    vmId: values.get('vm'),
  };
}

type ResultState = 'running' | 'succeeded' | 'failed' | 'cancelled' | 'unknown';

interface TurnResult {
  state: ResultState;
  text?: string;
  message_id?: string;
  session_id?: string;
  evidence?: string;
}

function writeResult(out: string, result: TurnResult): void {
  const tmp = `${out}.tmp-${process.pid}`;
  writeFileSync(tmp, JSON.stringify(result), 'utf8');
  renameSync(tmp, out);
}

async function main(): Promise<number> {
  const args = parseArgs(process.argv.slice(2));
  if (!Number.isSafeInteger(args.timeoutSeconds) || args.timeoutSeconds < 1) {
    console.error('timeout must be a positive integer number of seconds');
    return 2;
  }
  const startedAt = Date.now();
  let settled: TurnResult | null = null;
  const TERMINAL_RESULT = new Set(['succeeded', 'failed', 'cancelled', 'unknown']);
  const settle = (next: TurnResult): void => {
    // A terminal result is final. A 'running' placeholder may be overwritten
    // by the terminal outcome; without this the worker would exit leaving
    // the file saying 'running' forever (seen live 2026-11-06).
    if (settled && TERMINAL_RESULT.has(settled.state)) return;
    settled = next;
    try {
      writeResult(args.out, next);
    } catch (error) {
      console.error('cannot write result file:', error instanceof Error ? error.message : String(error));
    }
    console.error(`result: ${next.state}${next.evidence ? ` (${next.evidence})` : ''}`);
  };
  const onSignal = (): void => {
    if (!settled || !TERMINAL_RESULT.has(settled.state)) {
      settle({ state: 'cancelled', evidence: 'worker_terminated', session_id: args.session });
    }
    process.exit(0);
  };
  process.on('SIGTERM', onSignal);
  process.on('SIGINT', onSignal);

  const text = readFileSync(args.messageFile, 'utf8');
  const deadline = Date.now() + args.timeoutSeconds * 1000;
  let client: MuseClient | null = null;
  try {
    const credentials = await loadCredentials(args.credentialsDir);
    client = await MuseClient.connect({ credentials, vmId: args.vmId });
    console.error(`connected to ${client.vm.id}`);

    const ack = await client.sendMessage(text, { sessionId: args.session });
    console.error(`sent, message id ${ack.messageId}`);
    settle({ state: 'running', message_id: ack.messageId, session_id: args.session });

    // A subscribe on a just-created side chat can stall server-side (seen
    // live 2026-11-06: the call hung past the turn deadline while the reply
    // was generated anyway). Retry it a few times, aborting only the stuck
    // OPEN: the abort signal stays armed for the subscription's whole life,
    // so a plain AbortSignal.timeout would kill a live subscription 20s in
    // (that is what "Muse request cancelled" was). The winning attempt's
    // controller is kept and never aborted; the connection close ends it.
    let sub: Awaited<ReturnType<MuseClient['subscribe']>> | null = null;
    let subscribeError: unknown = null;
    for (let attempt = 0; attempt < 3 && !sub; attempt++) {
      if (attempt > 0) console.error(`subscribe retry ${attempt + 1}/3`);
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 20000);
      try {
        sub = await client.subscribe({ sessionId: args.session, signal: ctrl.signal });
      } catch (error) {
        subscribeError = error;
        if (Date.now() >= deadline) break;
      } finally {
        clearTimeout(timer);
      }
    }
    if (!sub) {
      const detail = subscribeError instanceof Error ? subscribeError.message : String(subscribeError ?? 'subscribe timed out');
      settle({ state: 'unknown', evidence: `subscribe_timeout: ${detail}`.slice(0, 200), message_id: ack.messageId, session_id: args.session });
      return 0;
    }
    console.error('subscribed');
    const iterator = sub[Symbol.asyncIterator]();
    let turn = new MuseTurn({ sessionId: args.session, ownsChat: true });
    try {
      turn.acknowledge(ack);
    } catch (error) {
      console.error(`owned acknowledgement rejected (${error instanceof Error ? error.message : String(error)}); falling back to parent-linked binding`);
      turn = new MuseTurn({ sessionId: args.session });
      turn.acknowledge(ack);
    }

    let terminalTaskStatus: string | undefined;
    let timedOut = false;
    let subEnded = false;
    let next = iterator.next();
    void next.catch(() => {});
    while (true) {
      if (turn.complete()) break;
      const remaining = deadline - Date.now();
      if (remaining <= 0) { timedOut = true; break; }
      const delay = turn.completionDelay();
      const wait = Number.isFinite(delay) ? Math.min(delay, remaining) : remaining;
      const raced = await Promise.race([
        next.then((value) => ({ kind: 'event' as const, value })),
        new Promise<{ kind: 'timer' }>((resolve) => setTimeout(() => resolve({ kind: 'timer' }), wait)),
      ]);
      if (raced.kind === 'timer') {
        if (turn.complete()) break;
        if (Date.now() >= deadline) { timedOut = true; break; }
        continue;
      }
      const { value, done } = raced.value;
      if (done) { subEnded = true; break; }
      const event = value;
      if (event.event === 'task.status' && typeof event.payload.status === 'string'
          && TERMINAL_TASK.has(event.payload.status)) {
        terminalTaskStatus = event.payload.status;
      }
      turn.feed(event);
      next = iterator.next();
      void next.catch(() => {});
    }
    try { await iterator.return?.(); } catch { /* subscription already closed */ }
    sub.close();

    if (timedOut) {
      settle({ state: 'unknown', evidence: 'turn_timeout', message_id: ack.messageId, session_id: args.session });
      return 0;
    }
    if (subEnded && !turn.complete()) {
      settle({ state: 'failed', evidence: 'subscription_ended_before_reply_completed', message_id: ack.messageId, session_id: args.session });
      return 0;
    }
    const bound = [...turn.replies.values()].filter((reply) => reply.bound && reply.done && reply.text.trim());
    const reply = bound.at(-1);
    if (reply) {
      settle({ state: 'succeeded', text: reply.text, message_id: reply.messageId, session_id: args.session });
      return 0;
    }
    if (terminalTaskStatus === 'cancelled' || terminalTaskStatus === 'canceled') {
      settle({ state: 'cancelled', evidence: 'platform_turn_cancelled', message_id: ack.messageId, session_id: args.session });
      return 0;
    }
    if (terminalTaskStatus === 'failed' || terminalTaskStatus === 'errored') {
      settle({ state: 'failed', evidence: `platform_turn_${terminalTaskStatus}`, message_id: ack.messageId, session_id: args.session });
      return 0;
    }
    settle({ state: 'unknown', evidence: 'turn_completed_without_bound_reply', message_id: ack.messageId, session_id: args.session });
    return 0;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`turn failed: ${message}`);
    if (!settled) {
      settle({ state: 'failed', evidence: message.slice(0, 200), session_id: args.session });
      return 0;
    }
    // A failure after the acknowledgement was recorded: the message was
    // sent, so the turn's fate is unknown, not failed.
    if (settled.state === 'running') {
      settle({ state: 'unknown', evidence: message.slice(0, 200), message_id: settled.message_id, session_id: args.session });
    }
    return 0;
  } finally {
    try { client?.close(); } catch { /* already closed */ }
    console.error(`elapsed_ms=${Date.now() - startedAt}`);
  }
}

main().then((code) => process.exit(code)).catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
