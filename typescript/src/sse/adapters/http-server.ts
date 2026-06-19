/**
 * Inbound HTTP adapter (Node `http`).
 *
 * Translates HTTP/SSE into calls on the `SseHub`:
 *   - `GET  /`              → serves the frontend.
 *   - `GET  /healthz`       → liveness JSON.
 *   - `GET  /events`        → opens an SSE stream (delegates fan-out to the hub).
 *   - `POST /publish`       → publishes one envelope via the hub.
 *   - `OPTIONS`             → CORS preflight (204).
 *
 * It also adapts a `ServerResponse` into a `ConnectionSink` (SSE headers,
 * `hello` event, heartbeat, framing, teardown on close). The hub never sees any
 * of this — that boundary is the whole point of the hexagon.
 */

import { createServer, IncomingMessage, Server, ServerResponse } from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { PublishTarget, SseFrame } from '../domain/contract';
import { SseHub } from '../domain/hub';
import { ConnectionSink, Unsubscribe } from '../ports';

const __dirname = dirname(fileURLToPath(import.meta.url));
/** The frontend lives one level up, in `sse/public/`. */
const INDEX_HTML = join(__dirname, '..', 'public', 'index.html');

/** Heartbeat comment interval — keeps idle SSE streams alive through proxies. */
const HEARTBEAT_MS = 15000;

/** Shape of the `POST /publish` request body. */
interface PublishBody {
  to: PublishTarget;
  from?: string;
  text?: string;
}

/** A running HTTP adapter; close it to stop accepting connections. */
export interface SseHttpServer {
  readonly port: number;
  close(): Promise<void>;
}

/** Permissive CORS for the demo: any origin, the methods/headers this API uses. */
function setCors(res: ServerResponse): void {
  res.setHeader('access-control-allow-origin', '*');
  res.setHeader('access-control-allow-methods', 'GET, POST, OPTIONS');
  res.setHeader('access-control-allow-headers', 'content-type');
  res.setHeader('access-control-max-age', '86400');
}

/** Collect a request body into a string. */
function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c) => chunks.push(c as Buffer));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

/**
 * Adapt a `ServerResponse` into a `ConnectionSink`: write SSE headers + the
 * immediate `hello` event, start the heartbeat, and frame every `send` as an
 * SSE `data:` line. `onClose` is invoked once when the client disconnects.
 */
function openSseSink(
  res: ServerResponse,
  req: IncomingMessage,
  instanceId: string,
  identity: { userId: string; orgId: string },
  onClose: () => void,
): ConnectionSink {
  res.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache, no-transform',
    connection: 'keep-alive',
    'x-instance-id': instanceId,
  });
  // Tell the client which instance it is connected to, immediately.
  res.write(`event: hello\ndata: ${JSON.stringify({ instanceId, ...identity })}\n\n`);

  const heartbeat = setInterval(() => {
    res.write(`: keep-alive ${Date.now()}\n\n`);
  }, HEARTBEAT_MS);
  if (typeof heartbeat.unref === 'function') heartbeat.unref();

  let closed = false;
  const cleanup = () => {
    if (closed) return;
    closed = true;
    clearInterval(heartbeat);
    onClose();
  };
  req.on('close', cleanup);
  res.on('close', cleanup);

  return {
    send: (frame: SseFrame) => {
      res.write(`data: ${JSON.stringify(frame)}\n\n`);
    },
    close: () => res.end(),
  };
}

/** Start the HTTP adapter on `port`, dispatching into `hub`. */
export function startHttpServer(opts: {
  port: number;
  instanceId: string;
  hub: SseHub;
}): Promise<SseHttpServer> {
  const { port, instanceId, hub } = opts;

  const server: Server = createServer((req, res) => {
    void handle(req, res).catch((err) => {
      if (!res.headersSent) res.writeHead(500, { 'content-type': 'text/plain' });
      res.end(`error: ${String((err as Error)?.message ?? err)}`);
    });
  });

  async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', `http://localhost:${port}`);

    // Allow any origin — local demo, frontend may be served from a different
    // origin/instance than the one it publishes to. Set via setHeader so the
    // headers survive every writeHead below (incl. the SSE stream + errors).
    setCors(res);
    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.method === 'GET' && url.pathname === '/healthz') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true, instanceId }));
      return;
    }

    if (req.method === 'GET' && url.pathname === '/') {
      const html = await readFile(INDEX_HTML, 'utf8');
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      res.end(html);
      return;
    }

    if (req.method === 'GET' && url.pathname === '/events') {
      await handleEvents(url, req, res);
      return;
    }

    if (req.method === 'POST' && url.pathname === '/publish') {
      await handlePublish(req, res);
      return;
    }

    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
  }

  async function handleEvents(
    url: URL,
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<void> {
    const userId = url.searchParams.get('userId');
    const orgId = url.searchParams.get('orgId');
    if (!userId || !orgId) {
      res.writeHead(400, { 'content-type': 'text/plain' });
      res.end('userId and orgId query params are required');
      return;
    }

    // Bind the connection's subscriptions through the hub; tear them down when
    // the client disconnects (the sink wires `onClose` to req/res close).
    let teardown: Unsubscribe = async () => {};
    const sink = openSseSink(res, req, instanceId, { userId, orgId }, () => {
      void teardown().catch(() => {});
    });
    teardown = await hub.connect({ userId, orgId }, sink);
  }

  async function handlePublish(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const raw = await readBody(req);
    let body: PublishBody;
    try {
      body = JSON.parse(raw) as PublishBody;
    } catch {
      res.writeHead(400, { 'content-type': 'text/plain' });
      res.end('invalid JSON body');
      return;
    }
    if (!body?.to?.type || !body?.to?.id) {
      res.writeHead(400, { 'content-type': 'text/plain' });
      res.end('body must be { to: { type, id }, from, text }');
      return;
    }
    const routingKey = await hub.publish({
      to: body.to,
      from: body.from ?? 'anon',
      text: body.text ?? '',
    });
    res.writeHead(202, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ accepted: true, routingKey, via: instanceId }));
  }

  return new Promise<SseHttpServer>((resolve) => {
    server.listen(port, () => {
      resolve({
        port,
        close: () =>
          new Promise<void>((res2, rej) => server.close((err) => (err ? rej(err) : res2()))),
      });
    });
  });
}
