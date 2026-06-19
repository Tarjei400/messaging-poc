import { fileURLToPath } from 'node:url';
import { IMessageBus, IMessageScheduler } from '../abstractions';
import { BrokerId, KNOWN_BROKERS } from '../adapters';
import { connect } from '../interactive/common';
import { BusMessagePort } from './adapters/bus-message-port';
import { startHttpServer } from './adapters/http-server';
import { SseHub } from './domain/hub';

/**
 * Composition root of the SSE cross-instance cluster (hexagonal architecture).
 *
 * The hexagon, from the inside out:
 *   - DOMAIN   `domain/contract.ts`, `domain/hub.ts` — the wire contract + the
 *     `SseHub` application logic. Pure; knows only the ports.
 *   - PORTS    `ports.ts` — `MessageBusPort` (broker) + `ConnectionSink` (client).
 *   - ADAPTERS `adapters/bus-message-port.ts` (IMessageBus → MessageBusPort),
 *     `adapters/http-server.ts` (Node http → hub, ServerResponse → ConnectionSink).
 *   - ROOT     this file — wires `connect(broker)` → `BusMessagePort` → `SseHub`
 *     → HTTP adapter. The CLI `main()` is the process entrypoint.
 *
 * It proves the broker is a horizontal-scaling backplane: a message published
 * from ANY instance reaches a client connected to a DIFFERENT instance, because
 * the server owns NO presence registry — it leans entirely on the broker's
 * topic routing (two transient subscriptions per connection: `user.<id>` and
 * `org.<id>` on the single `mbc.sse` topic).
 */

export interface SseServer {
  readonly port: number;
  readonly instanceId: string;
  close(): Promise<void>;
}

/** Start one SSE server instance bound to `port`, backed by `bus`. */
export async function startSseServer(opts: {
  port: number;
  instanceId: string;
  bus: IMessageBus;
}): Promise<SseServer> {
  const { port, instanceId, bus } = opts;

  const messageBus = new BusMessagePort(bus, instanceId);
  const hub = new SseHub(messageBus, instanceId);
  const http = await startHttpServer({ port, instanceId, hub });

  return {
    port: http.port,
    instanceId,
    close: () => http.close(),
  };
}

/** CLI entry: connect to a broker and run one server instance. */
async function main(): Promise<void> {
  const port = Number(process.env.PORT ?? 7001);
  const instanceId = process.env.INSTANCE_ID ?? `inst-${port}`;
  const broker = (process.env.BROKER ?? 'artemis') as BrokerId;
  if (!KNOWN_BROKERS.includes(broker)) {
    console.error(`Unknown BROKER '${broker}'. Known: ${KNOWN_BROKERS.join(', ')}`);
    process.exit(2);
  }

  const adapter = (await connect(broker)) as IMessageScheduler & IMessageBus;
  const srv = await startSseServer({ port, instanceId, bus: adapter });
  console.log(`SSE instance '${instanceId}' on http://localhost:${port} (broker=${broker})`);

  const stop = async () => {
    console.log(`\nshutting down ${instanceId}…`);
    try {
      await srv.close();
      await adapter[Symbol.asyncDispose]();
    } finally {
      process.exit(0);
    }
  };
  process.on('SIGINT', () => void stop());
  process.on('SIGTERM', () => void stop());
}

// Run only when executed directly (not when imported by the test).
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  void main();
}
