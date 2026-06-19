import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

/**
 * Launches TWO SSE server instances on ports 7001 and 7002 sharing the same
 * BROKER, so you can open two browser tabs (one per instance) and watch a
 * message published on one instance reach a client connected to the other.
 *
 * Cross-platform: spawns each instance through `tsx` (the same runner the `sse`
 * script uses), wiring PORT/INSTANCE_ID via env. Ctrl-C tears both down.
 */
const __dirname = dirname(fileURLToPath(import.meta.url));
const SERVER = join(__dirname, 'server.ts');
const BROKER = process.env.BROKER ?? 'artemis';

const instances = [
  { port: 7001, id: 'inst-7001' },
  { port: 7002, id: 'inst-7002' },
];

const npxCmd = process.platform === 'win32' ? 'npx.cmd' : 'npx';
const children = instances.map(({ port, id }) =>
  spawn(npxCmd, ['tsx', SERVER], {
    stdio: 'inherit',
    env: { ...process.env, PORT: String(port), INSTANCE_ID: id, BROKER },
  }),
);

console.log(
  `SSE cluster: :7001 and :7002 on broker=${BROKER}. Open http://localhost:7001/ and http://localhost:7002/`,
);

const stop = () => {
  for (const c of children) c.kill('SIGTERM');
  setTimeout(() => process.exit(0), 500);
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
for (const c of children) c.on('exit', stop);
