/**
 * Documentation generator for the SSE event contract.
 *
 * Reads `SSE_CONTRACT` from `sse/domain/contract.ts` (the single source of
 * truth) and emits two artifacts into the repo `docs/` folder:
 *   - `docs/sse-events.md`     — a human-readable event catalog.
 *   - `docs/sse-asyncapi.yaml`  — an AsyncAPI 2.6 document.
 *
 * Both are DERIVED from the contract object — no hand-written content is
 * duplicated, so the docs cannot drift from code. Zero npm dependencies: the
 * AsyncAPI YAML is hand-rendered with Node built-ins only.
 *
 * Run with: `npm run sse:docs` (→ `tsx src/sse/docs/generate.ts`).
 */

import { writeFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { FieldDoc, SSE_CONTRACT, SseContract } from '../domain/contract';

const __dirname = dirname(fileURLToPath(import.meta.url));
// repo docs/ folder: ../../../../docs from typescript/src/sse/docs/
const DOCS_DIR = join(__dirname, '..', '..', '..', '..', 'docs');

// ---------------------------------------------------------------------------
// Markdown rendering
// ---------------------------------------------------------------------------

function fieldTable(fields: readonly FieldDoc[]): string {
  const head = '| Field | Type | Description |\n| --- | --- | --- |';
  const rows = fields.map((f) => `| \`${f.name}\` | \`${f.type}\` | ${f.description} |`);
  return [head, ...rows].join('\n');
}

function renderMarkdown(c: SseContract): string {
  const lines: string[] = [];
  lines.push('# SSE event catalog');
  lines.push('');
  lines.push('> Generated from `typescript/src/sse/domain/contract.ts` via `npm run sse:docs`. Do not edit by hand.');
  lines.push('');
  lines.push(`## Channel \`${c.channel}\``);
  lines.push('');
  lines.push(c.channelDescription);
  lines.push('');

  lines.push('## Address schemes');
  lines.push('');
  lines.push('| Scope | Routing key | Description |');
  lines.push('| --- | --- | --- |');
  for (const a of c.addressSchemes) {
    lines.push(`| \`${a.scope}\` | \`${a.pattern}\` | ${a.description} |`);
  }
  lines.push('');

  lines.push('## Operations');
  lines.push('');
  for (const op of c.operations) {
    lines.push(`### \`${op.id}\` (${op.direction})`);
    lines.push('');
    lines.push(op.summary);
    lines.push('');
    lines.push(`- **Direction:** ${op.direction}`);
    lines.push(`- **Routing key:** \`${op.routingKeyPattern}\``);
    lines.push(`- **When:** ${op.when}`);
    lines.push('');
  }

  lines.push('## Message envelope');
  lines.push('');
  lines.push('The payload published to the channel and re-emitted as the `message` of an SSE frame.');
  lines.push('');
  lines.push(fieldTable(c.envelopeFields));
  lines.push('');

  lines.push('## SSE frame');
  lines.push('');
  lines.push('The `data:` frame delivered to a connected client.');
  lines.push('');
  lines.push(fieldTable(c.frameFields));
  lines.push('');

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// AsyncAPI 2.6 rendering (hand-rolled YAML — no dependency)
// ---------------------------------------------------------------------------

/** Quote a scalar for YAML when it could be misread; keep simple ids bare. */
function yamlScalar(value: string): string {
  if (/^[A-Za-z0-9_.\-]+$/.test(value)) return value;
  return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

function renderAsyncApi(c: SseContract): string {
  const L: string[] = [];
  L.push('asyncapi: 2.6.0');
  L.push('info:');
  L.push('  title: SSE cross-instance cluster');
  L.push('  version: 1.0.0');
  L.push(`  description: ${yamlScalar(c.channelDescription)}`);
  L.push('defaultContentType: application/json');
  // AsyncAPI 2.6 channel operations: `publish` = the app RECEIVES a message,
  // `subscribe` = the app SENDS one. We model one channel per routing-key
  // pattern (`mbc.sse` with a `{scope}Id` parameter) and group every contract
  // operation that shares that pattern under it, so each channel key is unique.
  const byPattern = new Map<string, typeof c.operations[number][]>();
  for (const op of c.operations) {
    const list = byPattern.get(op.routingKeyPattern) ?? [];
    list.push(op);
    byPattern.set(op.routingKeyPattern, list);
  }

  L.push('channels:');
  for (const [pattern, ops] of byPattern) {
    L.push(`  ${yamlScalar(pattern)}:`);
    L.push(`    description: ${yamlScalar(`Channel ${c.channel}, routing key ${pattern}.`)}`);
    L.push('    parameters:');
    // Each pattern carries exactly one `{...Id}` parameter.
    const paramScheme = c.addressSchemes.find((a) => pattern.startsWith(`${a.scope}.`));
    if (paramScheme) {
      L.push(`      ${paramScheme.scope}Id:`);
      L.push(`        description: ${yamlScalar(paramScheme.description)}`);
      L.push('        schema:');
      L.push('          type: string');
    }
    for (const op of ops) {
      const verb = op.direction === 'subscribe' ? 'subscribe' : 'publish';
      L.push(`    ${verb}:`);
      L.push(`      operationId: ${yamlScalar(op.id)}`);
      L.push(`      summary: ${yamlScalar(op.summary)}`);
      L.push(`      description: ${yamlScalar(op.when)}`);
      L.push('      message:');
      L.push(`        $ref: '#/components/messages/${verb === 'subscribe' ? 'SseFrame' : 'SseEnvelope'}'`);
    }
  }

  L.push('components:');
  L.push('  messages:');
  L.push('    SseEnvelope:');
  L.push('      name: SseEnvelope');
  L.push('      title: Published message envelope');
  L.push('      contentType: application/json');
  L.push("      payload:");
  L.push("        $ref: '#/components/schemas/SseEnvelope'");
  L.push('    SseFrame:');
  L.push('      name: SseFrame');
  L.push('      title: SSE data frame delivered to a client');
  L.push('      contentType: application/json');
  L.push("      payload:");
  L.push("        $ref: '#/components/schemas/SseFrame'");
  L.push('  schemas:');
  L.push('    SseEnvelope:');
  L.push('      type: object');
  L.push('      properties:');
  for (const f of c.envelopeFields) {
    L.push(`        ${f.name}:`);
    L.push(`          type: ${asyncApiType(f.type)}`);
    L.push(`          description: ${yamlScalar(f.description)}`);
  }
  L.push('    SseFrame:');
  L.push('      type: object');
  L.push('      properties:');
  for (const f of c.frameFields) {
    L.push(`        ${f.name}:`);
    if (f.name === 'message') {
      L.push("          $ref: '#/components/schemas/SseEnvelope'");
    } else {
      L.push(`          type: ${asyncApiType(f.type)}`);
      L.push(`          description: ${yamlScalar(f.description)}`);
    }
  }

  return L.join('\n') + '\n';
}

/** Coarse map of our contract type strings to JSON-schema primitive types. */
function asyncApiType(type: string): string {
  if (type.startsWith('object') || type === 'SseEnvelope') return 'object';
  // string, ISO-8601 string, and the `"user" | "org"` enum all serialize as strings.
  return 'string';
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  await mkdir(DOCS_DIR, { recursive: true });
  const mdPath = join(DOCS_DIR, 'sse-events.md');
  const yamlPath = join(DOCS_DIR, 'sse-asyncapi.yaml');
  await writeFile(mdPath, renderMarkdown(SSE_CONTRACT), 'utf8');
  await writeFile(yamlPath, renderAsyncApi(SSE_CONTRACT), 'utf8');
  console.log(`wrote ${mdPath}`);
  console.log(`wrote ${yamlPath}`);
}

void main();
