/**
 * The shared terminal vocabulary (colors + status glyphs) used by both the
 * scenario report and the fault-tolerance narration. Extracted here so the two
 * presenters stay visually consistent and we don't duplicate escape codes.
 */
export const c = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
  blue: '\x1b[34m',
};

export type ResultStatus = 'pass' | 'fail' | 'unsupported' | 'skipped';

export const GLYPH: Record<ResultStatus, string> = {
  pass: `${c.green}✓ pass${c.reset}`,
  fail: `${c.red}✗ fail${c.reset}`,
  unsupported: `${c.yellow}⊘ n/a ${c.reset}`,
  skipped: `${c.dim}- skip${c.reset}`,
};

export const HR = `${c.dim}${'─'.repeat(76)}${c.reset}`;

export const yn = (b: boolean): string => (b ? 'yes' : 'no');
