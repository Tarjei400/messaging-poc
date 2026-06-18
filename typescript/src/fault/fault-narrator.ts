import { HR, c } from '../ansi';

/**
 * Prints a fault-tolerance demo as a live, readable TIMELINE (not a pass/fail
 * table). Reuses the shared ANSI palette so it sits visually beside the
 * scenario report. Each line is a step in the story the broker is telling.
 */
export class FaultNarrator {
  constructor(private readonly title: string) {}

  header(): void {
    console.log('');
    console.log(`${c.bold}${c.cyan}${this.title}${c.reset}  —  fault-tolerance demo`);
    console.log(HR);
  }

  /** A normal step in the narrative. */
  step(text: string): void {
    console.log(`  ${c.dim}▸${c.reset} ${text}`);
  }

  /** A successful outcome. */
  ok(text: string, millis?: number): void {
    const t = millis === undefined ? '' : `  ${c.dim}${millis}ms${c.reset}`;
    console.log(`  ${c.green}✓${c.reset} ${text}${t}`);
  }

  /** A recovery / redelivery moment. */
  recover(text: string): void {
    console.log(`  ${c.cyan}⟳${c.reset} ${text}`);
  }

  /** A warning (retry attempt). */
  retry(text: string): void {
    console.log(`  ${c.yellow}⚠${c.reset} ${text}`);
  }

  /** Circuit-breaker state change. */
  breaker(text: string): void {
    console.log(`  ${c.magenta}◍${c.reset} ${text}`);
  }

  /** A dead-lettered message. */
  dead(text: string): void {
    console.log(`  ${c.red}☠${c.reset} ${text}`);
  }

  footer(demonstrated: number, failed: number): void {
    console.log(HR);
    console.log(
      `  ${c.green}${demonstrated} demonstrated${c.reset}, ` +
        `${failed ? c.red : c.dim}${failed} failed${c.reset}`,
    );
  }
}
