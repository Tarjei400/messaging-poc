/**
 * Thrown by an adapter when a caller invokes an operation the broker cannot
 * honor (e.g. cancelling a scheduled message on RabbitMQ's delayed plugin).
 *
 * Having a *dedicated* error type — rather than returning null or false — lets
 * the scenario runner distinguish "honestly unsupported" from "tried and broke",
 * and keeps the port's contract explicit (Liskov: every adapter either fulfils
 * the operation or fails in this one well-known way).
 */
export class NotSupportedError extends Error {
  constructor(
    public readonly operation: string,
    public readonly broker: string,
    reason?: string,
  ) {
    super(
      `Operation '${operation}' is not supported by ${broker}` +
        (reason ? `: ${reason}` : ''),
    );
    this.name = 'NotSupportedError';
  }
}
