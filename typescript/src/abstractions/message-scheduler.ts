import {
  Capabilities,
  Destination,
  MessageHandler,
  ScheduleHandle,
  ScheduledInfo,
  Subscription,
} from './types';

/**
 * The single seam the whole project hangs off (Dependency Inversion).
 *
 * Application code, the scenario runner, and the tests all depend on THIS, never
 * on a concrete broker client. Adding a broker means adding an adapter — no
 * existing code changes (Open/Closed). Each adapter has exactly one reason to
 * change: its broker (Single Responsibility).
 *
 * Contract notes:
 *  - `cancel` and `listScheduled` MUST throw `NotSupportedError` (never silently
 *    no-op) when `capabilities` says they are unsupported.
 *  - `schedule` with a past `deliverAt` is adapter-defined; adapters should
 *    document their behavior. The scenarios only ever schedule into the future.
 */
export interface IMessageScheduler extends AsyncDisposable {
  /** Human-readable adapter name, e.g. "Apache ActiveMQ Artemis". */
  readonly name: string;

  /** What this adapter can and cannot do, used to score the comparison. */
  readonly capabilities: Capabilities;

  /** Establish the connection and provision any required broker topology. */
  connect(): Promise<void>;

  /** Publish a message for immediate delivery. */
  sendNow(destination: Destination, payload: string): Promise<void>;

  /** Publish a message to be delivered at `deliverAt`. */
  schedule(
    destination: Destination,
    payload: string,
    deliverAt: Date,
  ): Promise<ScheduleHandle>;

  /** Cancel a previously scheduled message before it fires.
   *  @throws NotSupportedError if `capabilities.supportsCancel` is false. */
  cancel(handle: ScheduleHandle): Promise<void>;

  /** List scheduled-but-not-yet-delivered messages for a destination.
   *  @throws NotSupportedError if `capabilities.supportsList` is false. */
  listScheduled(destination: Destination): Promise<ScheduledInfo[]>;

  /** Begin consuming messages from a destination. */
  consume(
    destination: Destination,
    handler: MessageHandler,
  ): Promise<Subscription>;
}
