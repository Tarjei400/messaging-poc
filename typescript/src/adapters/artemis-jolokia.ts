/**
 * Minimal Jolokia (HTTP/JSON over JMX) client for the Artemis broker.
 *
 * Artemis exposes scheduled-message management through its `QueueControl` MBean
 * (`listScheduledMessages`, `removeMessages`). The AMQP wire protocol has no
 * notion of "cancel a scheduled message", so the real cancellation story goes
 * through management — and Jolokia is the same JSON endpoint from .NET and TS,
 * which keeps the two adapters symmetric.
 */
export interface JolokiaOptions {
  baseUrl: string; // e.g. http://localhost:8161/console/jolokia
  username: string;
  password: string;
}

export class JolokiaClient {
  constructor(private readonly opts: JolokiaOptions) {}

  private async post(body: unknown): Promise<any> {
    const res = await fetch(this.opts.baseUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization:
          'Basic ' +
          Buffer.from(`${this.opts.username}:${this.opts.password}`).toString(
            'base64',
          ),
        // Jolokia requires a non-empty Origin/Referer when strict checking is on.
        Origin: 'http://localhost',
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`Jolokia HTTP ${res.status}: ${await res.text()}`);
    }
    const json = await res.json();
    if (json.status !== 200) {
      throw new Error(`Jolokia error ${json.status}: ${json.error ?? 'unknown'}`);
    }
    return json.value;
  }

  /** Resolve the full ObjectName of the anycast queue backing `queueName`. */
  async findQueueMBean(queueName: string): Promise<string | undefined> {
    const pattern =
      `org.apache.activemq.artemis:broker=*,component=addresses,` +
      `address="${queueName}",subcomponent=queues,routing-type="anycast",` +
      `queue="${queueName}"`;
    const value = (await this.post({ type: 'search', mbean: pattern })) as
      | string[]
      | undefined;
    return value && value.length ? value[0] : undefined;
  }

  /** Returns the raw list of scheduled messages currently held by the queue. */
  async listScheduledMessages(queueName: string): Promise<any[]> {
    const mbean = await this.findQueueMBean(queueName);
    if (!mbean) return [];
    const value = await this.post({
      type: 'exec',
      mbean,
      operation: 'listScheduledMessages()',
    });
    return Array.isArray(value) ? value : [];
  }

  /** Removes messages matching an Artemis filter; returns the number removed. */
  async removeMessages(queueName: string, filter: string): Promise<number> {
    const mbean = await this.findQueueMBean(queueName);
    if (!mbean) return 0;
    const value = await this.post({
      type: 'exec',
      mbean,
      operation: 'removeMessages(java.lang.String)',
      arguments: [filter],
    });
    return typeof value === 'number' ? value : 0;
  }
}
