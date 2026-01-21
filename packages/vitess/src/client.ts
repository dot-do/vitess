/**
 * VitessClient - Unified client for Vitess.do
 *
 * Backend-agnostic client that works with both PostgreSQL (PGlite)
 * and SQLite (Turso) storage engines. The client communicates with
 * VTGate via CapnWeb RPC and doesn't need to know the underlying backend.
 */

import type {
  QueryResult,
  ExecuteResult,
  BatchResult,
  Row,
  TransactionOptions,
  ClusterStatus,
  VSchema,
  StorageEngineType,
} from '@dotdo/vitess-rpc';
import {
  MessageType,
  createQueryRequest,
  createExecuteRequest,
  createMessageId,
} from '@dotdo/vitess-rpc';

/**
 * Client configuration
 */
export interface VitessConfig {
  /** VTGate endpoint URL */
  endpoint: string;
  /** Default keyspace */
  keyspace?: string;
  /** Authentication token */
  token?: string;
  /** Request timeout in milliseconds */
  timeout?: number;
  /** Retry configuration */
  retry?: {
    maxAttempts: number;
    backoffMs: number;
  };
}

/**
 * Transaction handle for multi-statement transactions
 */
export interface Transaction {
  /** Transaction ID */
  readonly id: string;
  /** Shards involved in this transaction */
  readonly shards: string[];

  /** Execute a query within the transaction */
  query<T extends Row = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;

  /** Execute a write statement within the transaction */
  execute(sql: string, params?: unknown[]): Promise<ExecuteResult>;

  /** Commit the transaction */
  commit(): Promise<void>;

  /** Rollback the transaction */
  rollback(): Promise<void>;
}

/**
 * VitessClient - Main client interface
 */
export class VitessClient {
  private config: VitessConfig;
  private connected = false;

  constructor(config: VitessConfig) {
    this.config = {
      timeout: 30000,
      retry: { maxAttempts: 3, backoffMs: 100 },
      ...config,
    };
  }

  /**
   * Connect to VTGate
   */
  async connect(): Promise<void> {
    // Verify connection with a health check
    const response = await this.request({
      type: MessageType.HEALTH,
      id: createMessageId(),
      timestamp: Date.now(),
    });

    if (response.type === MessageType.ERROR) {
      throw new Error(`Connection failed: ${response.message}`);
    }

    this.connected = true;
  }

  /**
   * Disconnect from VTGate
   */
  async disconnect(): Promise<void> {
    this.connected = false;
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Execute a read query
   *
   * @example
   * ```ts
   * const result = await client.query<User>('SELECT * FROM users WHERE id = $1', [userId]);
   * for (const user of result.rows) {
   *   console.log(user.name);
   * }
   * ```
   */
  async query<T extends Row = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    const request = createQueryRequest(sql, params, { keyspace: this.config.keyspace });
    const response = await this.request(request);

    if (response.type === MessageType.ERROR) {
      throw new VitessError(response.code, response.message, response.shard);
    }

    return response.result as QueryResult<T>;
  }

  /**
   * Execute a write statement (INSERT, UPDATE, DELETE)
   *
   * @example
   * ```ts
   * const result = await client.execute(
   *   'INSERT INTO users (name, email) VALUES ($1, $2)',
   *   ['Alice', 'alice@example.com']
   * );
   * console.log(`Inserted ${result.affected} rows`);
   * ```
   */
  async execute(sql: string, params?: unknown[]): Promise<ExecuteResult> {
    const request = createExecuteRequest(sql, params, { keyspace: this.config.keyspace });
    const response = await this.request(request);

    if (response.type === MessageType.ERROR) {
      throw new VitessError(response.code, response.message, response.shard);
    }

    return response.result as ExecuteResult;
  }

  /**
   * Execute multiple statements in a batch
   *
   * @example
   * ```ts
   * const result = await client.batch([
   *   { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Alice'] },
   *   { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Bob'] },
   * ]);
   * ```
   */
  async batch(
    statements: Array<{ sql: string; params?: unknown[] }>
  ): Promise<BatchResult> {
    const request = {
      type: MessageType.BATCH as const,
      id: createMessageId(),
      timestamp: Date.now(),
      statements,
      keyspace: this.config.keyspace,
    };
    const response = await this.request(request);

    if (response.type === MessageType.ERROR) {
      throw new VitessError(response.code, response.message, response.shard);
    }

    return response.result as BatchResult;
  }

  /**
   * Execute statements within a transaction
   *
   * @example
   * ```ts
   * await client.transaction(async (tx) => {
   *   await tx.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, fromId]);
   *   await tx.execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, toId]);
   * });
   * ```
   */
  async transaction<T>(
    fn: (tx: Transaction) => Promise<T>,
    options?: TransactionOptions
  ): Promise<T> {
    // Begin transaction
    const beginRequest = {
      type: MessageType.BEGIN as const,
      id: createMessageId(),
      timestamp: Date.now(),
      keyspace: this.config.keyspace,
      options,
    };
    const beginResponse = await this.request(beginRequest);

    if (beginResponse.type === MessageType.ERROR) {
      throw new VitessError(beginResponse.code, beginResponse.message);
    }

    const { txId, shards } = beginResponse as { txId: string; shards: string[] };

    // Create transaction handle
    const tx: Transaction = {
      id: txId,
      shards,

      query: async <R extends Row = Row>(sql: string, params?: unknown[]) => {
        const req = createQueryRequest(sql, params, {
          keyspace: this.config.keyspace,
          txId,
        });
        const res = await this.request(req);
        if (res.type === MessageType.ERROR) {
          throw new VitessError(res.code, res.message, res.shard);
        }
        return res.result as QueryResult<R>;
      },

      execute: async (sql: string, params?: unknown[]) => {
        const req = createExecuteRequest(sql, params, {
          keyspace: this.config.keyspace,
          txId,
        });
        const res = await this.request(req);
        if (res.type === MessageType.ERROR) {
          throw new VitessError(res.code, res.message, res.shard);
        }
        return res.result as ExecuteResult;
      },

      commit: async () => {
        const req = {
          type: MessageType.COMMIT as const,
          id: createMessageId(),
          timestamp: Date.now(),
          txId,
        };
        const res = await this.request(req);
        if (res.type === MessageType.ERROR) {
          throw new VitessError(res.code, res.message);
        }
      },

      rollback: async () => {
        const req = {
          type: MessageType.ROLLBACK as const,
          id: createMessageId(),
          timestamp: Date.now(),
          txId,
        };
        await this.request(req);
      },
    };

    // Execute transaction function
    try {
      const result = await fn(tx);
      await tx.commit();
      return result;
    } catch (error) {
      await tx.rollback();
      throw error;
    }
  }

  /**
   * Get cluster status
   */
  async status(): Promise<ClusterStatus> {
    const request = {
      type: MessageType.STATUS as const,
      id: createMessageId(),
      timestamp: Date.now(),
      keyspace: this.config.keyspace,
    };
    const response = await this.request(request);

    if (response.type === MessageType.ERROR) {
      throw new VitessError(response.code, response.message);
    }

    return (response as any).status;
  }

  /**
   * Get VSchema configuration
   */
  async vschema(): Promise<VSchema> {
    const request = {
      type: MessageType.VSCHEMA as const,
      id: createMessageId(),
      timestamp: Date.now(),
      keyspace: this.config.keyspace,
    };
    const response = await this.request(request);

    if (response.type === MessageType.ERROR) {
      throw new VitessError(response.code, response.message);
    }

    return (response as any).vschema;
  }

  /**
   * Send RPC request to VTGate
   */
  private async request(message: any): Promise<any> {
    const { maxAttempts, backoffMs } = this.config.retry!;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const response = await fetch(this.config.endpoint, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            ...(this.config.token && { Authorization: `Bearer ${this.config.token}` }),
          },
          body: JSON.stringify(message),
          signal: AbortSignal.timeout(this.config.timeout!),
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        return await response.json();
      } catch (error) {
        if (attempt === maxAttempts) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, backoffMs * attempt));
      }
    }
  }
}

/**
 * Vitess-specific error
 */
export class VitessError extends Error {
  constructor(
    public readonly code: string,
    message: string,
    public readonly shard?: string
  ) {
    super(message);
    this.name = 'VitessError';
  }
}

/**
 * Create a Vitess client
 *
 * @example
 * ```ts
 * import { createClient } from '@dotdo/vitess';
 *
 * const client = createClient({
 *   endpoint: 'https://my-app.vitess.do',
 *   keyspace: 'main',
 * });
 *
 * await client.connect();
 * const users = await client.query('SELECT * FROM users');
 * ```
 */
export function createClient(config: VitessConfig): VitessClient {
  return new VitessClient(config);
}
