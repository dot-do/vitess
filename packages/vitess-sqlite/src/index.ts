/**
 * @dotdo/vitess-sqlite
 *
 * SQLite storage engine for Vitess.do using Turso/libSQL.
 * Provides a unified interface compatible with the Vitess RPC protocol.
 *
 * @example
 * ```ts
 * import { TursoAdapter } from '@dotdo/vitess-sqlite';
 *
 * const adapter = new TursoAdapter({
 *   url: 'libsql://my-database.turso.io',
 *   authToken: process.env.TURSO_AUTH_TOKEN,
 * });
 *
 * await adapter.connect();
 *
 * const result = await adapter.query('SELECT * FROM users WHERE id = ?', [1]);
 * console.log(result.rows);
 *
 * await adapter.close();
 * ```
 */

import type {
  StorageEngineType,
  QueryResult,
  ExecuteResult,
  BatchResult,
  Row,
  Field,
  TransactionOptions,
} from '@dotdo/vitess-rpc';

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for TursoAdapter
 */
export interface TursoAdapterConfig {
  /** Database URL - ':memory:', 'file:path', or 'libsql://...' */
  url: string;
  /** Auth token for remote Turso databases */
  authToken?: string;
  /** Sync URL for embedded replicas */
  syncUrl?: string;
  /** SQL dialect mode - 'sqlite' (default) or 'postgres' (auto-translate) */
  dialect?: 'sqlite' | 'postgres';
}

/**
 * Connection info (safe to expose, no secrets)
 */
export interface ConnectionInfo {
  url: string;
  authToken?: undefined; // Never exposed
  isEmbeddedReplica: boolean;
}

/**
 * Options for query/execute operations
 */
export interface QueryOptions {
  /** Transaction ID to execute within */
  txId?: string;
  /** Dialect mode for this query */
  dialect?: 'sqlite' | 'postgres';
}

/**
 * Batch statement
 */
export interface BatchStatement {
  sql: string;
  params?: unknown[];
}

/**
 * Batch options
 */
export interface BatchOptions extends QueryOptions {}

/**
 * Transaction callback context
 */
export interface TransactionContext {
  query<T extends Row = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
  execute(sql: string, params?: unknown[]): Promise<ExecuteResult>;
}

/**
 * Transaction begin options
 */
export interface BeginOptions extends TransactionOptions {
  /** Transaction mode: 'deferred' (default), 'immediate', or 'exclusive' */
  mode?: 'deferred' | 'immediate' | 'exclusive';
}

/**
 * Savepoint options
 */
export interface SavepointOptions {
  txId: string;
}

/**
 * Transaction callback options
 */
export interface TransactionCallbackOptions {
  dialect?: 'sqlite' | 'postgres';
}

/**
 * Event types for the adapter
 */
export type TursoAdapterEvents = {
  ready: () => void;
  close: () => void;
  error: (error: Error) => void;
  'transaction:begin': (txId: string) => void;
  'transaction:commit': (txId: string) => void;
  'transaction:rollback': (txId: string) => void;
};

// ============================================================================
// Errors
// ============================================================================

/**
 * Base error class for TursoAdapter
 */
export class TursoError extends Error {
  public readonly code: string;
  public readonly cause?: Error;

  constructor(message: string, code: string = 'TURSO_ERROR', options?: { cause?: Error }) {
    super(message);
    this.name = 'TursoError';
    this.code = code;
    this.cause = options?.cause;
    Error.captureStackTrace?.(this, this.constructor);
  }

  toJSON() {
    return {
      name: this.name,
      message: this.message,
      code: this.code,
      stack: this.stack,
    };
  }
}

/**
 * Connection-related errors
 */
export class ConnectionError extends TursoError {
  public readonly url?: string;

  constructor(message: string, url?: string, options?: { cause?: Error }) {
    super(message, 'CONNECTION_FAILED', options);
    this.name = 'ConnectionError';
    this.url = url;
  }
}

/**
 * Query execution errors
 */
export class QueryError extends TursoError {
  public readonly sql?: string;
  public readonly params?: unknown[];

  constructor(message: string, sql?: string, params?: unknown[], options?: { cause?: Error }) {
    super(message, 'QUERY_ERROR', options);
    this.name = 'QueryError';
    this.sql = sql;
    this.params = params;
  }
}

/**
 * SQL syntax errors
 */
export class SyntaxError extends QueryError {
  public readonly position?: number;

  constructor(
    message: string,
    sql?: string,
    params?: unknown[],
    position?: number,
    options?: { cause?: Error }
  ) {
    super(message, sql, params, options);
    this.name = 'SyntaxError';
    this.code = 'SYNTAX_ERROR';
    this.position = position;
  }
}

/**
 * Constraint violation errors
 */
export class ConstraintError extends TursoError {
  public readonly constraintType:
    | 'UNIQUE'
    | 'NOT_NULL'
    | 'CHECK'
    | 'FOREIGN_KEY'
    | 'PRIMARY_KEY'
    | 'UNKNOWN';

  constructor(
    message: string,
    constraintType:
      | 'UNIQUE'
      | 'NOT_NULL'
      | 'CHECK'
      | 'FOREIGN_KEY'
      | 'PRIMARY_KEY'
      | 'UNKNOWN' = 'UNKNOWN',
    options?: { cause?: Error }
  ) {
    super(message, 'CONSTRAINT_VIOLATION', options);
    this.name = 'ConstraintError';
    this.constraintType = constraintType;
  }
}

/**
 * Transaction-related errors
 */
export class TransactionError extends TursoError {
  public readonly txId?: string;

  constructor(message: string, code: string = 'TRANSACTION_NOT_FOUND', txId?: string, options?: { cause?: Error }) {
    super(message, code, options);
    this.name = 'TransactionError';
    this.txId = txId;
  }
}

// ============================================================================
// Parameter Translation
// ============================================================================

/**
 * Result of parameter translation
 */
export interface TranslateParamsResult {
  sql: string;
  params: unknown[];
}

/**
 * Translates PostgreSQL-style $1, $2, ... placeholders to SQLite ? placeholders.
 * Also handles parameter reordering when placeholders are used out of order.
 *
 * @param sql - SQL statement with $n placeholders
 * @param params - Array of parameters or named params object
 * @returns Translated SQL and reordered params
 *
 * @example
 * ```ts
 * const result = translateParams('SELECT * FROM users WHERE id = $1 AND name = $2', [1, 'Alice']);
 * // result.sql = 'SELECT * FROM users WHERE id = ? AND name = ?'
 * // result.params = [1, 'Alice']
 *
 * // Out of order:
 * const result2 = translateParams('INSERT INTO t (b, a) VALUES ($2, $1)', ['a', 'b']);
 * // result2.sql = 'INSERT INTO t (b, a) VALUES (?, ?)'
 * // result2.params = ['b', 'a']
 * ```
 */
export function translateParams(
  sql: string,
  params?: unknown[] | Record<string, unknown>
): TranslateParamsResult {
  // TODO: Implement placeholder translation
  // This is a stub that will fail tests (TDD Red phase)
  throw new Error('translateParams not implemented');
}

// ============================================================================
// SQL Dialect Translation
// ============================================================================

/**
 * Translates PostgreSQL SQL syntax to SQLite-compatible SQL.
 *
 * @param sql - PostgreSQL-style SQL statement
 * @returns SQLite-compatible SQL statement
 *
 * @example
 * ```ts
 * const sqlite = translatePostgresToSQLite('CREATE TABLE t (id SERIAL PRIMARY KEY)');
 * // sqlite = 'CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)'
 * ```
 */
export function translatePostgresToSQLite(sql: string): string {
  // TODO: Implement dialect translation
  // This is a stub that will fail tests (TDD Red phase)
  throw new Error('translatePostgresToSQLite not implemented');
}

// ============================================================================
// TursoAdapter
// ============================================================================

/**
 * SQLite storage engine adapter using Turso/libSQL.
 *
 * Implements the Vitess storage engine interface for SQLite databases,
 * with support for local files, in-memory databases, and Turso cloud.
 */
export class TursoAdapter {
  private readonly config: TursoAdapterConfig;
  private ready: boolean = false;
  private eventHandlers: Map<string, Set<(...args: unknown[]) => void>> = new Map();

  /**
   * Storage engine type identifier
   */
  public readonly engineType: StorageEngineType = 'sqlite';

  constructor(config: TursoAdapterConfig) {
    // Validate config
    if (!config.url) {
      throw new Error('URL is required');
    }

    // Validate auth token for cloud URLs
    if (this.isCloudUrl(config.url) && !config.authToken) {
      throw new Error('authToken is required for remote databases');
    }

    this.config = config;
  }

  private isCloudUrl(url: string): boolean {
    return url.startsWith('libsql://') || url.startsWith('https://');
  }

  /**
   * Check if the adapter is connected and ready
   */
  isReady(): boolean {
    return this.ready;
  }

  /**
   * Connect to the database
   */
  async connect(): Promise<TursoAdapter> {
    // TODO: Implement connection logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('connect not implemented');
  }

  /**
   * Close the database connection
   */
  async close(): Promise<void> {
    // TODO: Implement close logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('close not implemented');
  }

  /**
   * Get connection information (safe to expose)
   */
  getConnectionInfo(): ConnectionInfo {
    return {
      url: this.config.url,
      isEmbeddedReplica: !!this.config.syncUrl,
    };
  }

  /**
   * Execute a SELECT query
   */
  async query<T extends Row = Row>(
    sql: string,
    params?: unknown[],
    options?: QueryOptions
  ): Promise<QueryResult<T>> {
    // TODO: Implement query logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('query not implemented');
  }

  /**
   * Execute an INSERT/UPDATE/DELETE statement
   */
  async execute(
    sql: string,
    params?: unknown[],
    options?: QueryOptions
  ): Promise<ExecuteResult> {
    // TODO: Implement execute logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('execute not implemented');
  }

  /**
   * Execute multiple statements in a batch
   */
  async batch(statements: BatchStatement[], options?: BatchOptions): Promise<BatchResult> {
    // TODO: Implement batch logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('batch not implemented');
  }

  /**
   * Begin a new transaction
   */
  async begin(options?: BeginOptions): Promise<string> {
    // TODO: Implement begin logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('begin not implemented');
  }

  /**
   * Commit a transaction
   */
  async commit(txId: string): Promise<void> {
    // TODO: Implement commit logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('commit not implemented');
  }

  /**
   * Rollback a transaction
   */
  async rollback(txId: string): Promise<void> {
    // TODO: Implement rollback logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('rollback not implemented');
  }

  /**
   * Create a savepoint within a transaction
   */
  async savepoint(name: string, options: SavepointOptions): Promise<void> {
    // TODO: Implement savepoint logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('savepoint not implemented');
  }

  /**
   * Rollback to a savepoint
   */
  async rollbackToSavepoint(name: string, options: SavepointOptions): Promise<void> {
    // TODO: Implement rollbackToSavepoint logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('rollbackToSavepoint not implemented');
  }

  /**
   * Release a savepoint
   */
  async releaseSavepoint(name: string, options: SavepointOptions): Promise<void> {
    // TODO: Implement releaseSavepoint logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('releaseSavepoint not implemented');
  }

  /**
   * Execute a transaction with a callback (auto-commit/rollback)
   */
  async transaction<T>(
    callback: (tx: TransactionContext) => Promise<T>,
    options?: TransactionCallbackOptions
  ): Promise<T> {
    // TODO: Implement transaction callback logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('transaction not implemented');
  }

  /**
   * Get list of active transaction IDs
   */
  getActiveTransactions(): string[] {
    // TODO: Implement getActiveTransactions logic
    // This is a stub that will fail tests (TDD Red phase)
    throw new Error('getActiveTransactions not implemented');
  }

  /**
   * Register an event handler
   */
  on<K extends keyof TursoAdapterEvents>(
    event: K,
    handler: TursoAdapterEvents[K]
  ): void {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, new Set());
    }
    this.eventHandlers.get(event)!.add(handler as (...args: unknown[]) => void);
  }

  /**
   * Remove an event handler
   */
  off<K extends keyof TursoAdapterEvents>(
    event: K,
    handler: TursoAdapterEvents[K]
  ): void {
    this.eventHandlers.get(event)?.delete(handler as (...args: unknown[]) => void);
  }

  /**
   * Emit an event
   */
  protected emit<K extends keyof TursoAdapterEvents>(
    event: K,
    ...args: Parameters<TursoAdapterEvents[K]>
  ): void {
    this.eventHandlers.get(event)?.forEach((handler) => {
      try {
        handler(...args);
      } catch {
        // Ignore handler errors
      }
    });
  }
}

// Re-export types from vitess-rpc for convenience
export type {
  StorageEngineType,
  QueryResult,
  ExecuteResult,
  BatchResult,
  Row,
  Field,
  TransactionOptions,
} from '@dotdo/vitess-rpc';
