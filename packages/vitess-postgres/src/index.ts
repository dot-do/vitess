/**
 * @dotdo/vitess-postgres
 *
 * PostgreSQL storage engine for Vitess.do using PGlite
 */

import { PGlite } from '@electric-sql/pglite';
import type {
  QueryResult,
  ExecuteResult,
  Row,
  Field,
  TransactionOptions,
  IsolationLevel,
} from '@dotdo/vitess-rpc';

/**
 * PGlite adapter options
 */
export interface PGliteAdapterOptions {
  /** Data directory path (optional, defaults to in-memory) */
  dataDir?: string;
  /** Enable debug logging */
  debug?: boolean;
  /** Initial schema to execute on init */
  initSchema?: string;
}

/**
 * Transaction handle for PGlite
 */
export interface PGliteTransaction {
  /** Transaction ID */
  readonly id: string;
  /** Check if transaction is active */
  readonly active: boolean;
  /** Execute a query within this transaction */
  query<T extends Row = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
  /** Execute a write statement within this transaction */
  execute(sql: string, params?: unknown[]): Promise<ExecuteResult>;
  /** Commit the transaction */
  commit(): Promise<void>;
  /** Rollback the transaction */
  rollback(): Promise<void>;
}

/**
 * PGlite adapter error codes
 */
export enum PGliteErrorCode {
  CONNECTION_ERROR = 'PGLITE_CONNECTION_ERROR',
  QUERY_ERROR = 'PGLITE_QUERY_ERROR',
  TRANSACTION_ERROR = 'PGLITE_TRANSACTION_ERROR',
  TYPE_ERROR = 'PGLITE_TYPE_ERROR',
  CONSTRAINT_VIOLATION = 'PGLITE_CONSTRAINT_VIOLATION',
  SYNTAX_ERROR = 'PGLITE_SYNTAX_ERROR',
  NOT_READY = 'PGLITE_NOT_READY',
  ALREADY_CLOSED = 'PGLITE_ALREADY_CLOSED',
}

/**
 * PGlite adapter error
 */
export class PGliteAdapterError extends Error {
  constructor(
    public readonly code: PGliteErrorCode,
    message: string,
    public readonly cause?: Error,
    public readonly sqlState?: string,
  ) {
    super(message);
    this.name = 'PGliteAdapterError';
  }
}

/**
 * PostgreSQL type OIDs
 * See: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
 */
const PG_TYPE_OID = {
  BOOL: 16,
  BYTEA: 17,
  CHAR: 18,
  INT8: 20,
  INT2: 21,
  INT4: 23,
  TEXT: 25,
  OID: 26,
  JSON: 114,
  XML: 142,
  FLOAT4: 700,
  FLOAT8: 701,
  MONEY: 790,
  MACADDR: 829,
  INET: 869,
  CIDR: 650,
  BPCHAR: 1042,
  VARCHAR: 1043,
  DATE: 1082,
  TIME: 1083,
  TIMESTAMP: 1114,
  TIMESTAMPTZ: 1184,
  TIMETZ: 1266,
  INTERVAL: 1186,
  NUMERIC: 1700,
  UUID: 2950,
  JSONB: 3802,
  INT4_ARRAY: 1007,
  TEXT_ARRAY: 1009,
  INT8_ARRAY: 1016,
  FLOAT4_ARRAY: 1021,
  FLOAT8_ARRAY: 1022,
  VARCHAR_ARRAY: 1015,
  BOOL_ARRAY: 1000,
} as const;

/**
 * Map Postgres type OID to type name
 */
function getTypeName(dataTypeID: number): string {
  switch (dataTypeID) {
    case PG_TYPE_OID.BOOL:
      return 'bool';
    case PG_TYPE_OID.INT2:
      return 'int2';
    case PG_TYPE_OID.INT4:
      return 'int4';
    case PG_TYPE_OID.INT8:
      return 'int8';
    case PG_TYPE_OID.FLOAT4:
      return 'float4';
    case PG_TYPE_OID.FLOAT8:
      return 'float8';
    case PG_TYPE_OID.NUMERIC:
      return 'numeric';
    case PG_TYPE_OID.TEXT:
      return 'text';
    case PG_TYPE_OID.VARCHAR:
      return 'varchar';
    case PG_TYPE_OID.BPCHAR:
      return 'char';
    case PG_TYPE_OID.CHAR:
      return 'char';
    case PG_TYPE_OID.DATE:
      return 'date';
    case PG_TYPE_OID.TIME:
      return 'time';
    case PG_TYPE_OID.TIMETZ:
      return 'timetz';
    case PG_TYPE_OID.TIMESTAMP:
      return 'timestamp';
    case PG_TYPE_OID.TIMESTAMPTZ:
      return 'timestamptz';
    case PG_TYPE_OID.INTERVAL:
      return 'interval';
    case PG_TYPE_OID.BYTEA:
      return 'bytea';
    case PG_TYPE_OID.JSON:
      return 'json';
    case PG_TYPE_OID.JSONB:
      return 'jsonb';
    case PG_TYPE_OID.UUID:
      return 'uuid';
    case PG_TYPE_OID.INET:
      return 'inet';
    case PG_TYPE_OID.CIDR:
      return 'cidr';
    case PG_TYPE_OID.MACADDR:
      return 'macaddr';
    case PG_TYPE_OID.INT4_ARRAY:
      return 'int4[]';
    case PG_TYPE_OID.INT8_ARRAY:
      return 'int8[]';
    case PG_TYPE_OID.TEXT_ARRAY:
      return 'text[]';
    case PG_TYPE_OID.FLOAT4_ARRAY:
      return 'float4[]';
    case PG_TYPE_OID.FLOAT8_ARRAY:
      return 'float8[]';
    case PG_TYPE_OID.VARCHAR_ARRAY:
      return 'varchar[]';
    case PG_TYPE_OID.BOOL_ARRAY:
      return 'bool[]';
    case PG_TYPE_OID.OID:
      return 'oid';
    case PG_TYPE_OID.MONEY:
      return 'money';
    case PG_TYPE_OID.XML:
      return 'xml';
    default:
      return `unknown(${dataTypeID})`;
  }
}

/**
 * Classify a PostgreSQL error and return appropriate error code
 */
function classifyPgError(error: unknown): { code: PGliteErrorCode; sqlState?: string } {
  if (!(error instanceof Error)) {
    return { code: PGliteErrorCode.QUERY_ERROR };
  }

  const message = error.message.toLowerCase();
  const pgError = error as Error & { code?: string; sqlstate?: string; severity?: string };
  const sqlState = pgError.code || pgError.sqlstate;

  // Check SQL state codes
  if (sqlState) {
    // Class 23 - Integrity Constraint Violation
    if (sqlState.startsWith('23')) {
      return { code: PGliteErrorCode.CONSTRAINT_VIOLATION, sqlState };
    }
    // Class 42 - Syntax Error or Access Rule Violation
    if (sqlState.startsWith('42')) {
      // 42601 is syntax error, 42P01 is undefined table, etc.
      if (sqlState === '42601') {
        return { code: PGliteErrorCode.SYNTAX_ERROR, sqlState };
      }
      return { code: PGliteErrorCode.QUERY_ERROR, sqlState };
    }
    // Class 22 - Data Exception (type errors)
    if (sqlState.startsWith('22')) {
      return { code: PGliteErrorCode.TYPE_ERROR, sqlState };
    }
  }

  // Fallback to message-based classification
  if (
    message.includes('syntax error') ||
    message.includes('at or near') ||
    message.includes('unexpected')
  ) {
    return { code: PGliteErrorCode.SYNTAX_ERROR, sqlState };
  }

  if (
    message.includes('violates') ||
    message.includes('constraint') ||
    message.includes('unique') ||
    message.includes('foreign key') ||
    message.includes('not-null') ||
    message.includes('null value')
  ) {
    return { code: PGliteErrorCode.CONSTRAINT_VIOLATION, sqlState };
  }

  if (
    message.includes('does not exist') ||
    message.includes('relation') ||
    message.includes('column') ||
    message.includes('ambiguous')
  ) {
    return { code: PGliteErrorCode.QUERY_ERROR, sqlState };
  }

  if (
    message.includes('invalid input') ||
    message.includes('cannot be cast') ||
    message.includes('invalid')
  ) {
    return { code: PGliteErrorCode.TYPE_ERROR, sqlState };
  }

  return { code: PGliteErrorCode.QUERY_ERROR, sqlState };
}

/**
 * Internal transaction implementation
 */
class PGliteTransactionImpl implements PGliteTransaction {
  private _active = true;
  private _isManaged = false;
  private _timeout: ReturnType<typeof setTimeout> | null = null;
  private _timedOut = false;
  private _db: PGlite;
  private _adapter: PGliteAdapter;
  private _readOnly: boolean;

  constructor(
    public readonly id: string,
    db: PGlite,
    adapter: PGliteAdapter,
    options?: TransactionOptions,
  ) {
    this._db = db;
    this._adapter = adapter;
    this._readOnly = options?.readOnly ?? false;

    // Set up timeout if specified
    if (options?.timeout) {
      this._timeout = setTimeout(() => {
        this._timedOut = true;
        this._active = false;
      }, options.timeout);
    }
  }

  get active(): boolean {
    return this._active && !this._timedOut;
  }

  /** Mark this transaction as managed (commit/rollback handled externally) */
  setManaged(managed: boolean): void {
    this._isManaged = managed;
  }

  private checkActive(): void {
    if (this._timedOut) {
      throw new PGliteAdapterError(
        PGliteErrorCode.TRANSACTION_ERROR,
        'Transaction has timed out',
      );
    }
    if (!this._active) {
      throw new PGliteAdapterError(
        PGliteErrorCode.TRANSACTION_ERROR,
        'Transaction is no longer active',
      );
    }
  }

  async query<T extends Row = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    this.checkActive();

    const start = performance.now();

    try {
      const result = await this._db.query(sql, params);
      const duration = performance.now() - start;

      const fields: Field[] = result.fields.map((f) => ({
        name: f.name,
        type: getTypeName(f.dataTypeID),
        nativeType: f.dataTypeID,
      }));

      return {
        rows: result.rows as T[],
        rowCount: result.rows.length,
        fields,
        duration,
      };
    } catch (error) {
      const { code, sqlState } = classifyPgError(error);
      throw new PGliteAdapterError(
        code,
        (error as Error).message,
        error as Error,
        sqlState,
      );
    }
  }

  async execute(sql: string, params?: unknown[]): Promise<ExecuteResult> {
    this.checkActive();

    if (this._readOnly) {
      throw new PGliteAdapterError(
        PGliteErrorCode.TRANSACTION_ERROR,
        'Cannot execute write operations in read-only transaction',
      );
    }

    try {
      const result = await this._db.query(sql, params);

      return {
        affected: result.affectedRows ?? 0,
        lastInsertId: undefined, // Will be fetched separately if needed
      };
    } catch (error) {
      const { code, sqlState } = classifyPgError(error);
      throw new PGliteAdapterError(
        code,
        (error as Error).message,
        error as Error,
        sqlState,
      );
    }
  }

  async commit(): Promise<void> {
    if (this._isManaged) {
      throw new PGliteAdapterError(
        PGliteErrorCode.TRANSACTION_ERROR,
        'Cannot manually commit a managed transaction',
      );
    }

    this.checkActive();

    try {
      await this._db.query('COMMIT');
      this._active = false;
      this.clearTimeout();
      this._adapter['removeTransaction'](this.id);
    } catch (error) {
      const { code, sqlState } = classifyPgError(error);
      throw new PGliteAdapterError(
        code,
        (error as Error).message,
        error as Error,
        sqlState,
      );
    }
  }

  async rollback(): Promise<void> {
    if (this._isManaged) {
      throw new PGliteAdapterError(
        PGliteErrorCode.TRANSACTION_ERROR,
        'Cannot manually rollback a managed transaction',
      );
    }

    this.checkActive();

    try {
      await this._db.query('ROLLBACK');
      this._active = false;
      this.clearTimeout();
      this._adapter['removeTransaction'](this.id);
    } catch (error) {
      const { code, sqlState } = classifyPgError(error);
      throw new PGliteAdapterError(
        code,
        (error as Error).message,
        error as Error,
        sqlState,
      );
    }
  }

  /** Internal commit for managed transactions */
  async internalCommit(): Promise<void> {
    if (!this._active) return;

    try {
      await this._db.query('COMMIT');
    } finally {
      this._active = false;
      this.clearTimeout();
      this._adapter['removeTransaction'](this.id);
    }
  }

  /** Internal rollback for managed transactions */
  async internalRollback(): Promise<void> {
    if (!this._active) return;

    try {
      await this._db.query('ROLLBACK');
    } catch {
      // Ignore rollback errors during cleanup
    } finally {
      this._active = false;
      this.clearTimeout();
      this._adapter['removeTransaction'](this.id);
    }
  }

  private clearTimeout(): void {
    if (this._timeout) {
      clearTimeout(this._timeout);
      this._timeout = null;
    }
  }
}

/**
 * PGlite Adapter - PostgreSQL storage engine for Vitess.do
 *
 * Wraps PGlite with Vitess-compatible interface for use as a
 * VTTablet storage backend.
 */
export class PGliteAdapter {
  private db: PGlite | null = null;
  private _ready = false;
  private _closed = false;
  private _initPromise: Promise<void> | null = null;
  private _initResolve: (() => void) | null = null;
  private activeTransactions = new Map<string, PGliteTransactionImpl>();
  private txCounter = 0;

  constructor(private readonly options: PGliteAdapterOptions = {}) {}

  /**
   * Check if the adapter is ready for queries
   */
  get ready(): boolean {
    return this._ready;
  }

  /**
   * Check if the adapter has been closed
   */
  get closed(): boolean {
    return this._closed;
  }

  /**
   * Wait for the adapter to be ready
   */
  get waitReady(): Promise<void> {
    if (this._ready) {
      return Promise.resolve();
    }
    if (this._initPromise) {
      return this._initPromise;
    }
    // Create a promise that will be resolved when init is called
    this._initPromise = new Promise((resolve) => {
      this._initResolve = resolve;
    });
    return this._initPromise;
  }

  /**
   * Initialize the PGlite instance
   */
  async init(): Promise<void> {
    if (this._closed) {
      throw new PGliteAdapterError(
        PGliteErrorCode.ALREADY_CLOSED,
        'Cannot initialize: adapter has been closed',
      );
    }

    if (this._ready) {
      return;
    }

    // Handle concurrent init calls
    if (this._initPromise && !this._initResolve) {
      return this._initPromise;
    }

    const initWork = async () => {
      try {
        this.db = new PGlite(this.options.dataDir);

        // Wait for the database to be ready
        await this.db.waitReady;

        // Execute initial schema if provided
        if (this.options.initSchema) {
          try {
            await this.db.exec(this.options.initSchema);
          } catch (error) {
            const { code, sqlState } = classifyPgError(error);
            throw new PGliteAdapterError(
              code,
              `Failed to execute init schema: ${(error as Error).message}`,
              error as Error,
              sqlState,
            );
          }
        }

        this._ready = true;

        // Resolve any waiting promises
        if (this._initResolve) {
          this._initResolve();
          this._initResolve = null;
        }
      } catch (error) {
        // Re-throw if already a PGliteAdapterError
        if (error instanceof PGliteAdapterError) {
          throw error;
        }
        throw new PGliteAdapterError(
          PGliteErrorCode.CONNECTION_ERROR,
          `Failed to initialize PGlite: ${(error as Error).message}`,
          error as Error,
        );
      }
    };

    this._initPromise = initWork();
    return this._initPromise;
  }

  /**
   * Close the PGlite instance
   */
  async close(): Promise<void> {
    if (this._closed) {
      return;
    }

    // Rollback any active transactions
    for (const tx of this.activeTransactions.values()) {
      try {
        await tx.internalRollback();
      } catch {
        // Ignore rollback errors during close
      }
    }
    this.activeTransactions.clear();

    // Close the database
    if (this.db) {
      try {
        await this.db.close();
      } catch {
        // Ignore close errors
      }
      this.db = null;
    }

    this._ready = false;
    this._closed = true;
  }

  /**
   * Check adapter state and ensure it's ready
   */
  private async ensureReady(): Promise<void> {
    if (this._closed) {
      throw new PGliteAdapterError(
        PGliteErrorCode.ALREADY_CLOSED,
        'Adapter has been closed',
      );
    }

    if (!this._ready) {
      if (this._initPromise) {
        // Wait for init to complete
        await this._initPromise;
        if (!this._ready) {
          throw new PGliteAdapterError(
            PGliteErrorCode.NOT_READY,
            'Adapter initialization failed',
          );
        }
      } else {
        throw new PGliteAdapterError(
          PGliteErrorCode.NOT_READY,
          'Adapter has not been initialized. Call init() first.',
        );
      }
    }
  }

  /**
   * Execute a SELECT query
   */
  async query<T extends Row = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    await this.ensureReady();

    const start = performance.now();

    try {
      const result = await this.db!.query(sql, params);
      const duration = performance.now() - start;

      const fields: Field[] = result.fields.map((f) => ({
        name: f.name,
        type: getTypeName(f.dataTypeID),
        nativeType: f.dataTypeID,
      }));

      return {
        rows: result.rows as T[],
        rowCount: result.rows.length,
        fields,
        duration,
      };
    } catch (error) {
      const { code, sqlState } = classifyPgError(error);
      throw new PGliteAdapterError(
        code,
        (error as Error).message,
        error as Error,
        sqlState,
      );
    }
  }

  /**
   * Execute an INSERT/UPDATE/DELETE statement
   */
  async execute(sql: string, params?: unknown[]): Promise<ExecuteResult> {
    await this.ensureReady();

    try {
      // Check if this is an INSERT with a SERIAL/sequence column
      const isInsert = sql.trim().toUpperCase().startsWith('INSERT');
      let lastInsertId: number | undefined;

      const result = await this.db!.query(sql, params);

      // For INSERT statements, try to get the last inserted ID
      if (isInsert && result.affectedRows && result.affectedRows > 0) {
        try {
          // Get the sequence value from the last insert
          const seqResult = await this.db!.query('SELECT lastval()');
          if (seqResult.rows.length > 0) {
            const val = seqResult.rows[0].lastval;
            lastInsertId = typeof val === 'bigint' ? Number(val) : val as number;
          }
        } catch {
          // lastval() fails if no sequence was used - that's okay
        }
      }

      return {
        affected: result.affectedRows ?? 0,
        lastInsertId,
      };
    } catch (error) {
      const { code, sqlState } = classifyPgError(error);
      throw new PGliteAdapterError(
        code,
        (error as Error).message,
        error as Error,
        sqlState,
      );
    }
  }

  /**
   * Begin a transaction
   */
  async begin(options?: TransactionOptions): Promise<PGliteTransaction> {
    await this.ensureReady();

    const txId = this.generateTxId();

    try {
      // Build BEGIN statement with isolation level
      let beginSql = 'BEGIN';
      if (options?.isolation) {
        const isoLevel = this.mapIsolationLevel(options.isolation);
        beginSql += ` ISOLATION LEVEL ${isoLevel}`;
      }
      if (options?.readOnly) {
        beginSql += ' READ ONLY';
      }

      await this.db!.query(beginSql);

      const tx = new PGliteTransactionImpl(txId, this.db!, this, options);
      this.activeTransactions.set(txId, tx);

      return tx;
    } catch (error) {
      const { code, sqlState } = classifyPgError(error);
      throw new PGliteAdapterError(
        code,
        (error as Error).message,
        error as Error,
        sqlState,
      );
    }
  }

  /**
   * Execute a callback within a transaction
   */
  async transaction<T>(
    callback: (tx: PGliteTransaction) => Promise<T>,
    options?: TransactionOptions,
  ): Promise<T> {
    const tx = await this.begin(options) as PGliteTransactionImpl;
    tx.setManaged(true);

    try {
      const result = await callback(tx);
      await tx.internalCommit();
      return result;
    } catch (error) {
      await tx.internalRollback();
      throw error;
    }
  }

  /**
   * Get an active transaction by ID
   */
  getTransaction(txId: string): PGliteTransaction | undefined {
    const tx = this.activeTransactions.get(txId);
    if (tx && tx.active) {
      return tx;
    }
    return undefined;
  }

  /**
   * Remove a transaction from the active set
   */
  private removeTransaction(txId: string): void {
    this.activeTransactions.delete(txId);
  }

  /**
   * Map PGlite field types to Vitess field types
   */
  private mapFields(fields: Array<{ name: string; dataTypeID: number }>): Field[] {
    return fields.map((f) => ({
      name: f.name,
      type: getTypeName(f.dataTypeID),
      nativeType: f.dataTypeID,
    }));
  }

  /**
   * Map isolation level to PostgreSQL syntax
   */
  private mapIsolationLevel(level: IsolationLevel): string {
    switch (level) {
      case 'read_uncommitted':
        return 'READ UNCOMMITTED';
      case 'read_committed':
        return 'READ COMMITTED';
      case 'repeatable_read':
        return 'REPEATABLE READ';
      case 'serializable':
        return 'SERIALIZABLE';
      default:
        return 'READ COMMITTED';
    }
  }

  /**
   * Generate a unique transaction ID
   */
  private generateTxId(): string {
    this.txCounter++;
    return `tx_${Date.now()}_${this.txCounter}_${Math.random().toString(36).slice(2, 8)}`;
  }
}

// Re-export types for convenience
export type { QueryResult, ExecuteResult, Row, Field, TransactionOptions, IsolationLevel };
