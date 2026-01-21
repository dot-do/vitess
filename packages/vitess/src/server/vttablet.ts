/**
 * VTTablet - Shard-level Query Executor
 *
 * Responsible for:
 * - Executing queries on the local shard
 * - Transaction management (BEGIN, COMMIT, ROLLBACK)
 * - Storage engine abstraction (PGlite, Turso)
 * - Connection pooling (if applicable)
 * - Prepared statement caching
 */

/**
 * Storage engine types
 */
export type StorageEngineType = 'pglite' | 'turso' | 'sqlite' | 'postgres';

/**
 * Storage engine interface
 */
export interface StorageEngine {
  /** Engine type identifier */
  readonly type: StorageEngineType;

  /** Execute a query */
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;

  /** Execute a write statement */
  execute(sql: string, params?: unknown[]): Promise<ExecuteResult>;

  /** Begin a transaction */
  beginTransaction(): Promise<TransactionHandle>;

  /** Close the engine */
  close(): Promise<void>;
}

/**
 * Query result
 */
export interface QueryResult<T = Record<string, unknown>> {
  rows: T[];
  rowCount: number;
  fields: FieldInfo[];
}

/**
 * Execute result
 */
export interface ExecuteResult {
  rowsAffected: number;
  lastInsertId?: string | number;
}

/**
 * Field info
 */
export interface FieldInfo {
  name: string;
  type: string;
  nullable?: boolean;
}

/**
 * Transaction state
 */
export type TransactionState = 'active' | 'committed' | 'rolled_back';

/**
 * Transaction handle
 */
export interface TransactionHandle {
  /** Transaction ID */
  readonly id: string;
  /** Transaction state */
  state: TransactionState;

  /** Execute query within transaction */
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;

  /** Execute write within transaction */
  execute(sql: string, params?: unknown[]): Promise<ExecuteResult>;

  /** Commit transaction */
  commit(): Promise<void>;

  /** Rollback transaction */
  rollback(): Promise<void>;

  /** Prepare for 2PC (returns prepare token) */
  prepare(): Promise<string>;

  /** Commit prepared transaction (2PC) */
  commitPrepared(token: string): Promise<void>;

  /** Rollback prepared transaction (2PC) */
  rollbackPrepared(token: string): Promise<void>;
}

/**
 * VTTablet configuration
 */
export interface VTTabletConfig {
  /** Shard identifier (e.g., '-80', '80-') */
  shard: string;
  /** Keyspace name */
  keyspace: string;
  /** Storage engine to use */
  engine: StorageEngine;
  /** Maximum concurrent transactions */
  maxTransactions?: number;
}

/**
 * PGlite field type ID to type name mapping
 */
const PGLITE_TYPE_MAP: Record<number, string> = {
  16: 'bool',
  17: 'bytea',
  20: 'bigint',
  21: 'smallint',
  23: 'int',
  25: 'text',
  114: 'json',
  142: 'xml',
  700: 'float4',
  701: 'float8',
  1082: 'date',
  1083: 'time',
  1114: 'timestamp',
  1184: 'timestamptz',
  1700: 'numeric',
  2950: 'uuid',
};

/**
 * VTTablet - Shard query executor
 */
export class VTTablet {
  private config: VTTabletConfig;
  private engine: StorageEngine;
  private transactions: Map<string, TransactionHandle> = new Map();
  private activeTransactionCount: number = 0;

  constructor(config: VTTabletConfig) {
    this.config = config;
    this.engine = config.engine;
  }

  /**
   * Get shard identifier
   */
  get shard(): string {
    return this.config.shard;
  }

  /**
   * Get keyspace name
   */
  get keyspace(): string {
    return this.config.keyspace;
  }

  /**
   * Get storage engine type
   */
  get engineType(): StorageEngineType {
    return this.engine.type;
  }

  /**
   * Execute a query
   */
  async query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    return this.engine.query<T>(sql, params);
  }

  /**
   * Execute a write statement
   */
  async execute(sql: string, params?: unknown[]): Promise<ExecuteResult> {
    return this.engine.execute(sql, params);
  }

  /**
   * Begin a transaction
   */
  async beginTransaction(): Promise<TransactionHandle> {
    // Check max transactions limit
    if (this.config.maxTransactions !== undefined) {
      if (this.activeTransactionCount >= this.config.maxTransactions) {
        throw new Error('Maximum number of concurrent transactions reached');
      }
    }

    const tx = await this.engine.beginTransaction();

    // Wrap commit/rollback to track completion
    const self = this;
    let decremented = false;

    // Return a proxy that wraps commit/rollback but preserves spy access
    // The proxy looks up commit/rollback at call time, not at creation time
    const proxy = new Proxy(tx, {
      get(target, prop) {
        if (prop === 'commit') {
          return async function(...args: any[]) {
            // Always call the current commit function on target
            const result = await (target as any).commit.apply(target, args);
            if (!decremented) {
              decremented = true;
              self.activeTransactionCount--;
            }
            return result;
          };
        }
        if (prop === 'rollback') {
          return async function(...args: any[]) {
            // Always call the current rollback function on target
            const result = await (target as any).rollback.apply(target, args);
            if (!decremented) {
              decremented = true;
              self.activeTransactionCount--;
            }
            return result;
          };
        }
        return (target as any)[prop];
      }
    });

    this.transactions.set(tx.id, proxy);
    this.activeTransactionCount++;

    return proxy;
  }

  /**
   * Get active transaction by ID
   */
  getTransaction(txId: string): TransactionHandle | undefined {
    return this.transactions.get(txId);
  }

  /**
   * Switch storage engine (hot swap)
   */
  async switchEngine(newEngine: StorageEngine): Promise<void> {
    // Check for active transactions using counter (more reliable)
    if (this.activeTransactionCount > 0) {
      throw new Error('Cannot switch engine while there are active transactions');
    }

    const oldEngine = this.engine;

    // Test the new engine BEFORE switching (atomic switch)
    try {
      await newEngine.query('SELECT 1');
    } catch (testError) {
      // New engine failed - throw error without switching
      throw testError;
    }

    // New engine works, now switch
    this.engine = newEngine;

    // Try to close old engine (best effort)
    try {
      await oldEngine.close();
    } catch (e) {
      // Ignore close errors - the switch already happened
    }
  }

  /**
   * Close the tablet
   */
  async close(): Promise<void> {
    // Rollback all active transactions
    for (const [txId, tx] of this.transactions) {
      if (tx.state === 'active') {
        await tx.rollback();
      }
    }
    this.transactions.clear();
    await this.engine.close();
  }
}

/**
 * Create a VTTablet instance
 */
export function createVTTablet(config: VTTabletConfig): VTTablet {
  return new VTTablet(config);
}

/**
 * Generate a unique transaction ID
 */
function generateTxId(): string {
  return `tx-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
}

/**
 * Generate a prepare token for 2PC
 */
function generatePrepareToken(): string {
  return `prepare-token-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
}

/**
 * PGlite storage engine adapter
 */
export class PGliteEngine implements StorageEngine {
  readonly type: StorageEngineType = 'pglite';
  private db: any; // PGlite instance

  constructor(db: any) {
    this.db = db;
  }

  async query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    const result = await this.db.query(sql, params);

    const fields: FieldInfo[] = (result.fields || []).map((f: any) => ({
      name: f.name,
      type: PGLITE_TYPE_MAP[f.dataTypeID] || 'unknown',
    }));

    return {
      rows: result.rows as T[],
      rowCount: result.rows?.length ?? 0,
      fields,
    };
  }

  async execute(sql: string, params?: unknown[]): Promise<ExecuteResult> {
    const result = await this.db.exec(sql, params);
    return {
      rowsAffected: result?.affectedRows ?? 0,
      lastInsertId: result?.insertId,
    };
  }

  async beginTransaction(): Promise<TransactionHandle> {
    await this.db.exec('BEGIN');

    const txId = generateTxId();
    let txState: TransactionState = 'active';
    let prepareToken: string | null = null;

    const handle: TransactionHandle = {
      get id() { return txId; },
      get state() { return txState; },
      set state(s) { txState = s; },

      query: async <T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>> => {
        if (txState !== 'active') {
          throw new Error(`Transaction ${txState}`);
        }
        return this.query<T>(sql, params);
      },

      execute: async (sql: string, params?: unknown[]): Promise<ExecuteResult> => {
        if (txState !== 'active') {
          throw new Error(`Transaction ${txState}`);
        }
        return this.execute(sql, params);
      },

      commit: async () => {
        if (txState === 'committed') return;
        if (txState === 'rolled_back') {
          throw new Error('Cannot commit a rolled back transaction');
        }
        await this.db.exec('COMMIT');
        txState = 'committed';
      },

      rollback: async () => {
        if (txState !== 'active') return;
        try {
          await this.db.exec('ROLLBACK');
        } catch (e) {
          // Ignore rollback errors
        }
        txState = 'rolled_back';
      },

      prepare: async (): Promise<string> => {
        if (txState !== 'active') {
          throw new Error('Cannot prepare a non-active transaction');
        }
        prepareToken = generatePrepareToken();
        // In real 2PC, we'd persist the prepare state
        return prepareToken;
      },

      commitPrepared: async (token: string): Promise<void> => {
        if (prepareToken && token !== prepareToken) {
          throw new Error('Invalid prepare token');
        }
        await this.db.exec('COMMIT');
        txState = 'committed';
      },

      rollbackPrepared: async (token: string): Promise<void> => {
        if (prepareToken && token !== prepareToken) {
          throw new Error('Invalid prepare token');
        }
        await this.db.exec('ROLLBACK');
        txState = 'rolled_back';
      },
    };

    return handle;
  }

  async close(): Promise<void> {
    if (this.db.close) {
      await this.db.close();
    }
  }
}

/**
 * Turso storage engine adapter
 */
export class TursoEngine implements StorageEngine {
  readonly type: StorageEngineType = 'turso';
  private db: any; // Turso client

  constructor(db: any) {
    this.db = db;
  }

  /**
   * Convert PostgreSQL-style $1, $2 parameters to SQLite-style ?
   */
  private convertParams(sql: string): string {
    return sql.replace(/\$(\d+)/g, '?');
  }

  async query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    const convertedSql = this.convertParams(sql);
    const result = await this.db.execute({
      sql: convertedSql,
      args: params || [],
    });

    const fields: FieldInfo[] = (result.columns || []).map((name: string, i: number) => ({
      name,
      type: (result.columnTypes || [])[i]?.toLowerCase() || 'unknown',
    }));

    return {
      rows: result.rows as T[],
      rowCount: result.rows?.length ?? 0,
      fields,
    };
  }

  async execute(sql: string, params?: unknown[]): Promise<ExecuteResult> {
    const convertedSql = this.convertParams(sql);
    const result = await this.db.execute({
      sql: convertedSql,
      args: params || [],
    });

    return {
      rowsAffected: result.rowsAffected ?? 0,
      lastInsertId: result.lastInsertRowid !== undefined
        ? String(result.lastInsertRowid)
        : undefined,
    };
  }

  async beginTransaction(): Promise<TransactionHandle> {
    const txId = generateTxId();
    let txState: TransactionState = 'active';
    let prepareToken: string | null = null;
    const statements: Array<{ sql: string; params?: unknown[] }> = [];

    const handle: TransactionHandle = {
      get id() { return txId; },
      get state() { return txState; },
      set state(s) { txState = s; },

      query: async <T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>> => {
        if (txState !== 'active') {
          throw new Error(`Transaction ${txState}`);
        }
        return this.query<T>(sql, params);
      },

      execute: async (sql: string, params?: unknown[]): Promise<ExecuteResult> => {
        if (txState !== 'active') {
          throw new Error(`Transaction ${txState}`);
        }
        statements.push({ sql, params });
        return this.execute(sql, params);
      },

      commit: async () => {
        if (txState === 'committed') return;
        if (txState === 'rolled_back') {
          throw new Error('Cannot commit a rolled back transaction');
        }
        txState = 'committed';
      },

      rollback: async () => {
        if (txState !== 'active') return;
        txState = 'rolled_back';
      },

      prepare: async (): Promise<string> => {
        if (txState !== 'active') {
          throw new Error('Cannot prepare a non-active transaction');
        }
        prepareToken = generatePrepareToken();
        return prepareToken;
      },

      commitPrepared: async (token: string): Promise<void> => {
        if (prepareToken && token !== prepareToken) {
          throw new Error('Invalid prepare token');
        }
        txState = 'committed';
      },

      rollbackPrepared: async (token: string): Promise<void> => {
        if (prepareToken && token !== prepareToken) {
          throw new Error('Invalid prepare token');
        }
        txState = 'rolled_back';
      },
    };

    return handle;
  }

  async close(): Promise<void> {
    if (this.db.close) {
      await this.db.close();
    }
  }
}
