/**
 * Unified Vitess Types
 *
 * These types are backend-agnostic - they work identically whether
 * the underlying storage is PostgreSQL (PGlite) or SQLite (Turso).
 * The client SDK uses these types; the storage engine handles translation.
 */
/**
 * Storage engine type identifier
 */
export type StorageEngineType = 'postgres' | 'sqlite';
/**
 * Query result row - generic record type
 */
export type Row = Record<string, unknown>;
/**
 * Field metadata from query results
 */
export interface Field {
    name: string;
    type: string;
    /** Original database-specific type (for advanced use) */
    nativeType?: string | number;
}
/**
 * Unified query result - same structure regardless of backend
 */
export interface QueryResult<T extends Row = Row> {
    rows: T[];
    rowCount: number;
    fields?: Field[];
    /** Execution time in milliseconds */
    duration?: number;
}
/**
 * Execute result for write operations
 */
export interface ExecuteResult {
    /** Number of rows affected */
    affected: number;
    /** Last inserted row ID (if applicable) */
    lastInsertId?: string | number;
}
/**
 * Batch operation result
 */
export interface BatchResult {
    results: QueryResult[];
    success: boolean;
    /** If failed, which statement index failed */
    failedAt?: number;
    error?: string;
}
/**
 * Shard identifier
 */
export type ShardId = string;
/**
 * Keyspace configuration
 */
export interface Keyspace {
    name: string;
    shardCount: number;
    /** Storage engine for this keyspace */
    engine: StorageEngineType;
}
/**
 * Vindex types for shard routing
 */
export type VindexType = 'hash' | 'consistent_hash' | 'range' | 'lookup' | 'null';
/**
 * Vindex definition
 */
export interface VindexDef {
    type: VindexType;
    /** Column(s) used for sharding */
    columns: string[];
    /** For lookup vindexes, the lookup table */
    lookupTable?: string;
}
/**
 * Table sharding configuration
 */
export interface TableDef {
    /** Primary vindex for this table */
    vindex: VindexDef;
    /** Auto-increment column (if any) */
    autoIncrement?: {
        column: string;
        sequence: string;
    };
}
/**
 * VSchema - sharding configuration for a keyspace
 */
export interface VSchema {
    keyspace: string;
    sharded: boolean;
    tables: Record<string, TableDef>;
    vindexes: Record<string, VindexDef>;
}
/**
 * Transaction isolation levels
 */
export type IsolationLevel = 'read_uncommitted' | 'read_committed' | 'repeatable_read' | 'serializable';
/**
 * Transaction options
 */
export interface TransactionOptions {
    isolation?: IsolationLevel;
    readOnly?: boolean;
    /** Timeout in milliseconds */
    timeout?: number;
}
/**
 * Shard health status
 */
export interface ShardHealth {
    id: ShardId;
    healthy: boolean;
    engine: StorageEngineType;
    queryCount: number;
    errorCount: number;
    lastQuery: number;
    /** Latency percentiles in ms */
    latency?: {
        p50: number;
        p95: number;
        p99: number;
    };
}
/**
 * Cluster status
 */
export interface ClusterStatus {
    keyspace: string;
    shardCount: number;
    engine: StorageEngineType;
    shards: ShardHealth[];
    /** Total queries across all shards */
    totalQueries: number;
    /** Total errors across all shards */
    totalErrors: number;
}
/**
 * Query target - where to route the query
 */
export interface QueryTarget {
    keyspace: string;
    shard?: ShardId;
    /** Target all shards (scatter) */
    scatter?: boolean;
}
/**
 * Aggregation function for cross-shard queries
 */
export type AggregateFunction = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';
/**
 * Aggregation specification
 */
export interface AggregateSpec {
    function: AggregateFunction;
    column: string;
    alias?: string;
}
//# sourceMappingURL=types.d.ts.map