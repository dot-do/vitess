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
export type VindexType =
  | 'hash'
  | 'consistent_hash'
  | 'range'
  | 'lookup'
  | 'null';

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
export type IsolationLevel =
  | 'read_uncommitted'
  | 'read_committed'
  | 'repeatable_read'
  | 'serializable';

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

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if value is a valid Row object
 */
export function isRow(value: unknown): value is Row {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value)
  );
}

/**
 * Check if value is a valid Field object
 */
export function isField(value: unknown): value is Field {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (typeof obj.name !== 'string' || typeof obj.type !== 'string') {
    return false;
  }
  if (obj.nativeType !== undefined && typeof obj.nativeType !== 'string' && typeof obj.nativeType !== 'number') {
    return false;
  }
  return true;
}

/**
 * Check if value is a valid QueryResult object
 */
export function isQueryResult(value: unknown): value is QueryResult {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (!Array.isArray(obj.rows) || typeof obj.rowCount !== 'number') {
    return false;
  }
  if (obj.fields !== undefined) {
    if (!Array.isArray(obj.fields)) {
      return false;
    }
    for (const field of obj.fields) {
      if (!isField(field)) {
        return false;
      }
    }
  }
  if (obj.duration !== undefined && typeof obj.duration !== 'number') {
    return false;
  }
  return true;
}

/**
 * Check if value is a valid ExecuteResult object
 */
export function isExecuteResult(value: unknown): value is ExecuteResult {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (typeof obj.affected !== 'number' || obj.affected < 0) {
    return false;
  }
  if (obj.lastInsertId !== undefined && typeof obj.lastInsertId !== 'string' && typeof obj.lastInsertId !== 'number') {
    return false;
  }
  return true;
}

/**
 * Check if value is a valid BatchResult object
 */
export function isBatchResult(value: unknown): value is BatchResult {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (!Array.isArray(obj.results) || typeof obj.success !== 'boolean') {
    return false;
  }
  return true;
}

/**
 * Check if value is a valid ShardHealth object
 */
export function isShardHealth(value: unknown): value is ShardHealth {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (
    typeof obj.id !== 'string' ||
    typeof obj.healthy !== 'boolean' ||
    typeof obj.engine !== 'string' ||
    typeof obj.queryCount !== 'number' ||
    typeof obj.errorCount !== 'number' ||
    typeof obj.lastQuery !== 'number'
  ) {
    return false;
  }
  if (!isStorageEngineType(obj.engine)) {
    return false;
  }
  if (obj.latency !== undefined) {
    if (typeof obj.latency !== 'object' || obj.latency === null) {
      return false;
    }
    const latency = obj.latency as Record<string, unknown>;
    if (
      typeof latency.p50 !== 'number' ||
      typeof latency.p95 !== 'number' ||
      typeof latency.p99 !== 'number'
    ) {
      return false;
    }
  }
  return true;
}

/**
 * Check if value is a valid ClusterStatus object
 */
export function isClusterStatus(value: unknown): value is ClusterStatus {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (
    typeof obj.keyspace !== 'string' ||
    typeof obj.shardCount !== 'number' ||
    typeof obj.engine !== 'string' ||
    !Array.isArray(obj.shards) ||
    typeof obj.totalQueries !== 'number' ||
    typeof obj.totalErrors !== 'number'
  ) {
    return false;
  }
  if (!isStorageEngineType(obj.engine)) {
    return false;
  }
  for (const shard of obj.shards) {
    if (!isShardHealth(shard)) {
      return false;
    }
  }
  return true;
}

/**
 * Check if value is a valid StorageEngineType
 */
export function isStorageEngineType(value: unknown): value is StorageEngineType {
  return value === 'postgres' || value === 'sqlite';
}

/**
 * Check if value is a valid VindexType
 */
export function isVindexType(value: unknown): value is VindexType {
  return (
    value === 'hash' ||
    value === 'consistent_hash' ||
    value === 'range' ||
    value === 'lookup' ||
    value === 'null'
  );
}

/**
 * Check if value is a valid Keyspace object
 */
export function isKeyspace(value: unknown): value is Keyspace {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (
    typeof obj.name !== 'string' ||
    obj.name === '' ||
    typeof obj.shardCount !== 'number' ||
    obj.shardCount < 1 ||
    !isStorageEngineType(obj.engine)
  ) {
    return false;
  }
  return true;
}

/**
 * Check if value is a valid VindexDef object
 */
export function isVindexDef(value: unknown): value is VindexDef {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (!isVindexType(obj.type)) {
    return false;
  }
  if (!Array.isArray(obj.columns) || obj.columns.length === 0) {
    return false;
  }
  for (const col of obj.columns) {
    if (typeof col !== 'string') {
      return false;
    }
  }
  if (obj.lookupTable !== undefined && typeof obj.lookupTable !== 'string') {
    return false;
  }
  return true;
}

/**
 * Check if value is a valid TableDef object
 */
export function isTableDef(value: unknown): value is TableDef {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (!isVindexDef(obj.vindex)) {
    return false;
  }
  if (obj.autoIncrement !== undefined) {
    if (typeof obj.autoIncrement !== 'object' || obj.autoIncrement === null) {
      return false;
    }
    const autoInc = obj.autoIncrement as Record<string, unknown>;
    if (typeof autoInc.column !== 'string' || typeof autoInc.sequence !== 'string') {
      return false;
    }
  }
  return true;
}

/**
 * Check if value is a valid VSchema object
 */
export function isVSchema(value: unknown): value is VSchema {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (
    typeof obj.keyspace !== 'string' ||
    obj.keyspace === '' ||
    typeof obj.sharded !== 'boolean' ||
    typeof obj.tables !== 'object' ||
    obj.tables === null ||
    typeof obj.vindexes !== 'object' ||
    obj.vindexes === null
  ) {
    return false;
  }
  // Validate tables
  const tables = obj.tables as Record<string, unknown>;
  for (const tableName of Object.keys(tables)) {
    if (!isTableDef(tables[tableName])) {
      return false;
    }
  }
  // Validate vindexes
  const vindexes = obj.vindexes as Record<string, unknown>;
  for (const vindexName of Object.keys(vindexes)) {
    if (!isVindexDef(vindexes[vindexName])) {
      return false;
    }
  }
  return true;
}

/**
 * Check if value is a valid IsolationLevel
 */
export function isIsolationLevel(value: unknown): value is IsolationLevel {
  return (
    value === 'read_uncommitted' ||
    value === 'read_committed' ||
    value === 'repeatable_read' ||
    value === 'serializable'
  );
}

/**
 * Check if value is a valid TransactionOptions object
 */
export function isTransactionOptions(value: unknown): value is TransactionOptions {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (obj.isolation !== undefined && !isIsolationLevel(obj.isolation)) {
    return false;
  }
  if (obj.readOnly !== undefined && typeof obj.readOnly !== 'boolean') {
    return false;
  }
  if (obj.timeout !== undefined && (typeof obj.timeout !== 'number' || obj.timeout < 0)) {
    return false;
  }
  return true;
}

/**
 * Check if value is a valid QueryTarget object
 */
export function isQueryTarget(value: unknown): value is QueryTarget {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (typeof obj.keyspace !== 'string' || obj.keyspace === '') {
    return false;
  }
  if (obj.shard !== undefined && typeof obj.shard !== 'string') {
    return false;
  }
  if (obj.scatter !== undefined && typeof obj.scatter !== 'boolean') {
    return false;
  }
  return true;
}

/**
 * Check if value is a valid AggregateFunction
 */
export function isAggregateFunction(value: unknown): value is AggregateFunction {
  return (
    value === 'COUNT' ||
    value === 'SUM' ||
    value === 'AVG' ||
    value === 'MIN' ||
    value === 'MAX'
  );
}

/**
 * Check if value is a valid AggregateSpec object
 */
export function isAggregateSpec(value: unknown): value is AggregateSpec {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (!isAggregateFunction(obj.function)) {
    return false;
  }
  if (typeof obj.column !== 'string' || obj.column === '') {
    return false;
  }
  if (obj.alias !== undefined && typeof obj.alias !== 'string') {
    return false;
  }
  return true;
}

// =============================================================================
// Validation Functions
// =============================================================================

/**
 * Validate a Field object and return error message or null
 */
export function validateField(value: unknown): string | null {
  if (typeof value !== 'object' || value === null) {
    return 'Field must be an object';
  }
  const obj = value as Record<string, unknown>;
  if (typeof obj.name !== 'string') {
    return 'Field name must be a string';
  }
  if (typeof obj.type !== 'string') {
    return 'Field type must be a string';
  }
  if (obj.nativeType !== undefined && typeof obj.nativeType !== 'string' && typeof obj.nativeType !== 'number') {
    return 'Field nativeType must be a string or number';
  }
  return null;
}

/**
 * Validate a QueryResult object and return error message or null
 */
export function validateQueryResult(value: unknown): string | null {
  if (typeof value !== 'object' || value === null) {
    return 'QueryResult must be an object';
  }
  const obj = value as Record<string, unknown>;
  if (!Array.isArray(obj.rows)) {
    return 'QueryResult rows must be an array';
  }
  if (typeof obj.rowCount !== 'number') {
    return 'QueryResult rowCount must be a number';
  }
  if (obj.rowCount < 0) {
    return 'QueryResult rowCount must be non-negative';
  }
  if (obj.duration !== undefined && (typeof obj.duration !== 'number' || obj.duration < 0)) {
    return 'QueryResult duration must be a non-negative number';
  }
  return null;
}

/**
 * Validate an ExecuteResult object and return error message or null
 */
export function validateExecuteResult(value: unknown): string | null {
  if (typeof value !== 'object' || value === null) {
    return 'ExecuteResult must be an object';
  }
  const obj = value as Record<string, unknown>;
  if (typeof obj.affected !== 'number') {
    return 'ExecuteResult affected must be a number';
  }
  if (obj.affected < 0) {
    return 'ExecuteResult affected must be non-negative';
  }
  if (obj.lastInsertId !== undefined && typeof obj.lastInsertId !== 'string' && typeof obj.lastInsertId !== 'number') {
    return 'ExecuteResult lastInsertId must be a string or number';
  }
  return null;
}

/**
 * Validate a Keyspace object and return error message or null
 */
export function validateKeyspace(value: unknown): string | null {
  if (typeof value !== 'object' || value === null) {
    return 'Keyspace must be an object';
  }
  const obj = value as Record<string, unknown>;
  if (typeof obj.name !== 'string' || obj.name === '') {
    return 'Keyspace name must be a non-empty string';
  }
  if (typeof obj.shardCount !== 'number' || obj.shardCount < 1) {
    return 'Keyspace shardCount must be a positive number';
  }
  if (!isStorageEngineType(obj.engine)) {
    return 'Keyspace engine must be "postgres" or "sqlite"';
  }
  return null;
}

/**
 * Validate a VindexDef object and return error message or null
 */
export function validateVindexDef(value: unknown): string | null {
  if (typeof value !== 'object' || value === null) {
    return 'VindexDef must be an object';
  }
  const obj = value as Record<string, unknown>;
  if (!isVindexType(obj.type)) {
    return 'VindexDef type must be a valid vindex type';
  }
  if (!Array.isArray(obj.columns) || obj.columns.length === 0) {
    return 'VindexDef columns must be a non-empty array';
  }
  for (const col of obj.columns) {
    if (typeof col !== 'string') {
      return 'VindexDef columns must contain only strings';
    }
  }
  if (obj.lookupTable !== undefined && typeof obj.lookupTable !== 'string') {
    return 'VindexDef lookupTable must be a string';
  }
  return null;
}

/**
 * Validate a TableDef object and return error message or null
 */
export function validateTableDef(value: unknown): string | null {
  if (typeof value !== 'object' || value === null) {
    return 'TableDef must be an object';
  }
  const obj = value as Record<string, unknown>;
  const vindexError = validateVindexDef(obj.vindex);
  if (vindexError !== null) {
    return `TableDef vindex is invalid: ${vindexError}`;
  }
  if (obj.autoIncrement !== undefined) {
    if (typeof obj.autoIncrement !== 'object' || obj.autoIncrement === null) {
      return 'TableDef autoIncrement must be an object';
    }
    const autoInc = obj.autoIncrement as Record<string, unknown>;
    if (typeof autoInc.column !== 'string' || typeof autoInc.sequence !== 'string') {
      return 'TableDef autoIncrement must have column and sequence strings';
    }
  }
  return null;
}

/**
 * Validate a VSchema object and return error message or null
 */
export function validateVSchema(value: unknown): string | null {
  if (typeof value !== 'object' || value === null) {
    return 'VSchema must be an object';
  }
  const obj = value as Record<string, unknown>;
  if (typeof obj.keyspace !== 'string' || obj.keyspace === '') {
    return 'VSchema keyspace must be a non-empty string';
  }
  if (typeof obj.sharded !== 'boolean') {
    return 'VSchema sharded must be a boolean';
  }
  if (typeof obj.tables !== 'object' || obj.tables === null) {
    return 'VSchema tables must be an object';
  }
  if (typeof obj.vindexes !== 'object' || obj.vindexes === null) {
    return 'VSchema vindexes must be an object';
  }
  // Validate tables
  const tables = obj.tables as Record<string, unknown>;
  for (const tableName of Object.keys(tables)) {
    const tableError = validateTableDef(tables[tableName]);
    if (tableError !== null) {
      return `VSchema tables["${tableName}"] is invalid: ${tableError}`;
    }
  }
  // Validate vindexes
  const vindexes = obj.vindexes as Record<string, unknown>;
  for (const vindexName of Object.keys(vindexes)) {
    const vindexError = validateVindexDef(vindexes[vindexName]);
    if (vindexError !== null) {
      return `VSchema vindexes["${vindexName}"] is invalid: ${vindexError}`;
    }
  }
  return null;
}
