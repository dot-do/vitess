/**
 * CapnWeb RPC Protocol for Vitess.do
 *
 * Defines the wire protocol between VitessClient and VTGate,
 * and between VTGate and VTTablet Durable Objects.
 */

import type {
  QueryResult,
  ExecuteResult,
  BatchResult,
  ShardId,
  TransactionOptions,
  ClusterStatus,
  ShardHealth,
  VSchema,
  Row,
} from './types.js';

/**
 * RPC message types
 */
export enum MessageType {
  // Query operations
  QUERY = 0x01,
  EXECUTE = 0x02,
  BATCH = 0x03,

  // Transaction operations
  BEGIN = 0x10,
  COMMIT = 0x11,
  ROLLBACK = 0x12,

  // Admin operations
  STATUS = 0x20,
  HEALTH = 0x21,
  SCHEMA = 0x22,
  VSCHEMA = 0x23,

  // Shard operations
  SHARD_QUERY = 0x30,
  SHARD_EXECUTE = 0x31,
  SHARD_BATCH = 0x32,

  // Response types
  RESULT = 0x80,
  ERROR = 0x81,
  ACK = 0x82,
}

/**
 * Base RPC message
 */
export interface RpcMessage {
  type: MessageType;
  id: string;
  timestamp: number;
}

/**
 * Query request to VTGate
 */
export interface QueryRequest extends RpcMessage {
  type: MessageType.QUERY;
  sql: string;
  params?: unknown[];
  keyspace?: string;
  /** Optional transaction ID */
  txId?: string;
}

/**
 * Execute request to VTGate
 */
export interface ExecuteRequest extends RpcMessage {
  type: MessageType.EXECUTE;
  sql: string;
  params?: unknown[];
  keyspace?: string;
  txId?: string;
}

/**
 * Batch request to VTGate
 */
export interface BatchRequest extends RpcMessage {
  type: MessageType.BATCH;
  statements: Array<{
    sql: string;
    params?: unknown[];
  }>;
  keyspace?: string;
  txId?: string;
}

/**
 * Begin transaction request
 */
export interface BeginRequest extends RpcMessage {
  type: MessageType.BEGIN;
  keyspace?: string;
  options?: TransactionOptions;
}

/**
 * Commit transaction request
 */
export interface CommitRequest extends RpcMessage {
  type: MessageType.COMMIT;
  txId: string;
}

/**
 * Rollback transaction request
 */
export interface RollbackRequest extends RpcMessage {
  type: MessageType.ROLLBACK;
  txId: string;
}

/**
 * Status request
 */
export interface StatusRequest extends RpcMessage {
  type: MessageType.STATUS;
  keyspace?: string;
}

/**
 * Health check request
 */
export interface HealthRequest extends RpcMessage {
  type: MessageType.HEALTH;
  shard?: ShardId;
}

/**
 * Schema request
 */
export interface SchemaRequest extends RpcMessage {
  type: MessageType.SCHEMA;
  keyspace?: string;
}

/**
 * VSchema request
 */
export interface VSchemaRequest extends RpcMessage {
  type: MessageType.VSCHEMA;
  keyspace?: string;
}

/**
 * Shard-level query (VTGate -> VTTablet)
 */
export interface ShardQueryRequest extends RpcMessage {
  type: MessageType.SHARD_QUERY;
  shard: ShardId;
  sql: string;
  params?: unknown[];
}

/**
 * Shard-level execute (VTGate -> VTTablet)
 */
export interface ShardExecuteRequest extends RpcMessage {
  type: MessageType.SHARD_EXECUTE;
  shard: ShardId;
  sql: string;
  params?: unknown[];
}

/**
 * Shard-level batch (VTGate -> VTTablet)
 */
export interface ShardBatchRequest extends RpcMessage {
  type: MessageType.SHARD_BATCH;
  shard: ShardId;
  statements: Array<{
    sql: string;
    params?: unknown[];
  }>;
}

/**
 * Query result response
 */
export interface QueryResponse<T extends Row = Row> extends RpcMessage {
  type: MessageType.RESULT;
  result: QueryResult<T>;
}

/**
 * Execute result response
 */
export interface ExecuteResponse extends RpcMessage {
  type: MessageType.RESULT;
  result: ExecuteResult;
}

/**
 * Batch result response
 */
export interface BatchResponse extends RpcMessage {
  type: MessageType.RESULT;
  result: BatchResult;
}

/**
 * Begin transaction response
 */
export interface BeginResponse extends RpcMessage {
  type: MessageType.RESULT;
  txId: string;
  shards: ShardId[];
}

/**
 * Status response
 */
export interface StatusResponse extends RpcMessage {
  type: MessageType.RESULT;
  status: ClusterStatus;
}

/**
 * Health response
 */
export interface HealthResponse extends RpcMessage {
  type: MessageType.RESULT;
  health: ShardHealth | ShardHealth[];
}

/**
 * Schema response
 */
export interface SchemaResponse extends RpcMessage {
  type: MessageType.RESULT;
  tables: Array<{
    name: string;
    type: string;
    columns?: Array<{
      name: string;
      type: string;
      nullable: boolean;
    }>;
  }>;
}

/**
 * VSchema response
 */
export interface VSchemaResponse extends RpcMessage {
  type: MessageType.RESULT;
  vschema: VSchema;
}

/**
 * Error response
 */
export interface ErrorResponse extends RpcMessage {
  type: MessageType.ERROR;
  code: string;
  message: string;
  /** Shard that caused the error (if applicable) */
  shard?: ShardId;
  /** SQL state code */
  sqlState?: string;
}

/**
 * Acknowledgment response
 */
export interface AckResponse extends RpcMessage {
  type: MessageType.ACK;
}

/**
 * Union of all request types
 */
export type Request =
  | QueryRequest
  | ExecuteRequest
  | BatchRequest
  | BeginRequest
  | CommitRequest
  | RollbackRequest
  | StatusRequest
  | HealthRequest
  | SchemaRequest
  | VSchemaRequest
  | ShardQueryRequest
  | ShardExecuteRequest
  | ShardBatchRequest;

/**
 * Union of all response types
 */
export type Response =
  | QueryResponse
  | ExecuteResponse
  | BatchResponse
  | BeginResponse
  | StatusResponse
  | HealthResponse
  | SchemaResponse
  | VSchemaResponse
  | ErrorResponse
  | AckResponse;

/**
 * Create a unique message ID
 */
export function createMessageId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;
}

/**
 * Create a query request
 */
export function createQueryRequest(
  sql: string,
  params?: unknown[],
  options?: { keyspace?: string; txId?: string }
): QueryRequest {
  return {
    type: MessageType.QUERY,
    id: createMessageId(),
    timestamp: Date.now(),
    sql,
    params,
    ...options,
  };
}

/**
 * Create an execute request
 */
export function createExecuteRequest(
  sql: string,
  params?: unknown[],
  options?: { keyspace?: string; txId?: string }
): ExecuteRequest {
  return {
    type: MessageType.EXECUTE,
    id: createMessageId(),
    timestamp: Date.now(),
    sql,
    params,
    ...options,
  };
}

/**
 * Create an error response
 */
export function createErrorResponse(
  requestId: string,
  code: string,
  message: string,
  options?: { shard?: ShardId; sqlState?: string }
): ErrorResponse {
  return {
    type: MessageType.ERROR,
    id: requestId,
    timestamp: Date.now(),
    code,
    message,
    ...options,
  };
}

/**
 * Create a batch request
 */
export function createBatchRequest(
  statements: Array<{ sql: string; params?: unknown[] }>,
  options?: { keyspace?: string; txId?: string }
): BatchRequest {
  return {
    type: MessageType.BATCH,
    id: createMessageId(),
    timestamp: Date.now(),
    statements,
    ...options,
  };
}

/**
 * Create a begin transaction request
 */
export function createBeginRequest(
  options?: { keyspace?: string; options?: TransactionOptions }
): BeginRequest {
  return {
    type: MessageType.BEGIN,
    id: createMessageId(),
    timestamp: Date.now(),
    ...options,
  };
}

/**
 * Create a commit request
 */
export function createCommitRequest(txId: string): CommitRequest {
  return {
    type: MessageType.COMMIT,
    id: createMessageId(),
    timestamp: Date.now(),
    txId,
  };
}

/**
 * Create a rollback request
 */
export function createRollbackRequest(txId: string): RollbackRequest {
  return {
    type: MessageType.ROLLBACK,
    id: createMessageId(),
    timestamp: Date.now(),
    txId,
  };
}

/**
 * Create a status request
 */
export function createStatusRequest(
  options?: { keyspace?: string }
): StatusRequest {
  return {
    type: MessageType.STATUS,
    id: createMessageId(),
    timestamp: Date.now(),
    ...options,
  };
}

/**
 * Create a health check request
 */
export function createHealthRequest(
  options?: { shard?: ShardId }
): HealthRequest {
  return {
    type: MessageType.HEALTH,
    id: createMessageId(),
    timestamp: Date.now(),
    ...options,
  };
}

/**
 * Create a schema request
 */
export function createSchemaRequest(
  options?: { keyspace?: string }
): SchemaRequest {
  return {
    type: MessageType.SCHEMA,
    id: createMessageId(),
    timestamp: Date.now(),
    ...options,
  };
}

/**
 * Create a VSchema request
 */
export function createVSchemaRequest(
  options?: { keyspace?: string }
): VSchemaRequest {
  return {
    type: MessageType.VSCHEMA,
    id: createMessageId(),
    timestamp: Date.now(),
    ...options,
  };
}

/**
 * Create a shard-level query request
 */
export function createShardQueryRequest(
  shard: ShardId,
  sql: string,
  params?: unknown[]
): ShardQueryRequest {
  return {
    type: MessageType.SHARD_QUERY,
    id: createMessageId(),
    timestamp: Date.now(),
    shard,
    sql,
    params,
  };
}

/**
 * Create a shard-level execute request
 */
export function createShardExecuteRequest(
  shard: ShardId,
  sql: string,
  params?: unknown[]
): ShardExecuteRequest {
  return {
    type: MessageType.SHARD_EXECUTE,
    id: createMessageId(),
    timestamp: Date.now(),
    shard,
    sql,
    params,
  };
}

/**
 * Create a shard-level batch request
 */
export function createShardBatchRequest(
  shard: ShardId,
  statements: Array<{ sql: string; params?: unknown[] }>
): ShardBatchRequest {
  return {
    type: MessageType.SHARD_BATCH,
    id: createMessageId(),
    timestamp: Date.now(),
    shard,
    statements,
  };
}

/**
 * Create a query response
 */
export function createQueryResponse<T extends Row = Row>(
  requestId: string,
  result: QueryResult<T>
): QueryResponse<T> {
  return {
    type: MessageType.RESULT,
    id: requestId,
    timestamp: Date.now(),
    result,
  };
}

/**
 * Create an execute response
 */
export function createExecuteResponse(
  requestId: string,
  result: ExecuteResult
): ExecuteResponse {
  return {
    type: MessageType.RESULT,
    id: requestId,
    timestamp: Date.now(),
    result,
  };
}

/**
 * Create a batch response
 */
export function createBatchResponse(
  requestId: string,
  result: BatchResult
): BatchResponse {
  return {
    type: MessageType.RESULT,
    id: requestId,
    timestamp: Date.now(),
    result,
  };
}

/**
 * Create a begin transaction response
 */
export function createBeginResponse(
  requestId: string,
  txId: string,
  shards: ShardId[]
): BeginResponse {
  return {
    type: MessageType.RESULT,
    id: requestId,
    timestamp: Date.now(),
    txId,
    shards,
  };
}

/**
 * Create a status response
 */
export function createStatusResponse(
  requestId: string,
  status: ClusterStatus
): StatusResponse {
  return {
    type: MessageType.RESULT,
    id: requestId,
    timestamp: Date.now(),
    status,
  };
}

/**
 * Create a health response
 */
export function createHealthResponse(
  requestId: string,
  health: ShardHealth | ShardHealth[]
): HealthResponse {
  return {
    type: MessageType.RESULT,
    id: requestId,
    timestamp: Date.now(),
    health,
  };
}

/**
 * Create a schema response
 */
export function createSchemaResponse(
  requestId: string,
  tables: SchemaResponse['tables']
): SchemaResponse {
  return {
    type: MessageType.RESULT,
    id: requestId,
    timestamp: Date.now(),
    tables,
  };
}

/**
 * Create a VSchema response
 */
export function createVSchemaResponse(
  requestId: string,
  vschema: VSchema
): VSchemaResponse {
  return {
    type: MessageType.RESULT,
    id: requestId,
    timestamp: Date.now(),
    vschema,
  };
}

/**
 * Create an acknowledgment response
 */
export function createAckResponse(requestId: string): AckResponse {
  return {
    type: MessageType.ACK,
    id: requestId,
    timestamp: Date.now(),
  };
}

// =============================================================================
// Message Type Guards
// =============================================================================

import {
  isQueryResult,
  isExecuteResult,
  isBatchResult,
  isClusterStatus,
  isShardHealth,
  isVSchema,
  isTransactionOptions,
} from './types.js';

/**
 * Check if value is a valid MessageType enum value
 */
function isMessageType(value: unknown): value is MessageType {
  return (
    typeof value === 'number' &&
    Object.values(MessageType).includes(value)
  );
}

/**
 * Check if value is a valid RpcMessage base object
 */
export function isRpcMessage(value: unknown): value is RpcMessage {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    isMessageType(obj.type) &&
    typeof obj.id === 'string' &&
    typeof obj.timestamp === 'number'
  );
}

/**
 * Check if value is a valid QueryRequest
 */
export function isQueryRequest(value: unknown): value is QueryRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.QUERY &&
    typeof obj.sql === 'string' &&
    obj.sql !== ''
  );
}

/**
 * Check if value is a valid ExecuteRequest
 */
export function isExecuteRequest(value: unknown): value is ExecuteRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.EXECUTE &&
    typeof obj.sql === 'string' &&
    obj.sql !== ''
  );
}

/**
 * Check if value is a valid BatchRequest
 */
export function isBatchRequest(value: unknown): value is BatchRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.BATCH &&
    Array.isArray(obj.statements)
  );
}

/**
 * Check if value is a valid BeginRequest
 */
export function isBeginRequest(value: unknown): value is BeginRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (obj.type !== MessageType.BEGIN) {
    return false;
  }
  if (obj.options !== undefined && !isTransactionOptions(obj.options)) {
    return false;
  }
  return true;
}

/**
 * Check if value is a valid CommitRequest
 */
export function isCommitRequest(value: unknown): value is CommitRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.COMMIT &&
    typeof obj.txId === 'string'
  );
}

/**
 * Check if value is a valid RollbackRequest
 */
export function isRollbackRequest(value: unknown): value is RollbackRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.ROLLBACK &&
    typeof obj.txId === 'string'
  );
}

/**
 * Check if value is a valid StatusRequest
 */
export function isStatusRequest(value: unknown): value is StatusRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  return (value as Record<string, unknown>).type === MessageType.STATUS;
}

/**
 * Check if value is a valid HealthRequest
 */
export function isHealthRequest(value: unknown): value is HealthRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  return (value as Record<string, unknown>).type === MessageType.HEALTH;
}

/**
 * Check if value is a valid SchemaRequest
 */
export function isSchemaRequest(value: unknown): value is SchemaRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  return (value as Record<string, unknown>).type === MessageType.SCHEMA;
}

/**
 * Check if value is a valid VSchemaRequest
 */
export function isVSchemaRequest(value: unknown): value is VSchemaRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  return (value as Record<string, unknown>).type === MessageType.VSCHEMA;
}

/**
 * Check if value is a valid ShardQueryRequest
 */
export function isShardQueryRequest(value: unknown): value is ShardQueryRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.SHARD_QUERY &&
    typeof obj.shard === 'string' &&
    typeof obj.sql === 'string' &&
    obj.sql !== ''
  );
}

/**
 * Check if value is a valid ShardExecuteRequest
 */
export function isShardExecuteRequest(value: unknown): value is ShardExecuteRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.SHARD_EXECUTE &&
    typeof obj.shard === 'string' &&
    typeof obj.sql === 'string' &&
    obj.sql !== ''
  );
}

/**
 * Check if value is a valid ShardBatchRequest
 */
export function isShardBatchRequest(value: unknown): value is ShardBatchRequest {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.SHARD_BATCH &&
    typeof obj.shard === 'string' &&
    Array.isArray(obj.statements)
  );
}

/**
 * Check if value is a valid QueryResponse
 */
export function isQueryResponse(value: unknown): value is QueryResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.RESULT &&
    isQueryResult(obj.result)
  );
}

/**
 * Check if value is a valid ExecuteResponse
 */
export function isExecuteResponse(value: unknown): value is ExecuteResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.RESULT &&
    isExecuteResult(obj.result)
  );
}

/**
 * Check if value is a valid BatchResponse
 */
export function isBatchResponse(value: unknown): value is BatchResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.RESULT &&
    isBatchResult(obj.result)
  );
}

/**
 * Check if value is a valid BeginResponse
 */
export function isBeginResponse(value: unknown): value is BeginResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.RESULT &&
    typeof obj.txId === 'string' &&
    Array.isArray(obj.shards)
  );
}

/**
 * Check if value is a valid StatusResponse
 */
export function isStatusResponse(value: unknown): value is StatusResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.RESULT &&
    isClusterStatus(obj.status)
  );
}

/**
 * Check if value is a valid HealthResponse
 */
export function isHealthResponse(value: unknown): value is HealthResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  if (obj.type !== MessageType.RESULT) {
    return false;
  }
  if (Array.isArray(obj.health)) {
    return obj.health.every(isShardHealth);
  }
  return isShardHealth(obj.health);
}

/**
 * Check if value is a valid SchemaResponse
 */
export function isSchemaResponse(value: unknown): value is SchemaResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.RESULT &&
    Array.isArray(obj.tables)
  );
}

/**
 * Check if value is a valid VSchemaResponse
 */
export function isVSchemaResponse(value: unknown): value is VSchemaResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.RESULT &&
    isVSchema(obj.vschema)
  );
}

/**
 * Check if value is a valid ErrorResponse
 */
export function isErrorResponse(value: unknown): value is ErrorResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  const obj = value as Record<string, unknown>;
  return (
    obj.type === MessageType.ERROR &&
    typeof obj.code === 'string' &&
    typeof obj.message === 'string'
  );
}

/**
 * Check if value is a valid AckResponse
 */
export function isAckResponse(value: unknown): value is AckResponse {
  if (!isRpcMessage(value)) {
    return false;
  }
  return (value as Record<string, unknown>).type === MessageType.ACK;
}

/**
 * Check if value is any valid Request type
 */
export function isRequest(value: unknown): value is Request {
  if (!isRpcMessage(value)) {
    return false;
  }
  const type = (value as RpcMessage).type;
  return (
    type === MessageType.QUERY ||
    type === MessageType.EXECUTE ||
    type === MessageType.BATCH ||
    type === MessageType.BEGIN ||
    type === MessageType.COMMIT ||
    type === MessageType.ROLLBACK ||
    type === MessageType.STATUS ||
    type === MessageType.HEALTH ||
    type === MessageType.SCHEMA ||
    type === MessageType.VSCHEMA ||
    type === MessageType.SHARD_QUERY ||
    type === MessageType.SHARD_EXECUTE ||
    type === MessageType.SHARD_BATCH
  );
}

/**
 * Check if value is any valid Response type
 */
export function isResponse(value: unknown): value is Response {
  if (!isRpcMessage(value)) {
    return false;
  }
  const type = (value as RpcMessage).type;
  return (
    type === MessageType.RESULT ||
    type === MessageType.ERROR ||
    type === MessageType.ACK
  );
}
