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
