/**
 * @dotdo/vitess-rpc
 *
 * CapnWeb RPC protocol and unified types for Vitess.do
 */
export type { StorageEngineType, Row, Field, QueryResult, ExecuteResult, BatchResult, ShardId, Keyspace, VindexType, VindexDef, TableDef, VSchema, IsolationLevel, TransactionOptions, ShardHealth, ClusterStatus, QueryTarget, AggregateFunction, AggregateSpec, } from './types.js';
export { MessageType, createMessageId, createQueryRequest, createExecuteRequest, createErrorResponse, } from './protocol.js';
export type { RpcMessage, QueryRequest, ExecuteRequest, BatchRequest, BeginRequest, CommitRequest, RollbackRequest, StatusRequest, HealthRequest, SchemaRequest, VSchemaRequest, ShardQueryRequest, ShardExecuteRequest, ShardBatchRequest, QueryResponse, ExecuteResponse, BatchResponse, BeginResponse, StatusResponse, HealthResponse, SchemaResponse, VSchemaResponse, ErrorResponse, AckResponse, Request, Response, } from './protocol.js';
//# sourceMappingURL=index.d.ts.map