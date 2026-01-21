/**
 * @dotdo/vitess-rpc - Serialization Module
 *
 * JSON serialization and deserialization for RPC messages.
 */

import {
  MessageType,
  isRpcMessage,
  isRequest,
  isResponse,
  isQueryRequest,
  isExecuteRequest,
  isBatchRequest,
  isBeginRequest,
  isCommitRequest,
  isRollbackRequest,
  isStatusRequest,
  isHealthRequest,
  isSchemaRequest,
  isVSchemaRequest,
  isShardQueryRequest,
  isShardExecuteRequest,
  isShardBatchRequest,
} from './protocol.js';

import type {
  Request,
  Response,
  RpcMessage,
} from './protocol.js';

/**
 * Custom replacer for JSON.stringify that handles special types
 */
function jsonReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') {
    return value.toString();
  }
  if (value instanceof Uint8Array) {
    // Convert binary data to base64
    return {
      __type: 'Uint8Array',
      data: btoa(String.fromCharCode(...value)),
    };
  }
  return value;
}

/**
 * Custom reviver for JSON.parse that handles special types
 */
function jsonReviver(_key: string, value: unknown): unknown {
  if (
    typeof value === 'object' &&
    value !== null &&
    (value as Record<string, unknown>).__type === 'Uint8Array'
  ) {
    const base64 = (value as Record<string, unknown>).data as string;
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }
  return value;
}

/**
 * Serialize a request to JSON string
 */
export function serializeRequest(request: Request): string {
  return JSON.stringify(request, jsonReplacer);
}

/**
 * Serialize a response to JSON string
 */
export function serializeResponse(response: Response): string {
  return JSON.stringify(response, jsonReplacer);
}

/**
 * Serialize any RPC message to JSON string
 */
export function serializeMessage(message: RpcMessage): string {
  return JSON.stringify(message, jsonReplacer);
}

/**
 * Validate that a parsed request has the required fields
 */
function validateParsedRequest(value: unknown): void {
  if (!isRpcMessage(value)) {
    throw new Error('Invalid request: missing required RpcMessage fields (type, id, timestamp)');
  }

  const obj = value as Record<string, unknown>;
  const type = obj.type as MessageType;

  switch (type) {
    case MessageType.QUERY:
      if (!isQueryRequest(value)) {
        throw new Error('Invalid QueryRequest: missing or empty sql field');
      }
      break;
    case MessageType.EXECUTE:
      if (!isExecuteRequest(value)) {
        throw new Error('Invalid ExecuteRequest: missing or empty sql field');
      }
      break;
    case MessageType.BATCH:
      if (!isBatchRequest(value)) {
        throw new Error('Invalid BatchRequest: missing statements array');
      }
      break;
    case MessageType.BEGIN:
      if (!isBeginRequest(value)) {
        throw new Error('Invalid BeginRequest');
      }
      break;
    case MessageType.COMMIT:
      if (!isCommitRequest(value)) {
        throw new Error('Invalid CommitRequest: missing txId');
      }
      break;
    case MessageType.ROLLBACK:
      if (!isRollbackRequest(value)) {
        throw new Error('Invalid RollbackRequest: missing txId');
      }
      break;
    case MessageType.STATUS:
      if (!isStatusRequest(value)) {
        throw new Error('Invalid StatusRequest');
      }
      break;
    case MessageType.HEALTH:
      if (!isHealthRequest(value)) {
        throw new Error('Invalid HealthRequest');
      }
      break;
    case MessageType.SCHEMA:
      if (!isSchemaRequest(value)) {
        throw new Error('Invalid SchemaRequest');
      }
      break;
    case MessageType.VSCHEMA:
      if (!isVSchemaRequest(value)) {
        throw new Error('Invalid VSchemaRequest');
      }
      break;
    case MessageType.SHARD_QUERY:
      if (!isShardQueryRequest(value)) {
        throw new Error('Invalid ShardQueryRequest: missing shard or sql field');
      }
      break;
    case MessageType.SHARD_EXECUTE:
      if (!isShardExecuteRequest(value)) {
        throw new Error('Invalid ShardExecuteRequest: missing shard or sql field');
      }
      break;
    case MessageType.SHARD_BATCH:
      if (!isShardBatchRequest(value)) {
        throw new Error('Invalid ShardBatchRequest: missing shard or statements');
      }
      break;
    default:
      throw new Error(`Invalid request type: ${type}`);
  }
}

/**
 * Validate that a parsed response has the required structure
 */
function validateParsedResponse(value: unknown): void {
  if (!isRpcMessage(value)) {
    throw new Error('Invalid response: missing required RpcMessage fields (type, id, timestamp)');
  }

  if (!isResponse(value)) {
    throw new Error('Invalid response type');
  }
}

/**
 * Deserialize a JSON string to a Request
 */
export function deserializeRequest(json: string): Request {
  let parsed: unknown;
  try {
    parsed = JSON.parse(json, jsonReviver);
  } catch {
    throw new Error('Invalid JSON');
  }

  validateParsedRequest(parsed);
  return parsed as Request;
}

/**
 * Deserialize a JSON string to a Response
 */
export function deserializeResponse(json: string): Response {
  let parsed: unknown;
  try {
    parsed = JSON.parse(json, jsonReviver);
  } catch {
    throw new Error('Invalid JSON');
  }

  validateParsedResponse(parsed);
  return parsed as Response;
}

/**
 * Deserialize a JSON string to any RPC message
 */
export function deserializeMessage(json: string): RpcMessage {
  let parsed: unknown;
  try {
    parsed = JSON.parse(json, jsonReviver);
  } catch {
    throw new Error('Invalid JSON');
  }

  if (!isRpcMessage(parsed)) {
    throw new Error('Invalid message: missing required RpcMessage fields');
  }

  // Validate based on whether it's a request or response
  if (isRequest(parsed)) {
    validateParsedRequest(parsed);
  } else if (isResponse(parsed)) {
    validateParsedResponse(parsed);
  } else {
    throw new Error('Invalid message: unknown message type');
  }

  return parsed;
}

/**
 * Safely parse JSON without throwing
 * Returns null if parsing fails
 */
export function safeJsonParse(json: string): unknown | null {
  if (json === '') {
    return null;
  }
  try {
    return JSON.parse(json, jsonReviver);
  } catch {
    return null;
  }
}

/**
 * Safely stringify to JSON without throwing
 * Returns null if serialization fails (e.g., circular references)
 * Handles BigInt by converting to string
 */
export function safeJsonStringify(value: unknown): string | null {
  try {
    return JSON.stringify(value, jsonReplacer);
  } catch {
    return null;
  }
}
