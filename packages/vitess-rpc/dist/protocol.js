/**
 * CapnWeb RPC Protocol for Vitess.do
 *
 * Defines the wire protocol between VitessClient and VTGate,
 * and between VTGate and VTTablet Durable Objects.
 */
/**
 * RPC message types
 */
export var MessageType;
(function (MessageType) {
    // Query operations
    MessageType[MessageType["QUERY"] = 1] = "QUERY";
    MessageType[MessageType["EXECUTE"] = 2] = "EXECUTE";
    MessageType[MessageType["BATCH"] = 3] = "BATCH";
    // Transaction operations
    MessageType[MessageType["BEGIN"] = 16] = "BEGIN";
    MessageType[MessageType["COMMIT"] = 17] = "COMMIT";
    MessageType[MessageType["ROLLBACK"] = 18] = "ROLLBACK";
    // Admin operations
    MessageType[MessageType["STATUS"] = 32] = "STATUS";
    MessageType[MessageType["HEALTH"] = 33] = "HEALTH";
    MessageType[MessageType["SCHEMA"] = 34] = "SCHEMA";
    MessageType[MessageType["VSCHEMA"] = 35] = "VSCHEMA";
    // Shard operations
    MessageType[MessageType["SHARD_QUERY"] = 48] = "SHARD_QUERY";
    MessageType[MessageType["SHARD_EXECUTE"] = 49] = "SHARD_EXECUTE";
    MessageType[MessageType["SHARD_BATCH"] = 50] = "SHARD_BATCH";
    // Response types
    MessageType[MessageType["RESULT"] = 128] = "RESULT";
    MessageType[MessageType["ERROR"] = 129] = "ERROR";
    MessageType[MessageType["ACK"] = 130] = "ACK";
})(MessageType || (MessageType = {}));
/**
 * Create a unique message ID
 */
export function createMessageId() {
    return `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;
}
/**
 * Create a query request
 */
export function createQueryRequest(sql, params, options) {
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
export function createExecuteRequest(sql, params, options) {
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
export function createErrorResponse(requestId, code, message, options) {
    return {
        type: MessageType.ERROR,
        id: requestId,
        timestamp: Date.now(),
        code,
        message,
        ...options,
    };
}
//# sourceMappingURL=protocol.js.map