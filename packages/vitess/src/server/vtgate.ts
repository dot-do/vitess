/**
 * VTGate - Query Router
 *
 * Responsible for:
 * - Parsing incoming queries
 * - Determining target shard(s) using VSchema/Vindexes
 * - Single-shard routing for point queries
 * - Scatter-gather for cross-shard queries
 * - Result aggregation (COUNT, SUM, AVG, MIN, MAX)
 * - Transaction coordination (2PC for cross-shard)
 */

import type { VSchema, TableVSchema } from './vschema.js';
import type { Vindex, VindexType } from './vindexes.js';

/**
 * Shard routing result
 */
export interface ShardRoute {
  /** Target keyspace */
  keyspace: string;
  /** Target shard(s) */
  shards: string[];
  /** Whether this is a scatter query */
  scatter: boolean;
}

/**
 * Query plan types
 */
export type QueryPlanType =
  | 'single_shard'      // Route to exactly one shard
  | 'scatter'           // Route to all shards
  | 'scatter_aggregate' // Scatter + aggregate results
  | 'lookup'            // Lookup vindex to find shard
  | 'unsharded';        // Unsharded keyspace

/**
 * Query execution plan
 */
export interface QueryPlan {
  type: QueryPlanType;
  keyspace: string;
  table: string;
  shards: string[];
  sql: string;
  params?: unknown[];
  aggregations?: AggregationType[];
}

/**
 * Aggregation types supported
 */
export type AggregationType = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';

/**
 * VTGate configuration
 */
export interface VTGateConfig {
  /** VSchema configuration */
  vschema: VSchema;
  /** Available shards per keyspace */
  shards: Map<string, string[]>;
  /** VTTablet stub connections */
  tablets?: Map<string, VTTabletStub>;
}

/**
 * VTTablet stub interface (for routing)
 */
export interface VTTabletStub {
  /** Shard identifier */
  shard: string;
  /** Execute query on this tablet */
  execute(sql: string, params?: unknown[]): Promise<QueryResult>;
}

/**
 * Query result from tablet
 */
export interface QueryResult {
  rows: Record<string, unknown>[];
  rowCount: number;
  fields: FieldInfo[];
}

/**
 * Field metadata
 */
export interface FieldInfo {
  name: string;
  type: string;
}

/**
 * VTGate query router
 */
export class VTGate {
  private config: VTGateConfig;

  constructor(config: VTGateConfig) {
    this.config = config;
  }

  /**
   * Route a query to the appropriate shard(s)
   */
  route(sql: string, params?: unknown[]): ShardRoute {
    // TODO: Implement query routing
    throw new Error('Not implemented');
  }

  /**
   * Plan query execution
   */
  plan(sql: string, params?: unknown[]): QueryPlan {
    // TODO: Implement query planning
    throw new Error('Not implemented');
  }

  /**
   * Execute a query (routing + execution + aggregation)
   */
  async execute(sql: string, params?: unknown[]): Promise<QueryResult> {
    // TODO: Implement query execution
    throw new Error('Not implemented');
  }

  /**
   * Execute scatter-gather query
   */
  async scatter(plan: QueryPlan): Promise<QueryResult[]> {
    // TODO: Implement scatter-gather
    throw new Error('Not implemented');
  }

  /**
   * Aggregate results from multiple shards
   */
  aggregate(results: QueryResult[], aggregations: AggregationType[]): QueryResult {
    // TODO: Implement aggregation
    throw new Error('Not implemented');
  }
}

/**
 * Create a VTGate instance
 */
export function createVTGate(config: VTGateConfig): VTGate {
  return new VTGate(config);
}
