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
import {
  getTableVSchema,
  getPrimaryVindex,
  isSharded,
  getShards,
} from './vschema.js';
import type { Vindex, VindexType, KeyspaceId } from './vindexes.js';
import {
  createVindex,
  routeToShard,
  keyspaceIdInShard,
} from './vindexes.js';
import {
  mergeResults,
  aggregateCount,
  aggregateSum,
  aggregateAvg,
  aggregateMin,
  aggregateMax,
  mergeSorted,
  type AggregationFunction,
  type SortSpec,
} from './aggregation.js';

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
  /** Normalized table name */
  table?: string;
  /** Whether a lookup vindex was used */
  usedLookup?: boolean;
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
  orderBy?: SortSpec[];
  limit?: number;
  offset?: number;
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
  /** Default keyspace */
  defaultKeyspace?: string;
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
 * Simple SQL parser for extracting table, where clause, etc.
 */
interface ParsedQuery {
  type: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' | 'OTHER';
  table?: string;
  keyspace?: string;
  whereClause?: string;
  shardingKeyValue?: unknown;
  shardingKeyColumn?: string;
  aggregations?: AggregationType[];
  orderBy?: SortSpec[];
  limit?: number;
  offset?: number;
}

/**
 * Parse SQL query to extract routing information
 */
function parseQuery(sql: string, params?: unknown[]): ParsedQuery {
  const normalizedSql = sql.trim().toUpperCase();
  const originalSql = sql.trim();

  // Determine query type
  let type: ParsedQuery['type'] = 'OTHER';
  if (normalizedSql.startsWith('SELECT')) type = 'SELECT';
  else if (normalizedSql.startsWith('INSERT')) type = 'INSERT';
  else if (normalizedSql.startsWith('UPDATE')) type = 'UPDATE';
  else if (normalizedSql.startsWith('DELETE')) type = 'DELETE';

  // Extract table name
  let table: string | undefined;
  let keyspace: string | undefined;

  // Match various SQL patterns
  const fromMatch = normalizedSql.match(/\bFROM\s+([`"]?[\w.]+[`"]?)/i);
  const intoMatch = normalizedSql.match(/\bINTO\s+([`"]?[\w.]+[`"]?)/i);
  const updateMatch = normalizedSql.match(/\bUPDATE\s+([`"]?[\w.]+[`"]?)/i);

  let fullTableName: string | undefined;
  if (fromMatch) fullTableName = fromMatch[1];
  else if (intoMatch) fullTableName = intoMatch[1];
  else if (updateMatch) fullTableName = updateMatch[1];

  if (fullTableName) {
    // Remove quotes
    fullTableName = fullTableName.replace(/[`"]/g, '');

    // Check for keyspace.table format
    if (fullTableName.includes('.')) {
      const parts = fullTableName.split('.');
      keyspace = parts[0];
      table = parts[1];
    } else {
      table = fullTableName;
    }
  }

  // Extract WHERE clause
  const whereMatch = originalSql.match(/\bWHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|\s*$)/i);
  const whereClause = whereMatch ? whereMatch[1] : undefined;

  // Extract sharding key value from WHERE clause
  let shardingKeyValue: unknown;
  let shardingKeyColumn: string | undefined;

  if (whereClause) {
    // Look for equality conditions: column = $1, column = 'value', column = 123
    const equalityMatch = whereClause.match(/(\w+)\s*=\s*(?:\$(\d+)|'([^']*)'|(\d+))/i);
    if (equalityMatch) {
      shardingKeyColumn = equalityMatch[1];
      if (equalityMatch[2] && params) {
        // Parameter placeholder $n
        const paramIndex = parseInt(equalityMatch[2], 10) - 1;
        shardingKeyValue = params[paramIndex];
      } else if (equalityMatch[3] !== undefined) {
        // String literal
        shardingKeyValue = equalityMatch[3];
      } else if (equalityMatch[4] !== undefined) {
        // Number literal
        shardingKeyValue = parseInt(equalityMatch[4], 10);
      }
    }
  }

  // Detect aggregations in SELECT clause
  const aggregations: AggregationType[] = [];
  if (type === 'SELECT') {
    if (/\bCOUNT\s*\(/i.test(normalizedSql)) aggregations.push('COUNT');
    if (/\bSUM\s*\(/i.test(normalizedSql)) aggregations.push('SUM');
    if (/\bAVG\s*\(/i.test(normalizedSql)) aggregations.push('AVG');
    if (/\bMIN\s*\(/i.test(normalizedSql)) aggregations.push('MIN');
    if (/\bMAX\s*\(/i.test(normalizedSql)) aggregations.push('MAX');
  }

  // Extract ORDER BY
  const orderBy: SortSpec[] = [];
  const orderByMatch = originalSql.match(/\bORDER\s+BY\s+(.+?)(?:\s+LIMIT|\s*$)/i);
  if (orderByMatch) {
    const orderClauses = orderByMatch[1].split(',');
    for (const clause of orderClauses) {
      const orderParts = clause.trim().match(/(\w+)(?:\s+(ASC|DESC))?/i);
      if (orderParts) {
        orderBy.push({
          column: orderParts[1],
          direction: (orderParts[2]?.toUpperCase() === 'DESC' ? 'DESC' : 'ASC') as 'ASC' | 'DESC',
        });
      }
    }
  }

  // Extract LIMIT and OFFSET
  let limit: number | undefined;
  let offset: number | undefined;
  const limitMatch = originalSql.match(/\bLIMIT\s+(\d+)/i);
  if (limitMatch) {
    limit = parseInt(limitMatch[1], 10);
  }
  const offsetMatch = originalSql.match(/\bOFFSET\s+(\d+)/i);
  if (offsetMatch) {
    offset = parseInt(offsetMatch[1], 10);
  }

  return {
    type,
    table,
    keyspace,
    whereClause,
    shardingKeyValue,
    shardingKeyColumn,
    aggregations: aggregations.length > 0 ? aggregations : undefined,
    orderBy: orderBy.length > 0 ? orderBy : undefined,
    limit,
    offset,
  };
}

/**
 * Extract sharding key value from INSERT statement
 */
function extractInsertShardingKey(sql: string, shardingColumn: string, params?: unknown[]): unknown {
  // Match INSERT INTO table (col1, col2, ...) VALUES ($1, $2, ...)
  const insertMatch = sql.match(/INSERT\s+INTO\s+\S+\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i);
  if (!insertMatch) return undefined;

  const columns = insertMatch[1].split(',').map(c => c.trim().toLowerCase());
  const values = insertMatch[2].split(',').map(v => v.trim());

  const columnIndex = columns.indexOf(shardingColumn.toLowerCase());
  if (columnIndex === -1) return undefined;

  const valueExpr = values[columnIndex];
  if (!valueExpr) return undefined;

  // Check if it's a parameter placeholder
  const paramMatch = valueExpr.match(/\$(\d+)/);
  if (paramMatch && params) {
    const paramIndex = parseInt(paramMatch[1], 10) - 1;
    return params[paramIndex];
  }

  // Check if it's a string literal
  const stringMatch = valueExpr.match(/^'([^']*)'$/);
  if (stringMatch) return stringMatch[1];

  // Check if it's a number
  const numMatch = valueExpr.match(/^(-?\d+(?:\.\d+)?)$/);
  if (numMatch) return parseFloat(numMatch[1]);

  return undefined;
}

/**
 * VTGate query router
 */
export class VTGate {
  private config: VTGateConfig;
  private vindexCache: Map<string, Vindex> = new Map();

  constructor(config: VTGateConfig) {
    this.config = config;
  }

  /**
   * Get or create vindex instance
   */
  private getVindex(keyspace: string, vindexName: string): Vindex | undefined {
    const cacheKey = `${keyspace}:${vindexName}`;
    if (this.vindexCache.has(cacheKey)) {
      return this.vindexCache.get(cacheKey);
    }

    const ks = this.config.vschema.keyspaces[keyspace];
    if (!ks || !ks.vindexes) return undefined;

    const vindexDef = ks.vindexes[vindexName];
    if (!vindexDef) return undefined;

    const vindex = createVindex(vindexDef.type, vindexDef.params);
    this.vindexCache.set(cacheKey, vindex);
    return vindex;
  }

  /**
   * Route a query to the appropriate shard(s)
   */
  route(sql: string, params?: unknown[]): ShardRoute {
    const parsed = parseQuery(sql, params);
    let keyspace = parsed.keyspace || this.config.defaultKeyspace;

    // If no keyspace specified, try to infer from table name
    if (!keyspace && parsed.table) {
      // Look for the table in all keyspaces
      for (const [ksName, ks] of Object.entries(this.config.vschema.keyspaces)) {
        if (ks.tables[parsed.table]) {
          keyspace = ksName;
          break;
        }
      }
    }

    // If still no keyspace and there's only one, use it
    if (!keyspace) {
      const keyspaceNames = Object.keys(this.config.vschema.keyspaces);
      if (keyspaceNames.length === 1) {
        keyspace = keyspaceNames[0];
      }
    }

    if (!keyspace) {
      throw new Error('No keyspace specified and no default keyspace configured');
    }

    // Validate keyspace exists
    if (!this.config.vschema.keyspaces[keyspace]) {
      throw new Error(`Keyspace '${keyspace}' not found`);
    }

    // Validate table exists (if specified) - case insensitive lookup
    if (parsed.table && keyspace) {
      const tables = this.config.vschema.keyspaces[keyspace]?.tables;
      if (tables) {
        const tableNames = Object.keys(tables);
        const normalizedTableName = parsed.table.toLowerCase();
        const matchedTable = tableNames.find(t => t.toLowerCase() === normalizedTableName);
        if (!matchedTable) {
          throw new Error(`Table '${parsed.table}' not found in keyspace '${keyspace}'`);
        }
        // Update parsed.table to the correct case
        parsed.table = matchedTable;
      }
    }

    // Validate query type is supported
    if (parsed.type === 'OTHER') {
      throw new Error(`Unsupported SQL syntax`);
    }

    // Check if keyspace is sharded
    if (!isSharded(this.config.vschema, keyspace)) {
      // Unsharded - route to the single shard
      const shards = this.config.shards.get(keyspace) || ['-'];
      return {
        keyspace,
        shards: shards.slice(0, 1),
        scatter: false,
        table: parsed.table,
      };
    }

    // Sharded keyspace - need to determine shard
    const table = parsed.table;
    if (!table) {
      // No table specified - scatter to all shards
      const shards = this.config.shards.get(keyspace) || [];
      return {
        keyspace,
        shards,
        scatter: true,
        table,
      };
    }

    // Get primary vindex for the table
    const primaryVindex = getPrimaryVindex(this.config.vschema, keyspace, table);
    if (!primaryVindex) {
      // No vindex - scatter
      const shards = this.config.shards.get(keyspace) || [];
      return {
        keyspace,
        shards,
        scatter: true,
        table,
      };
    }

    // Get table schema to find sharding column
    const tableSchema = getTableVSchema(this.config.vschema, keyspace, table);
    const primaryVindexColumn = tableSchema?.column_vindexes?.[0];
    const shardingColumn = primaryVindexColumn?.column || primaryVindexColumn?.columns?.[0];

    // Check if we have a sharding key value in the query
    if (parsed.shardingKeyValue !== undefined &&
        shardingColumn &&
        parsed.shardingKeyColumn?.toLowerCase() === shardingColumn.toLowerCase()) {
      // Single shard routing
      const vindex = this.getVindex(keyspace, primaryVindexColumn.name);
      if (vindex) {
        try {
          const keyspaceIds = vindex.map(parsed.shardingKeyValue);
          const allShards = this.config.shards.get(keyspace) || [];

          // Find the shard containing this keyspace ID
          const targetShard = routeToShard(keyspaceIds[0], allShards);
          return {
            keyspace,
            shards: [targetShard],
            scatter: false,
            table,
          };
        } catch (e) {
          // Fall through to scatter
        }
      }
    }

    // Check if query uses a secondary (lookup) vindex
    if (tableSchema?.column_vindexes && tableSchema.column_vindexes.length > 1 &&
        parsed.shardingKeyColumn) {
      // Check if the WHERE column matches any secondary vindex
      for (let i = 1; i < tableSchema.column_vindexes.length; i++) {
        const colVindex = tableSchema.column_vindexes[i];
        const vindexColumn = colVindex.column || colVindex.columns?.[0];
        if (vindexColumn?.toLowerCase() === parsed.shardingKeyColumn.toLowerCase()) {
          // This uses a lookup vindex
          const vindexDef = this.config.vschema.keyspaces[keyspace]?.vindexes?.[colVindex.name];
          if (vindexDef && (vindexDef.type === 'lookup' || vindexDef.type === 'lookup_unique' || vindexDef.type === 'lookup_hash')) {
            // Mark as using lookup
            const allShards = this.config.shards.get(keyspace) || [];
            return {
              keyspace,
              shards: allShards,
              scatter: true, // Will be resolved after lookup
              table,
              usedLookup: true,
            };
          }
        }
      }
    }

    // For INSERT on sharded tables without sharding key value, try to extract from VALUES
    if (parsed.type === 'INSERT' && shardingColumn && parsed.shardingKeyValue === undefined) {
      // Try to extract from INSERT statement
      const insertKeyValue = extractInsertShardingKey(sql, shardingColumn, params);
      if (insertKeyValue !== undefined) {
        // Single shard routing based on INSERT value
        const vindex = this.getVindex(keyspace, primaryVindexColumn!.name);
        if (vindex) {
          try {
            const keyspaceIds = vindex.map(insertKeyValue);
            const allShards = this.config.shards.get(keyspace) || [];
            const targetShard = routeToShard(keyspaceIds[0], allShards);
            return {
              keyspace,
              shards: [targetShard],
              scatter: false,
              table,
            };
          } catch (e) {
            // Fall through to error
          }
        }
      }
      // No sharding key found for INSERT - this is an error
      throw new Error(`Sharding key '${shardingColumn}' is required for INSERT on sharded table '${table}'`);
    }
    // UPDATE and DELETE without sharding key will scatter (allowed but not recommended)

    // No sharding key - scatter to all shards
    const shards = this.config.shards.get(keyspace) || [];
    return {
      keyspace,
      shards,
      scatter: true,
      table,
    };
  }

  /**
   * Plan query execution
   */
  plan(sql: string, params?: unknown[]): QueryPlan {
    const parsed = parseQuery(sql, params);
    const route = this.route(sql, params);

    let type: QueryPlanType;
    if (!isSharded(this.config.vschema, route.keyspace)) {
      type = 'unsharded';
    } else if (route.usedLookup) {
      type = 'lookup';
    } else if (route.scatter) {
      type = parsed.aggregations ? 'scatter_aggregate' : 'scatter';
    } else {
      type = 'single_shard';
    }

    return {
      type,
      keyspace: route.keyspace,
      table: route.table || parsed.table || '',
      shards: route.shards,
      sql,
      params,
      aggregations: parsed.aggregations,
      orderBy: parsed.orderBy,
      limit: parsed.limit,
      offset: parsed.offset,
    };
  }

  /**
   * Execute a query (routing + execution + aggregation)
   */
  async execute(sql: string, params?: unknown[]): Promise<QueryResult> {
    const plan = this.plan(sql, params);

    if (!this.config.tablets || this.config.tablets.size === 0) {
      throw new Error('No tablets configured');
    }

    if (plan.type === 'unsharded' || plan.type === 'single_shard') {
      // Execute on single shard
      const tablet = this.config.tablets.get(plan.shards[0]);
      if (!tablet) {
        throw new Error(`No tablet found for shard ${plan.shards[0]}`);
      }
      return tablet.execute(plan.sql, plan.params);
    }

    // Scatter-gather
    const shardResults = await this.scatter(plan);

    // Aggregate if needed
    if (plan.aggregations && plan.aggregations.length > 0) {
      return this.aggregate(shardResults, plan.aggregations);
    }

    // Merge results
    if (plan.orderBy && plan.orderBy.length > 0) {
      return mergeSorted(shardResults, plan.orderBy, plan.limit, plan.offset);
    }

    const merged = mergeResults(shardResults);

    // Apply limit/offset if specified
    if (plan.limit !== undefined || plan.offset !== undefined) {
      const start = plan.offset || 0;
      const end = plan.limit !== undefined ? start + plan.limit : undefined;
      return {
        rows: merged.rows.slice(start, end),
        rowCount: end !== undefined ? Math.min(plan.limit!, merged.rows.length - start) : merged.rows.length - start,
        fields: merged.fields,
      };
    }

    return merged;
  }

  /**
   * Execute scatter-gather query
   */
  async scatter(plan: QueryPlan): Promise<QueryResult[]> {
    if (!this.config.tablets) {
      throw new Error('No tablets configured');
    }

    const shardPromises: { shard: string; promise: Promise<QueryResult> }[] = [];

    for (const shard of plan.shards) {
      const tablet = this.config.tablets.get(shard);
      if (!tablet) {
        throw new Error(`No tablet found for shard ${shard}`);
      }
      shardPromises.push({
        shard,
        promise: tablet.execute(plan.sql, plan.params).catch((error) => {
          // Wrap error with shard information
          const wrappedError = new Error(`Error on shard ${shard}: ${error.message}`);
          (wrappedError as any).shard = shard;
          (wrappedError as any).originalError = error;
          throw wrappedError;
        }),
      });
    }

    return Promise.all(shardPromises.map(sp => sp.promise));
  }

  /**
   * Aggregate results from multiple shards
   */
  aggregate(results: QueryResult[], aggregations: AggregationType[]): QueryResult {
    if (results.length === 0) {
      return { rows: [], rowCount: 0, fields: [] };
    }

    const aggregatedRow: Record<string, unknown> = {};
    const fields: FieldInfo[] = [];

    // Get the first row to understand the column structure
    const sampleRow = results[0].rows[0] || {};
    const columns = Object.keys(sampleRow);

    for (const agg of aggregations) {
      // Find the column for this aggregation
      // Look for columns that look like they might be the result of this aggregation
      const aggColumnName = columns.find(col =>
        col.toLowerCase().includes(agg.toLowerCase()) ||
        col.toLowerCase() === 'count' ||
        col.toLowerCase() === 'sum' ||
        col.toLowerCase() === 'avg' ||
        col.toLowerCase() === 'min' ||
        col.toLowerCase() === 'max'
      ) || columns[0];

      let value: unknown;
      switch (agg) {
        case 'COUNT':
          value = aggregateCount(results, aggColumnName);
          break;
        case 'SUM':
          value = aggregateSum(results, aggColumnName);
          break;
        case 'AVG':
          // For AVG across shards, we need sum and count
          // If we have them, use them; otherwise, estimate
          const sumCol = columns.find(c => c.toLowerCase().includes('sum')) || aggColumnName;
          const countCol = columns.find(c => c.toLowerCase().includes('count'));
          if (countCol) {
            value = aggregateAvg(results, sumCol, countCol);
          } else {
            // Simple average of the values
            let total = 0;
            let count = 0;
            for (const result of results) {
              for (const row of result.rows) {
                const v = row[aggColumnName];
                if (v !== null && v !== undefined) {
                  total += Number(v);
                  count++;
                }
              }
            }
            value = count > 0 ? total / count : null;
          }
          break;
        case 'MIN':
          value = aggregateMin(results, aggColumnName);
          break;
        case 'MAX':
          value = aggregateMax(results, aggColumnName);
          break;
      }

      aggregatedRow[aggColumnName] = value;
      fields.push({ name: aggColumnName, type: 'numeric' });
    }

    return {
      rows: [aggregatedRow],
      rowCount: 1,
      fields,
    };
  }
}

/**
 * Create a VTGate instance
 */
export function createVTGate(config: VTGateConfig): VTGate {
  return new VTGate(config);
}
