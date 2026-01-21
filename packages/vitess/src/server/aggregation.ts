/**
 * Aggregation - Cross-shard Result Aggregation
 *
 * Handles aggregation of results from scatter-gather queries:
 * - COUNT: Sum of counts from all shards
 * - SUM: Sum of sums from all shards
 * - AVG: Weighted average (requires SUM and COUNT)
 * - MIN: Minimum across all shards
 * - MAX: Maximum across all shards
 *
 * Also handles:
 * - ORDER BY with LIMIT (merge sort)
 * - GROUP BY aggregation
 * - DISTINCT deduplication
 */

import type { QueryResult, FieldInfo } from './vttablet.js';

/**
 * Re-export QueryResult for aggregation module consumers
 */
export type { QueryResult };

/**
 * Aggregation function type
 */
export type AggregationFunction = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';

/**
 * Aggregation operation
 */
export interface AggregationOp {
  /** Aggregation function */
  function: AggregationFunction;
  /** Column/expression being aggregated */
  column: string;
  /** Alias for the result column */
  alias?: string;
  /** For AVG: requires both sum and count columns from shards */
  sumColumn?: string;
  countColumn?: string;
}

/**
 * Sort specification
 */
export interface SortSpec {
  column: string;
  direction: 'ASC' | 'DESC';
  nullsFirst?: boolean;
}

/**
 * Aggregation context
 */
export interface AggregationContext {
  /** Aggregation operations */
  aggregations: AggregationOp[];
  /** Group by columns */
  groupBy?: string[];
  /** Sort specifications */
  orderBy?: SortSpec[];
  /** Result limit */
  limit?: number;
  /** Result offset */
  offset?: number;
  /** Whether to deduplicate (DISTINCT) */
  distinct?: boolean;
}

/**
 * Merge results from multiple shards
 */
export function mergeResults(results: QueryResult[]): QueryResult {
  if (results.length === 0) {
    return { rows: [], rowCount: 0, fields: [] };
  }

  const allRows: Record<string, unknown>[] = [];
  let totalCount = 0;

  for (const result of results) {
    allRows.push(...result.rows);
    totalCount += result.rowCount;
  }

  return {
    rows: allRows,
    rowCount: totalCount,
    fields: results[0].fields || [],
  };
}

/**
 * Aggregate COUNT across shards
 */
export function aggregateCount(results: QueryResult[], column: string): number {
  let total = 0;

  for (const result of results) {
    for (const row of result.rows) {
      const value = row[column];
      if (value !== null && value !== undefined) {
        total += typeof value === 'bigint' ? Number(value) : Number(value);
      }
    }
  }

  return total;
}

/**
 * Aggregate SUM across shards
 */
export function aggregateSum(results: QueryResult[], column: string): number {
  let total = 0;

  for (const result of results) {
    for (const row of result.rows) {
      const value = row[column];
      if (value !== null && value !== undefined) {
        total += typeof value === 'bigint' ? Number(value) : Number(value);
      }
    }
  }

  return total;
}

/**
 * Aggregate AVG across shards (requires pre-computed SUM and COUNT)
 */
export function aggregateAvg(
  results: QueryResult[],
  sumColumn: string,
  countColumn: string
): number | null {
  let totalSum = 0;
  let totalCount = 0;

  for (const result of results) {
    for (const row of result.rows) {
      const sum = row[sumColumn];
      const count = row[countColumn];

      if (sum !== null && sum !== undefined) {
        totalSum += typeof sum === 'bigint' ? Number(sum) : Number(sum);
      }
      if (count !== null && count !== undefined) {
        totalCount += typeof count === 'bigint' ? Number(count) : Number(count);
      }
    }
  }

  if (totalCount === 0) {
    return null;
  }

  return totalSum / totalCount;
}

/**
 * Aggregate MIN across shards
 */
export function aggregateMin(results: QueryResult[], column: string): unknown {
  let min: unknown = undefined;

  for (const result of results) {
    for (const row of result.rows) {
      const value = row[column];
      if (value === null || value === undefined) continue;

      if (min === undefined) {
        min = value;
      } else if (compareValues(value, min, 'ASC') < 0) {
        min = value;
      }
    }
  }

  return min === undefined ? null : min;
}

/**
 * Aggregate MAX across shards
 */
export function aggregateMax(results: QueryResult[], column: string): unknown {
  let max: unknown = undefined;

  for (const result of results) {
    for (const row of result.rows) {
      const value = row[column];
      if (value === null || value === undefined) continue;

      if (max === undefined) {
        max = value;
      } else if (compareValues(value, max, 'ASC') > 0) {
        max = value;
      }
    }
  }

  return max === undefined ? null : max;
}

/**
 * Apply aggregations to merged results
 */
export function applyAggregations(
  results: QueryResult[],
  context: AggregationContext
): QueryResult {
  // Handle GROUP BY aggregation
  if (context.groupBy && context.groupBy.length > 0) {
    return groupAndAggregate(results, context.groupBy, context.aggregations);
  }

  // Handle simple aggregations (no GROUP BY)
  const aggregatedRow: Record<string, unknown> = {};
  const fields: FieldInfo[] = [];

  for (const agg of context.aggregations) {
    const alias = agg.alias || agg.column || agg.function.toLowerCase();

    switch (agg.function) {
      case 'COUNT':
        aggregatedRow[alias] = aggregateCount(results, agg.column);
        break;
      case 'SUM':
        aggregatedRow[alias] = aggregateSum(results, agg.column);
        break;
      case 'AVG':
        aggregatedRow[alias] = aggregateAvg(
          results,
          agg.sumColumn || 'sum',
          agg.countColumn || 'count'
        );
        break;
      case 'MIN':
        aggregatedRow[alias] = aggregateMin(results, agg.column);
        break;
      case 'MAX':
        aggregatedRow[alias] = aggregateMax(results, agg.column);
        break;
    }

    fields.push({ name: alias, type: 'numeric' });
  }

  return {
    rows: [aggregatedRow],
    rowCount: 1,
    fields,
  };
}

/**
 * Merge sorted results from multiple shards (merge sort)
 */
export function mergeSorted(
  results: QueryResult[],
  orderBy: SortSpec[],
  limit?: number,
  offset?: number
): QueryResult {
  if (results.length === 0) {
    return { rows: [], rowCount: 0, fields: [] };
  }

  // Create iterators for each result set
  const iterators: { rows: Record<string, unknown>[]; index: number }[] =
    results.map(r => ({ rows: r.rows, index: 0 }));

  const merged: Record<string, unknown>[] = [];
  const totalLimit = limit ? (offset || 0) + limit : Infinity;

  // Merge sort using a simple k-way merge
  while (merged.length < totalLimit) {
    let minIterator: number | null = null;
    let minRow: Record<string, unknown> | null = null;

    // Find the minimum row across all iterators
    for (let i = 0; i < iterators.length; i++) {
      const iter = iterators[i];
      if (iter.index >= iter.rows.length) continue;

      const row = iter.rows[iter.index];
      if (minRow === null) {
        minRow = row;
        minIterator = i;
      } else {
        const cmp = compareRows(row, minRow, orderBy);
        if (cmp < 0) {
          minRow = row;
          minIterator = i;
        }
      }
    }

    if (minRow === null) break;

    merged.push(minRow);
    iterators[minIterator!].index++;
  }

  // Apply offset
  const startIndex = offset || 0;
  const resultRows = merged.slice(startIndex, startIndex + (limit || merged.length));

  return {
    rows: resultRows,
    rowCount: resultRows.length,
    fields: results[0].fields || [],
  };
}

/**
 * Compare two rows based on ORDER BY spec
 */
function compareRows(
  a: Record<string, unknown>,
  b: Record<string, unknown>,
  orderBy: SortSpec[]
): number {
  for (const spec of orderBy) {
    const cmp = compareValues(a[spec.column], b[spec.column], spec.direction, spec.nullsFirst);
    if (cmp !== 0) return cmp;
  }
  return 0;
}

/**
 * Group results and aggregate
 */
export function groupAndAggregate(
  results: QueryResult[],
  groupBy: string[],
  aggregations: AggregationOp[]
): QueryResult {
  // Collect all rows
  const allRows = mergeResults(results).rows;

  // Group rows by groupBy columns
  const groups = new Map<string, Record<string, unknown>[]>();

  for (const row of allRows) {
    const groupKey = groupBy.map(col => JSON.stringify(row[col])).join('|');
    if (!groups.has(groupKey)) {
      groups.set(groupKey, []);
    }
    groups.get(groupKey)!.push(row);
  }

  // Aggregate each group
  const resultRows: Record<string, unknown>[] = [];
  const fields: FieldInfo[] = [];

  // Add group by columns to fields
  for (const col of groupBy) {
    fields.push({ name: col, type: 'unknown' });
  }

  // Add aggregation columns to fields
  for (const agg of aggregations) {
    const alias = agg.alias || agg.column || agg.function.toLowerCase();
    fields.push({ name: alias, type: 'numeric' });
  }

  for (const [groupKey, groupRows] of groups) {
    const row: Record<string, unknown> = {};

    // Add group by column values
    for (const col of groupBy) {
      row[col] = groupRows[0][col];
    }

    // Compute aggregations
    const groupResult: QueryResult = {
      rows: groupRows,
      rowCount: groupRows.length,
      fields: [],
    };

    for (const agg of aggregations) {
      const alias = agg.alias || agg.column || agg.function.toLowerCase();

      switch (agg.function) {
        case 'COUNT':
          if (agg.column === '*') {
            row[alias] = groupRows.length;
          } else {
            row[alias] = aggregateCount([groupResult], agg.column);
          }
          break;
        case 'SUM':
          row[alias] = aggregateSum([groupResult], agg.column);
          break;
        case 'AVG':
          const sum = aggregateSum([groupResult], agg.sumColumn || agg.column);
          const count = agg.countColumn
            ? aggregateCount([groupResult], agg.countColumn)
            : groupRows.length;
          row[alias] = count > 0 ? sum / count : null;
          break;
        case 'MIN':
          row[alias] = aggregateMin([groupResult], agg.column);
          break;
        case 'MAX':
          row[alias] = aggregateMax([groupResult], agg.column);
          break;
      }
    }

    resultRows.push(row);
  }

  return {
    rows: resultRows,
    rowCount: resultRows.length,
    fields,
  };
}

/**
 * Remove duplicates (DISTINCT)
 */
export function deduplicate(result: QueryResult, columns?: string[]): QueryResult {
  const seen = new Set<string>();
  const uniqueRows: Record<string, unknown>[] = [];

  for (const row of result.rows) {
    let key: string;
    if (columns && columns.length > 0) {
      key = columns.map(col => JSON.stringify(row[col])).join('|');
    } else {
      key = JSON.stringify(row);
    }

    if (!seen.has(key)) {
      seen.add(key);
      uniqueRows.push(row);
    }
  }

  return {
    rows: uniqueRows,
    rowCount: uniqueRows.length,
    fields: result.fields,
  };
}

/**
 * Compare two values for sorting
 */
export function compareValues(
  a: unknown,
  b: unknown,
  direction: 'ASC' | 'DESC',
  nullsFirst?: boolean
): number {
  // Handle nulls
  if (a === null || a === undefined) {
    if (b === null || b === undefined) return 0;
    return nullsFirst ? -1 : 1;
  }
  if (b === null || b === undefined) {
    return nullsFirst ? 1 : -1;
  }

  let cmp: number;

  // Compare based on type
  if (typeof a === 'number' && typeof b === 'number') {
    cmp = a - b;
  } else if (typeof a === 'bigint' && typeof b === 'bigint') {
    cmp = a < b ? -1 : a > b ? 1 : 0;
  } else if (typeof a === 'string' && typeof b === 'string') {
    cmp = a.localeCompare(b);
  } else if (a instanceof Date && b instanceof Date) {
    cmp = a.getTime() - b.getTime();
  } else if (typeof a === 'string' && typeof b === 'string') {
    // Try parsing as dates
    const dateA = new Date(a);
    const dateB = new Date(b);
    if (!isNaN(dateA.getTime()) && !isNaN(dateB.getTime())) {
      cmp = dateA.getTime() - dateB.getTime();
    } else {
      cmp = a.localeCompare(b);
    }
  } else {
    // Fallback to string comparison
    cmp = String(a).localeCompare(String(b));
  }

  return direction === 'DESC' ? -cmp : cmp;
}

/**
 * Aggregation state for a group
 */
interface AggregationState {
  count: number;
  sum: Map<string, number>;
  min: Map<string, unknown>;
  max: Map<string, unknown>;
  groupKey: Record<string, unknown>;
}

/**
 * Aggregator class for streaming aggregation
 */
export class StreamingAggregator {
  private context: AggregationContext;
  private buffer: Record<string, unknown>[] = [];
  private groups: Map<string, AggregationState> = new Map();

  constructor(context: AggregationContext) {
    this.context = context;
  }

  /**
   * Add rows from a shard
   */
  addRows(rows: Record<string, unknown>[]): void {
    if (this.context.groupBy && this.context.groupBy.length > 0) {
      // GROUP BY aggregation
      for (const row of rows) {
        const groupKey = this.context.groupBy
          .map(col => JSON.stringify(row[col]))
          .join('|');

        if (!this.groups.has(groupKey)) {
          const state: AggregationState = {
            count: 0,
            sum: new Map(),
            min: new Map(),
            max: new Map(),
            groupKey: {},
          };

          for (const col of this.context.groupBy) {
            state.groupKey[col] = row[col];
          }

          this.groups.set(groupKey, state);
        }

        const state = this.groups.get(groupKey)!;
        state.count++;

        for (const agg of this.context.aggregations) {
          const value = row[agg.column];
          if (value === null || value === undefined) continue;

          const numValue = typeof value === 'number' ? value : Number(value);

          if (agg.function === 'SUM' || agg.function === 'AVG') {
            const current = state.sum.get(agg.column) || 0;
            state.sum.set(agg.column, current + numValue);
          }

          if (agg.function === 'MIN') {
            const current = state.min.get(agg.column);
            if (current === undefined || compareValues(value, current, 'ASC') < 0) {
              state.min.set(agg.column, value);
            }
          }

          if (agg.function === 'MAX') {
            const current = state.max.get(agg.column);
            if (current === undefined || compareValues(value, current, 'ASC') > 0) {
              state.max.set(agg.column, value);
            }
          }
        }
      }
    } else {
      // Simple aggregation (no GROUP BY)
      for (const row of rows) {
        this.buffer.push(row);
      }
    }
  }

  /**
   * Finalize and get results
   */
  finalize(): QueryResult {
    const fields: FieldInfo[] = [];
    const rows: Record<string, unknown>[] = [];

    if (this.context.groupBy && this.context.groupBy.length > 0) {
      // GROUP BY results
      for (const col of this.context.groupBy) {
        fields.push({ name: col, type: 'unknown' });
      }

      for (const agg of this.context.aggregations) {
        const alias = agg.alias || agg.function.toLowerCase();
        fields.push({ name: alias, type: 'numeric' });
      }

      for (const state of this.groups.values()) {
        const row: Record<string, unknown> = { ...state.groupKey };

        for (const agg of this.context.aggregations) {
          const alias = agg.alias || agg.function.toLowerCase();

          switch (agg.function) {
            case 'COUNT':
              row[alias] = state.count;
              break;
            case 'SUM':
              row[alias] = state.sum.get(agg.column) || 0;
              break;
            case 'AVG':
              const sum = state.sum.get(agg.column) || 0;
              row[alias] = state.count > 0 ? sum / state.count : null;
              break;
            case 'MIN':
              row[alias] = state.min.get(agg.column) ?? null;
              break;
            case 'MAX':
              row[alias] = state.max.get(agg.column) ?? null;
              break;
          }
        }

        rows.push(row);
      }
    } else {
      // Simple aggregation
      const row: Record<string, unknown> = {};

      for (const agg of this.context.aggregations) {
        const alias = agg.alias || agg.function.toLowerCase();
        fields.push({ name: alias, type: 'numeric' });

        const groupResult: QueryResult = {
          rows: this.buffer,
          rowCount: this.buffer.length,
          fields: [],
        };

        switch (agg.function) {
          case 'COUNT':
            if (agg.column === '*') {
              row[alias] = this.buffer.length;
            } else {
              row[alias] = this.buffer.filter(r => r[agg.column] !== null && r[agg.column] !== undefined).length;
            }
            break;
          case 'SUM':
            row[alias] = aggregateSum([groupResult], agg.column);
            break;
          case 'AVG':
            const sum = aggregateSum([groupResult], agg.column);
            const count = this.buffer.length;
            row[alias] = count > 0 ? sum / count : null;
            break;
          case 'MIN':
            row[alias] = aggregateMin([groupResult], agg.column);
            break;
          case 'MAX':
            row[alias] = aggregateMax([groupResult], agg.column);
            break;
        }
      }

      rows.push(row);
    }

    return {
      rows,
      rowCount: rows.length,
      fields,
    };
  }
}

/**
 * Create a streaming aggregator
 */
export function createAggregator(context: AggregationContext): StreamingAggregator {
  return new StreamingAggregator(context);
}
