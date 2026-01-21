/**
 * VTGate & VTTablet Server Runtime
 *
 * Server-side components for Vitess.do:
 * - VTGate: Query routing, scatter-gather, aggregation
 * - VTTablet: Storage engine abstraction (PGlite/Turso)
 * - VSchema: Schema-based routing configuration
 * - Vindexes: Sharding key computation
 */

export * from './vtgate.js';
export * from './vttablet.js';
export * from './vschema.js';
export * from './vindexes.js';
export * from './aggregation.js';
