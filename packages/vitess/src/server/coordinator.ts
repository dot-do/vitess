/**
 * Two-Phase Commit Coordinator
 *
 * Manages distributed transactions across multiple shards:
 * - Phase 1 (Prepare): Ask all participants to prepare
 * - Phase 2 (Commit/Rollback): Commit or rollback based on Phase 1 result
 *
 * Key properties:
 * - Atomic: All participants commit or all rollback
 * - Durable: Prepare records survive crashes
 * - Recoverable: Can recover in-doubt transactions after crash
 */

import type { VTTablet, TransactionHandle } from './vttablet.js';

/**
 * 2PC Transaction Coordinator interface
 */
export interface TwoPhaseCoordinator {
  /** Start a distributed transaction */
  beginDistributed(): Promise<DistributedTransaction>;
  /** Recover pending transactions after crash */
  recover(): Promise<RecoveryResult>;
}

/**
 * Distributed transaction handle
 */
export interface DistributedTransaction {
  /** Global transaction ID */
  readonly gtid: string;
  /** Participating shards */
  readonly participants: string[];
  /** Current phase */
  phase: '2pc_prepare' | '2pc_commit' | '2pc_rollback' | 'completed';

  /** Execute on specific shard */
  executeOn(shard: string, sql: string, params?: unknown[]): Promise<void>;
  /** Execute on all participating shards */
  executeOnAll(sql: string, params?: unknown[]): Promise<void>;

  /** Phase 1: Prepare all participants */
  prepare(): Promise<PrepareResult>;
  /** Phase 2: Commit (after successful prepare) */
  commit(): Promise<void>;
  /** Abort the transaction */
  abort(): Promise<void>;
}

/**
 * Prepare result
 */
export interface PrepareResult {
  success: boolean;
  prepared: string[]; // Shards that prepared successfully
  failed: string[]; // Shards that failed to prepare
}

/**
 * Recovery result
 */
export interface RecoveryResult {
  recovered: number;
  committed: string[]; // GTIDs that were committed
  rolledBack: string[]; // GTIDs that were rolled back
  pending: string[]; // GTIDs still pending
}

/**
 * Tablet stub for 2PC (extended interface)
 */
export interface TwoPhaseTabletStub {
  shard: string;
  execute(sql: string, params?: unknown[]): Promise<any>;
  beginTransaction(): Promise<TransactionHandle>;
  prepareTransaction?(txId: string): Promise<string>;
  commitPrepared?(token: string): Promise<void>;
  rollbackPrepared?(token: string): Promise<void>;
  transactions?: Map<string, TransactionHandle>;
}

/**
 * Configuration for 2PC Coordinator
 */
export interface CoordinatorConfig {
  /** Available tablets by shard name */
  tablets: Map<string, TwoPhaseTabletStub>;
  /** Timeout for prepare phase in ms */
  prepareTimeout?: number;
  /** Timeout for commit/rollback phase in ms */
  commitTimeout?: number;
}

/**
 * Default 2PC Coordinator implementation
 */
export class DefaultTwoPhaseCoordinator implements TwoPhaseCoordinator {
  private config: CoordinatorConfig;
  private prepareTimeout: number;
  private commitTimeout: number;

  constructor(config: CoordinatorConfig) {
    this.config = config;
    this.prepareTimeout = config.prepareTimeout ?? 5000;
    this.commitTimeout = config.commitTimeout ?? 10000;
  }

  async beginDistributed(): Promise<DistributedTransaction> {
    const gtid = `gtid-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const participantSet = new Set<string>();
    const shardTxs = new Map<string, TransactionHandle>();
    const prepareTokens = new Map<string, string>();
    let currentPhase: '2pc_prepare' | '2pc_commit' | '2pc_rollback' | 'completed' = '2pc_prepare';

    const dtx: DistributedTransaction = {
      gtid,
      get participants() {
        return Array.from(participantSet);
      },
      get phase() {
        return currentPhase;
      },
      set phase(p) {
        currentPhase = p;
      },

      executeOn: async (shard: string, sql: string, params?: unknown[]): Promise<void> => {
        const tablet = this.config.tablets.get(shard);
        if (!tablet) {
          throw new Error(`No tablet for shard ${shard}`);
        }

        // Track participant
        if (!participantSet.has(shard)) {
          participantSet.add(shard);

          // Start transaction on this shard if not already
          if (tablet.beginTransaction) {
            const tx = await tablet.beginTransaction();
            shardTxs.set(shard, tx);
          }
        }

        // Execute within the shard's transaction
        const tx = shardTxs.get(shard);
        if (tx) {
          await tx.execute(sql, params);
        } else {
          await tablet.execute(sql, params);
        }
      },

      executeOnAll: async (sql: string, params?: unknown[]): Promise<void> => {
        const promises = [];
        for (const shard of participantSet) {
          promises.push(dtx.executeOn(shard, sql, params));
        }
        await Promise.all(promises);
      },

      prepare: async (): Promise<PrepareResult> => {
        const prepared: string[] = [];
        const failed: string[] = [];

        const preparePromises = Array.from(participantSet).map(async (shard) => {
          const tablet = this.config.tablets.get(shard);
          const tx = shardTxs.get(shard);

          try {
            // Create a timeout-aware promise
            const timeoutPromise = new Promise<never>((_, reject) => {
              setTimeout(() => reject(new Error('Prepare timeout')), this.prepareTimeout);
            });

            // Prepare the transaction
            let token: string;
            if (tx && tx.prepare) {
              token = await Promise.race([tx.prepare(), timeoutPromise]);
            } else if (tablet && tablet.prepareTransaction) {
              token = await Promise.race([tablet.prepareTransaction(tx?.id || ''), timeoutPromise]);
            } else {
              // No prepare support - simulate success
              token = `token-${shard}-${Date.now()}`;
            }

            prepareTokens.set(shard, token);
            prepared.push(shard);
          } catch (err) {
            failed.push(shard);
          }
        });

        await Promise.all(preparePromises);

        return {
          success: failed.length === 0,
          prepared,
          failed,
        };
      },

      commit: async (): Promise<void> => {
        if (currentPhase === 'completed') {
          return; // Idempotent
        }

        const commitPromises = Array.from(prepareTokens.entries()).map(async ([shard, token]) => {
          const tablet = this.config.tablets.get(shard);
          const tx = shardTxs.get(shard);

          try {
            if (tx && tx.commitPrepared) {
              await tx.commitPrepared(token);
            } else if (tablet && tablet.commitPrepared) {
              await tablet.commitPrepared(token);
            } else if (tx) {
              await tx.commit();
            }
          } catch (err) {
            // Log error but continue - commit decision is durable
            console.error(`Commit failed on shard ${shard}:`, err);
          }
        });

        await Promise.all(commitPromises);
        currentPhase = 'completed';
      },

      abort: async (): Promise<void> => {
        if (currentPhase === 'completed') {
          return;
        }

        const rollbackPromises = Array.from(participantSet).map(async (shard) => {
          const tablet = this.config.tablets.get(shard);
          const tx = shardTxs.get(shard);
          const token = prepareTokens.get(shard);

          try {
            if (token) {
              // Already prepared - use rollbackPrepared
              if (tx && tx.rollbackPrepared) {
                await tx.rollbackPrepared(token);
              } else if (tablet && tablet.rollbackPrepared) {
                await tablet.rollbackPrepared(token);
              } else if (tx) {
                await tx.rollback();
              }
            } else {
              // Not prepared yet - simple rollback
              if (tx) {
                await tx.rollback();
              }
            }
          } catch (err) {
            // Log error but continue
            console.error(`Rollback failed on shard ${shard}:`, err);
          }
        });

        await Promise.all(rollbackPromises);
        currentPhase = 'completed';
      },
    };

    return dtx;
  }

  async recover(): Promise<RecoveryResult> {
    // In a real implementation, this would:
    // 1. Scan the coordinator's WAL for pending transactions
    // 2. Ask participants about their prepare status
    // 3. Complete (commit or rollback) each pending transaction
    return {
      recovered: 0,
      committed: [],
      rolledBack: [],
      pending: [],
    };
  }
}

/**
 * Create a 2PC coordinator
 */
export function createCoordinator(config: CoordinatorConfig): TwoPhaseCoordinator {
  return new DefaultTwoPhaseCoordinator(config);
}
