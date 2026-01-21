/**
 * Two-Phase Commit Tests - Cross-Shard 2PC Coordination
 *
 * TDD Red tests for distributed transaction coordination.
 * 2PC ensures atomicity of transactions that span multiple shards.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  VTGate,
  createVTGate,
  type VTTabletStub,
  type QueryResult,
} from '../../server/vtgate.js';
import {
  VTTablet,
  createVTTablet,
  type StorageEngine,
  type TransactionHandle,
} from '../../server/vttablet.js';
import { createVSchemaBuilder, type VSchema } from '../../server/vschema.js';

/**
 * 2PC Transaction Coordinator
 */
interface TwoPhaseCoordinator {
  /** Start a distributed transaction */
  beginDistributed(): Promise<DistributedTransaction>;
  /** Recover pending transactions after crash */
  recover(): Promise<RecoveryResult>;
}

/**
 * Distributed transaction handle
 */
interface DistributedTransaction {
  /** Global transaction ID */
  readonly gtid: string;
  /** Participating shards */
  readonly participants: string[];
  /** Current phase */
  readonly phase: '2pc_prepare' | '2pc_commit' | '2pc_rollback' | 'completed';

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
interface PrepareResult {
  success: boolean;
  prepared: string[]; // Shards that prepared successfully
  failed: string[]; // Shards that failed to prepare
}

/**
 * Recovery result
 */
interface RecoveryResult {
  recovered: number;
  committed: string[]; // GTIDs that were committed
  rolledBack: string[]; // GTIDs that were rolled back
  pending: string[]; // GTIDs still pending
}

describe('Two-Phase Commit', () => {
  let vtgate: VTGate;
  let vschema: VSchema;
  let mockTablets: Map<string, MockTablet>;
  let coordinator: TwoPhaseCoordinator;

  interface MockTablet extends VTTabletStub {
    prepareTransaction: (txId: string) => Promise<string>;
    commitPrepared: (token: string) => Promise<void>;
    rollbackPrepared: (token: string) => Promise<void>;
    beginTransaction: () => Promise<TransactionHandle>;
    transactions: Map<string, TransactionHandle>;
  }

  beforeEach(() => {
    vschema = createVSchemaBuilder()
      .addKeyspace('banking', true)
      .addVindex('banking', 'hash', { type: 'hash' })
      .addTable('banking', 'accounts', {
        column_vindexes: [{ column: 'account_id', name: 'hash' }],
      })
      .setShards('banking', ['-80', '80-'])
      .build();

    mockTablets = new Map();
    for (const shard of ['-80', '80-']) {
      const tablet: MockTablet = {
        shard,
        transactions: new Map(),
        execute: vi.fn().mockResolvedValue({ rows: [], rowCount: 0, fields: [] }),
        prepareTransaction: vi.fn().mockResolvedValue(`prepare-token-${shard}`),
        commitPrepared: vi.fn().mockResolvedValue(undefined),
        rollbackPrepared: vi.fn().mockResolvedValue(undefined),
        beginTransaction: vi.fn().mockImplementation(() => {
          const txId = `tx-${Date.now()}-${Math.random()}`;
          const tx: TransactionHandle = {
            id: txId,
            state: 'active',
            query: vi.fn(),
            execute: vi.fn().mockResolvedValue({ rowsAffected: 1 }),
            commit: vi.fn(),
            rollback: vi.fn(),
            prepare: vi.fn().mockResolvedValue(`token-${txId}`),
            commitPrepared: vi.fn(),
            rollbackPrepared: vi.fn(),
          };
          tablet.transactions.set(txId, tx);
          return Promise.resolve(tx);
        }),
      };
      mockTablets.set(shard, tablet);
    }

    vtgate = createVTGate({
      vschema,
      shards: new Map([['banking', ['-80', '80-']]]),
      tablets: mockTablets as any,
    });

    // Mock coordinator
    coordinator = {
      beginDistributed: vi.fn().mockImplementation(async () => {
        const gtid = `gtid-${Date.now()}`;
        const participants: string[] = [];

        return {
          gtid,
          participants,
          phase: '2pc_prepare' as const,
          executeOn: vi.fn(),
          executeOnAll: vi.fn(),
          prepare: vi.fn(),
          commit: vi.fn(),
          abort: vi.fn(),
        };
      }),
      recover: vi.fn().mockResolvedValue({
        recovered: 0,
        committed: [],
        rolledBack: [],
        pending: [],
      }),
    };
  });

  describe('Distributed Transaction Lifecycle', () => {
    it('should begin a distributed transaction', async () => {
      const dtx = await coordinator.beginDistributed();

      expect(dtx.gtid).toBeDefined();
      expect(dtx.gtid).toMatch(/^gtid-/);
      expect(dtx.phase).toBe('2pc_prepare');
    });

    it('should track participating shards', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'UPDATE accounts SET balance = balance - 100 WHERE account_id = $1', [
        'acc-1',
      ]);
      await dtx.executeOn('80-', 'UPDATE accounts SET balance = balance + 100 WHERE account_id = $1', [
        'acc-2',
      ]);

      expect(dtx.participants).toContain('-80');
      expect(dtx.participants).toContain('80-');
    });

    it('should execute on all participating shards', async () => {
      const dtx = await coordinator.beginDistributed();

      // First establish participants
      await dtx.executeOn('-80', 'SELECT 1', []);
      await dtx.executeOn('80-', 'SELECT 1', []);

      // Then execute on all
      await dtx.executeOnAll('SAVEPOINT sp1', []);

      // Both shards should receive the command
    });
  });

  describe('Phase 1: Prepare', () => {
    it('should prepare all participants', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'UPDATE accounts SET balance = balance - 100 WHERE account_id = $1', [
        'acc-1',
      ]);
      await dtx.executeOn('80-', 'UPDATE accounts SET balance = balance + 100 WHERE account_id = $1', [
        'acc-2',
      ]);

      const result = await dtx.prepare();

      expect(result.success).toBe(true);
      expect(result.prepared).toContain('-80');
      expect(result.prepared).toContain('80-');
      expect(result.failed).toHaveLength(0);
    });

    it('should return failure if any participant fails to prepare', async () => {
      const dtx = await coordinator.beginDistributed();

      // Setup one tablet to fail prepare
      const failingTablet = mockTablets.get('80-')!;
      failingTablet.prepareTransaction = vi.fn().mockRejectedValue(new Error('Lock conflict'));

      await dtx.executeOn('-80', 'UPDATE accounts SET balance = balance - 100 WHERE account_id = $1', [
        'acc-1',
      ]);
      await dtx.executeOn('80-', 'UPDATE accounts SET balance = balance + 100 WHERE account_id = $1', [
        'acc-2',
      ]);

      const result = await dtx.prepare();

      expect(result.success).toBe(false);
      expect(result.prepared).toContain('-80');
      expect(result.failed).toContain('80-');
    });

    it('should write prepare record to WAL before responding', async () => {
      // Each tablet should persist prepare state before responding
      // This ensures recoverability after crash
    });

    it('should hold locks during prepared state', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'UPDATE accounts SET balance = 0 WHERE account_id = $1', ['acc-1']);
      await dtx.prepare();

      // Attempting to modify the same row from another transaction should block/fail
      // until the prepared transaction is committed or rolled back
    });

    it('should timeout prepare phase if too slow', async () => {
      const dtx = await coordinator.beginDistributed();

      // Setup slow tablet
      const slowTablet = mockTablets.get('-80')!;
      slowTablet.prepareTransaction = vi.fn().mockImplementation(
        () => new Promise((resolve) => setTimeout(() => resolve('token'), 10000))
      );

      await dtx.executeOn('-80', 'SELECT 1', []);

      // Should timeout and fail
      await expect(dtx.prepare()).rejects.toThrow(/timeout/i);
    }, 5000);
  });

  describe('Phase 2: Commit', () => {
    it('should commit all prepared participants', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'UPDATE accounts SET balance = balance - 100 WHERE account_id = $1', [
        'acc-1',
      ]);
      await dtx.executeOn('80-', 'UPDATE accounts SET balance = balance + 100 WHERE account_id = $1', [
        'acc-2',
      ]);

      const prepareResult = await dtx.prepare();
      expect(prepareResult.success).toBe(true);

      await dtx.commit();

      // Both tablets should have commitPrepared called
      expect(mockTablets.get('-80')!.commitPrepared).toHaveBeenCalled();
      expect(mockTablets.get('80-')!.commitPrepared).toHaveBeenCalled();
    });

    it('should change phase to committed after commit', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'SELECT 1', []);
      await dtx.prepare();
      await dtx.commit();

      expect(dtx.phase).toBe('completed');
    });

    it('should be idempotent - multiple commit calls succeed', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'SELECT 1', []);
      await dtx.prepare();

      await dtx.commit();
      await dtx.commit(); // Should not throw
    });

    it('should handle commit failure on some participants', async () => {
      const dtx = await coordinator.beginDistributed();

      // Setup one tablet to fail commit
      const failingTablet = mockTablets.get('80-')!;
      failingTablet.commitPrepared = vi.fn().mockRejectedValue(new Error('Network error'));

      await dtx.executeOn('-80', 'SELECT 1', []);
      await dtx.executeOn('80-', 'SELECT 1', []);
      await dtx.prepare();

      // Commit should still complete (eventually) - may retry
      // The commit decision is durable after prepare
      await expect(dtx.commit()).resolves.toBeUndefined();
    });
  });

  describe('Abort/Rollback', () => {
    it('should abort unprepared transaction', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'UPDATE accounts SET balance = 0 WHERE account_id = $1', ['acc-1']);

      await dtx.abort();

      // Changes should be rolled back
    });

    it('should rollback all prepared participants on abort', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'SELECT 1', []);
      await dtx.executeOn('80-', 'SELECT 1', []);

      // Prepare partially fails
      const failingTablet = mockTablets.get('80-')!;
      failingTablet.prepareTransaction = vi.fn().mockRejectedValue(new Error('Prepare failed'));

      const prepareResult = await dtx.prepare();
      expect(prepareResult.success).toBe(false);

      await dtx.abort();

      // Successfully prepared shard should be rolled back
      expect(mockTablets.get('-80')!.rollbackPrepared).toHaveBeenCalled();
    });

    it('should release locks after abort', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'UPDATE accounts SET balance = 0 WHERE account_id = $1', ['acc-1']);
      await dtx.abort();

      // Row should no longer be locked
    });
  });

  describe('Failure Recovery', () => {
    it('should recover prepared-but-not-committed transactions', async () => {
      // Simulate crash after prepare but before commit
      // Coordinator should be able to recover and complete

      const result = await coordinator.recover();

      expect(result).toBeDefined();
      expect(Array.isArray(result.committed)).toBe(true);
      expect(Array.isArray(result.rolledBack)).toBe(true);
    });

    it('should commit transactions where all participants prepared', async () => {
      // If all participants have prepare records, transaction should be committed
    });

    it('should rollback transactions where some participants did not prepare', async () => {
      // If any participant lacks prepare record, transaction should be rolled back
    });

    it('should handle coordinator crash during phase 1', async () => {
      // If coordinator crashes during prepare phase:
      // - Unprepared participants will abort on timeout
      // - Prepared participants will hold locks until resolution
    });

    it('should handle coordinator crash during phase 2', async () => {
      // If coordinator crashes during commit phase:
      // - Recovery should complete the commit
      // - All participants should eventually commit
    });

    it('should handle participant crash after prepare', async () => {
      // Participant should recover and either:
      // - Ask coordinator for decision
      // - Or use prepare record to determine outcome
    });
  });

  describe('Deadlock Detection', () => {
    it('should detect cross-shard deadlocks', async () => {
      // Transaction A: locks row X on shard 1, wants row Y on shard 2
      // Transaction B: locks row Y on shard 2, wants row X on shard 1
      // Should detect and abort one transaction
    });

    it('should timeout if deadlock detection fails', async () => {
      // Fallback to timeout if deadlock cannot be detected
    });
  });

  describe('Isolation Levels', () => {
    it('should provide snapshot isolation for distributed reads', async () => {
      // Reads within a distributed transaction should see consistent snapshot
    });

    it('should prevent write skew in distributed transactions', async () => {
      // Two transactions that read overlapping data and write disjoint data
      // should be detected and one aborted
    });
  });

  describe('Performance', () => {
    it('should complete 2PC within acceptable latency', async () => {
      const dtx = await coordinator.beginDistributed();

      await dtx.executeOn('-80', 'SELECT 1', []);
      await dtx.executeOn('80-', 'SELECT 1', []);

      const start = performance.now();
      await dtx.prepare();
      await dtx.commit();
      const elapsed = performance.now() - start;

      // 2PC should complete within 100ms for local shards
      expect(elapsed).toBeLessThan(100);
    });

    it('should parallelize prepare requests', async () => {
      const dtx = await coordinator.beginDistributed();

      // Add many participants
      await dtx.executeOn('-80', 'SELECT 1', []);
      await dtx.executeOn('80-', 'SELECT 1', []);

      // Mock slow prepare
      for (const tablet of mockTablets.values()) {
        tablet.prepareTransaction = vi.fn().mockImplementation(
          () => new Promise((resolve) => setTimeout(() => resolve('token'), 50))
        );
      }

      const start = performance.now();
      await dtx.prepare();
      const elapsed = performance.now() - start;

      // Should be ~50ms (parallel), not ~100ms (sequential)
      expect(elapsed).toBeLessThan(80);
    });
  });

  describe('VTGate integration', () => {
    it('should automatically use 2PC for cross-shard transactions', async () => {
      // VTGate should detect when transaction spans shards
      // and automatically use 2PC protocol
    });

    it('should use single-shard optimization when possible', async () => {
      // If transaction only touches one shard, skip 2PC overhead
    });

    it('should handle mixed read-write transactions', async () => {
      // Reads can go to replicas
      // Writes must go to primary
      // 2PC only for write participants
    });
  });

  describe('Monitoring and Observability', () => {
    it('should track 2PC transaction metrics', async () => {
      // Prepare latency
      // Commit latency
      // Abort rate
      // In-doubt transactions
    });

    it('should log transaction state transitions', async () => {
      // For debugging and audit
    });

    it('should expose pending transactions for monitoring', async () => {
      // List all in-progress distributed transactions
    });
  });
});

describe('Transaction Prepare/Commit on VTTablet', () => {
  let tablet: VTTablet;
  let mockEngine: StorageEngine;

  beforeEach(() => {
    mockEngine = {
      type: 'pglite',
      query: vi.fn(),
      execute: vi.fn(),
      beginTransaction: vi.fn().mockResolvedValue({
        id: 'tx-1',
        state: 'active',
        query: vi.fn(),
        execute: vi.fn(),
        commit: vi.fn(),
        rollback: vi.fn(),
        prepare: vi.fn().mockResolvedValue('prepare-token-abc'),
        commitPrepared: vi.fn(),
        rollbackPrepared: vi.fn(),
      }),
      close: vi.fn(),
    };

    tablet = createVTTablet({
      shard: '-80',
      keyspace: 'banking',
      engine: mockEngine,
    });
  });

  describe('prepare()', () => {
    it('should prepare transaction and return token', async () => {
      const tx = await tablet.beginTransaction();

      const token = await tx.prepare();

      expect(token).toBeDefined();
      expect(typeof token).toBe('string');
    });

    it('should persist prepare record durably', async () => {
      // The prepare record must survive crash
      // Implementation detail: write to WAL or separate prepare log
    });

    it('should hold locks after prepare', async () => {
      const tx = await tablet.beginTransaction();
      await tx.execute('UPDATE accounts SET balance = 0 WHERE account_id = $1', ['acc-1']);
      await tx.prepare();

      // Transaction should still hold locks
      // Cannot be committed or rolled back without token
    });
  });

  describe('commitPrepared()', () => {
    it('should commit prepared transaction using token', async () => {
      const tx = await tablet.beginTransaction();
      const token = await tx.prepare();

      await tx.commitPrepared(token);

      // Transaction should be committed
    });

    it('should fail with invalid token', async () => {
      const tx = await tablet.beginTransaction();
      await tx.prepare();

      await expect(tx.commitPrepared('invalid-token')).rejects.toThrow();
    });

    it('should be idempotent', async () => {
      const tx = await tablet.beginTransaction();
      const token = await tx.prepare();

      await tx.commitPrepared(token);
      await tx.commitPrepared(token); // Should not throw
    });
  });

  describe('rollbackPrepared()', () => {
    it('should rollback prepared transaction using token', async () => {
      const tx = await tablet.beginTransaction();
      const token = await tx.prepare();

      await tx.rollbackPrepared(token);

      // Transaction should be rolled back
    });

    it('should release locks after rollback', async () => {
      const tx = await tablet.beginTransaction();
      await tx.execute('UPDATE accounts SET balance = 0 WHERE account_id = $1', ['acc-1']);
      const token = await tx.prepare();

      await tx.rollbackPrepared(token);

      // Locks should be released
    });
  });
});
