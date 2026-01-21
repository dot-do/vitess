/**
 * VTTablet Query Tests - VTTablet (DO) Query Handling
 *
 * TDD Red tests for VTTablet query execution.
 * VTTablet is the shard-level query executor that runs on Durable Objects.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  VTTablet,
  createVTTablet,
  PGliteEngine,
  TursoEngine,
  type VTTabletConfig,
  type StorageEngine,
  type QueryResult,
  type ExecuteResult,
} from '../../server/vttablet.js';

describe('VTTablet Query Handling', () => {
  let tablet: VTTablet;
  let mockEngine: StorageEngine;

  beforeEach(() => {
    // Create mock storage engine
    mockEngine = {
      type: 'pglite',
      query: vi.fn().mockResolvedValue({
        rows: [{ id: 1, name: 'Alice' }],
        rowCount: 1,
        fields: [
          { name: 'id', type: 'int' },
          { name: 'name', type: 'text' },
        ],
      }),
      execute: vi.fn().mockResolvedValue({
        rowsAffected: 1,
        lastInsertId: 1,
      }),
      beginTransaction: vi.fn(),
      close: vi.fn(),
    };

    tablet = createVTTablet({
      shard: '-80',
      keyspace: 'commerce',
      engine: mockEngine,
    });
  });

  describe('Properties', () => {
    it('should expose shard identifier', () => {
      expect(tablet.shard).toBe('-80');
    });

    it('should expose keyspace name', () => {
      expect(tablet.keyspace).toBe('commerce');
    });

    it('should expose storage engine type', () => {
      expect(tablet.engineType).toBe('pglite');
    });
  });

  describe('query()', () => {
    it('should execute SELECT query and return results', async () => {
      const result = await tablet.query('SELECT * FROM users WHERE id = $1', [1]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual({ id: 1, name: 'Alice' });
      expect(mockEngine.query).toHaveBeenCalledWith('SELECT * FROM users WHERE id = $1', [1]);
    });

    it('should return rowCount', async () => {
      const result = await tablet.query('SELECT * FROM users');

      expect(result.rowCount).toBe(1);
    });

    it('should return field metadata', async () => {
      const result = await tablet.query('SELECT id, name FROM users');

      expect(result.fields).toHaveLength(2);
      expect(result.fields[0].name).toBe('id');
      expect(result.fields[1].name).toBe('name');
    });

    it('should support typed results', async () => {
      interface User {
        id: number;
        name: string;
      }

      const result = await tablet.query<User>('SELECT * FROM users WHERE id = $1', [1]);

      expect(result.rows[0].id).toBe(1);
      expect(result.rows[0].name).toBe('Alice');
    });

    it('should handle empty results', async () => {
      mockEngine.query = vi.fn().mockResolvedValue({
        rows: [],
        rowCount: 0,
        fields: [
          { name: 'id', type: 'int' },
          { name: 'name', type: 'text' },
        ],
      });

      const result = await tablet.query('SELECT * FROM users WHERE id = $1', [999]);

      expect(result.rows).toHaveLength(0);
      expect(result.rowCount).toBe(0);
    });

    it('should handle NULL values', async () => {
      mockEngine.query = vi.fn().mockResolvedValue({
        rows: [{ id: 1, email: null }],
        rowCount: 1,
        fields: [
          { name: 'id', type: 'int' },
          { name: 'email', type: 'text', nullable: true },
        ],
      });

      const result = await tablet.query('SELECT id, email FROM users WHERE id = $1', [1]);

      expect(result.rows[0].email).toBeNull();
    });

    it('should handle various data types', async () => {
      mockEngine.query = vi.fn().mockResolvedValue({
        rows: [
          {
            int_col: 42,
            bigint_col: BigInt('9223372036854775807'),
            float_col: 3.14159,
            bool_col: true,
            text_col: 'hello',
            json_col: { key: 'value' },
            date_col: new Date('2024-01-15'),
            bytes_col: new Uint8Array([1, 2, 3]),
          },
        ],
        rowCount: 1,
        fields: [],
      });

      const result = await tablet.query('SELECT * FROM mixed_types');

      expect(result.rows[0].int_col).toBe(42);
      expect(result.rows[0].bigint_col).toBe(BigInt('9223372036854775807'));
      expect(result.rows[0].float_col).toBeCloseTo(3.14159);
      expect(result.rows[0].bool_col).toBe(true);
      expect(result.rows[0].json_col).toEqual({ key: 'value' });
    });

    it('should propagate query errors', async () => {
      mockEngine.query = vi.fn().mockRejectedValue(new Error('Syntax error'));

      await expect(tablet.query('SELECT * FORM users')).rejects.toThrow('Syntax error');
    });

    it('should handle prepared statement parameters', async () => {
      await tablet.query('SELECT * FROM users WHERE id = $1 AND status = $2', [1, 'active']);

      expect(mockEngine.query).toHaveBeenCalledWith(
        'SELECT * FROM users WHERE id = $1 AND status = $2',
        [1, 'active']
      );
    });

    it('should handle queries without parameters', async () => {
      await tablet.query('SELECT COUNT(*) FROM users');

      expect(mockEngine.query).toHaveBeenCalledWith('SELECT COUNT(*) FROM users', undefined);
    });
  });

  describe('execute()', () => {
    it('should execute INSERT and return affected rows', async () => {
      const result = await tablet.execute(
        'INSERT INTO users (name, email) VALUES ($1, $2)',
        ['Bob', 'bob@example.com']
      );

      expect(result.rowsAffected).toBe(1);
    });

    it('should return lastInsertId for INSERT', async () => {
      mockEngine.execute = vi.fn().mockResolvedValue({
        rowsAffected: 1,
        lastInsertId: 42,
      });

      const result = await tablet.execute('INSERT INTO users (name) VALUES ($1)', ['Charlie']);

      expect(result.lastInsertId).toBe(42);
    });

    it('should execute UPDATE and return affected rows', async () => {
      mockEngine.execute = vi.fn().mockResolvedValue({
        rowsAffected: 5,
      });

      const result = await tablet.execute(
        'UPDATE users SET status = $1 WHERE last_login < $2',
        ['inactive', '2024-01-01']
      );

      expect(result.rowsAffected).toBe(5);
    });

    it('should execute DELETE and return affected rows', async () => {
      mockEngine.execute = vi.fn().mockResolvedValue({
        rowsAffected: 3,
      });

      const result = await tablet.execute('DELETE FROM users WHERE status = $1', ['deleted']);

      expect(result.rowsAffected).toBe(3);
    });

    it('should handle zero affected rows', async () => {
      mockEngine.execute = vi.fn().mockResolvedValue({
        rowsAffected: 0,
      });

      const result = await tablet.execute('DELETE FROM users WHERE id = $1', [999]);

      expect(result.rowsAffected).toBe(0);
    });

    it('should propagate execute errors', async () => {
      mockEngine.execute = vi.fn().mockRejectedValue(new Error('Constraint violation'));

      await expect(tablet.execute('INSERT INTO users (id) VALUES ($1)', [1])).rejects.toThrow(
        'Constraint violation'
      );
    });

    it('should handle batch inserts', async () => {
      mockEngine.execute = vi.fn().mockResolvedValue({
        rowsAffected: 3,
      });

      const result = await tablet.execute(
        'INSERT INTO users (name) VALUES ($1), ($2), ($3)',
        ['Alice', 'Bob', 'Charlie']
      );

      expect(result.rowsAffected).toBe(3);
    });
  });

  describe('DDL operations', () => {
    it('should execute CREATE TABLE', async () => {
      mockEngine.execute = vi.fn().mockResolvedValue({ rowsAffected: 0 });

      const result = await tablet.execute(`
        CREATE TABLE IF NOT EXISTS users (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT UNIQUE
        )
      `);

      expect(result).toBeDefined();
    });

    it('should execute CREATE INDEX', async () => {
      mockEngine.execute = vi.fn().mockResolvedValue({ rowsAffected: 0 });

      const result = await tablet.execute('CREATE INDEX idx_users_email ON users(email)');

      expect(result).toBeDefined();
    });

    it('should execute ALTER TABLE', async () => {
      mockEngine.execute = vi.fn().mockResolvedValue({ rowsAffected: 0 });

      const result = await tablet.execute('ALTER TABLE users ADD COLUMN phone TEXT');

      expect(result).toBeDefined();
    });
  });

  describe('close()', () => {
    it('should close the storage engine', async () => {
      await tablet.close();

      expect(mockEngine.close).toHaveBeenCalled();
    });

    it('should rollback active transactions before closing', async () => {
      const mockTx = {
        id: 'tx-1',
        state: 'active' as const,
        query: vi.fn(),
        execute: vi.fn(),
        commit: vi.fn(),
        rollback: vi.fn().mockResolvedValue(undefined),
        prepare: vi.fn(),
        commitPrepared: vi.fn(),
        rollbackPrepared: vi.fn(),
      };

      mockEngine.beginTransaction = vi.fn().mockResolvedValue(mockTx);
      await tablet.beginTransaction();

      await tablet.close();

      // Should rollback active transactions
      expect(mockTx.rollback).toHaveBeenCalled();
    });
  });
});

describe('PGliteEngine', () => {
  let engine: PGliteEngine;
  let mockDb: any;

  beforeEach(() => {
    mockDb = {
      query: vi.fn(),
      exec: vi.fn(),
      close: vi.fn(),
    };
    engine = new PGliteEngine(mockDb);
  });

  describe('properties', () => {
    it('should have type pglite', () => {
      expect(engine.type).toBe('pglite');
    });
  });

  describe('query()', () => {
    it('should execute query using PGlite API', async () => {
      mockDb.query = vi.fn().mockResolvedValue({
        rows: [{ id: 1 }],
        fields: [{ name: 'id', dataTypeID: 23 }],
      });

      const result = await engine.query('SELECT * FROM users');

      expect(mockDb.query).toHaveBeenCalled();
      expect(result.rows).toHaveLength(1);
    });

    it('should map PGlite field types to standard types', async () => {
      mockDb.query = vi.fn().mockResolvedValue({
        rows: [],
        fields: [
          { name: 'id', dataTypeID: 23 }, // int4
          { name: 'name', dataTypeID: 25 }, // text
          { name: 'amount', dataTypeID: 1700 }, // numeric
        ],
      });

      const result = await engine.query('SELECT id, name, amount FROM users');

      expect(result.fields[0].type).toBe('int');
      expect(result.fields[1].type).toBe('text');
      expect(result.fields[2].type).toBe('numeric');
    });

    it('should handle parameterized queries', async () => {
      mockDb.query = vi.fn().mockResolvedValue({ rows: [], fields: [] });

      await engine.query('SELECT * FROM users WHERE id = $1', [123]);

      expect(mockDb.query).toHaveBeenCalledWith(
        'SELECT * FROM users WHERE id = $1',
        [123]
      );
    });
  });

  describe('execute()', () => {
    it('should execute write statements', async () => {
      mockDb.exec = vi.fn().mockResolvedValue({
        affectedRows: 1,
        insertId: 42,
      });

      const result = await engine.execute('INSERT INTO users (name) VALUES ($1)', ['Alice']);

      expect(result.rowsAffected).toBeDefined();
    });
  });

  describe('beginTransaction()', () => {
    it('should start a transaction', async () => {
      mockDb.exec = vi.fn().mockResolvedValue({});

      const tx = await engine.beginTransaction();

      expect(tx).toBeDefined();
      expect(tx.id).toBeDefined();
      expect(tx.state).toBe('active');
    });
  });

  describe('close()', () => {
    it('should close the database', async () => {
      await engine.close();

      expect(mockDb.close).toHaveBeenCalled();
    });
  });
});

describe('TursoEngine', () => {
  let engine: TursoEngine;
  let mockDb: any;

  beforeEach(() => {
    mockDb = {
      execute: vi.fn(),
      batch: vi.fn(),
      close: vi.fn(),
    };
    engine = new TursoEngine(mockDb);
  });

  describe('properties', () => {
    it('should have type turso', () => {
      expect(engine.type).toBe('turso');
    });
  });

  describe('query()', () => {
    it('should execute query using Turso API', async () => {
      mockDb.execute = vi.fn().mockResolvedValue({
        rows: [{ id: 1, name: 'Alice' }],
        columns: ['id', 'name'],
        columnTypes: ['INTEGER', 'TEXT'],
      });

      const result = await engine.query('SELECT * FROM users');

      expect(mockDb.execute).toHaveBeenCalled();
      expect(result.rows).toHaveLength(1);
    });

    it('should handle positional parameters (SQLite style)', async () => {
      mockDb.execute = vi.fn().mockResolvedValue({
        rows: [],
        columns: [],
        columnTypes: [],
      });

      await engine.query('SELECT * FROM users WHERE id = ?', [123]);

      expect(mockDb.execute).toHaveBeenCalledWith({
        sql: 'SELECT * FROM users WHERE id = ?',
        args: [123],
      });
    });

    it('should convert $N parameters to ?', async () => {
      mockDb.execute = vi.fn().mockResolvedValue({
        rows: [],
        columns: [],
        columnTypes: [],
      });

      await engine.query('SELECT * FROM users WHERE id = $1 AND status = $2', [1, 'active']);

      // Should convert Postgres-style params to SQLite style
      expect(mockDb.execute).toHaveBeenCalledWith({
        sql: 'SELECT * FROM users WHERE id = ? AND status = ?',
        args: [1, 'active'],
      });
    });
  });

  describe('execute()', () => {
    it('should execute write statements', async () => {
      mockDb.execute = vi.fn().mockResolvedValue({
        rowsAffected: 1,
        lastInsertRowid: BigInt(42),
      });

      const result = await engine.execute('INSERT INTO users (name) VALUES (?)', ['Alice']);

      expect(result.rowsAffected).toBe(1);
      expect(result.lastInsertId).toBe('42');
    });
  });

  describe('beginTransaction()', () => {
    it('should start a transaction using batch', async () => {
      const tx = await engine.beginTransaction();

      expect(tx).toBeDefined();
      expect(tx.state).toBe('active');
    });
  });

  describe('close()', () => {
    it('should close the database', async () => {
      await engine.close();

      expect(mockDb.close).toHaveBeenCalled();
    });
  });
});

describe('VTTablet with different engines', () => {
  it('should work with PGlite engine', async () => {
    const mockPGlite = {
      query: vi.fn().mockResolvedValue({ rows: [{ id: 1 }], fields: [] }),
      exec: vi.fn(),
      close: vi.fn(),
    };
    const engine = new PGliteEngine(mockPGlite);
    const tablet = createVTTablet({
      shard: '-80',
      keyspace: 'test',
      engine,
    });

    expect(tablet.engineType).toBe('pglite');
  });

  it('should work with Turso engine', async () => {
    const mockTurso = {
      execute: vi.fn().mockResolvedValue({ rows: [], columns: [], columnTypes: [] }),
      batch: vi.fn(),
      close: vi.fn(),
    };
    const engine = new TursoEngine(mockTurso);
    const tablet = createVTTablet({
      shard: '80-',
      keyspace: 'test',
      engine,
    });

    expect(tablet.engineType).toBe('turso');
  });
});
