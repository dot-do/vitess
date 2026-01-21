/**
 * TursoAdapter Parameter Placeholder Translation Tests
 *
 * Tests for translating PostgreSQL-style $1, $2, ... placeholders to SQLite ? placeholders.
 * Issue: vitess-1bb.11
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { TursoAdapter, translateParams } from '../index.js';

describe('Parameter Placeholder Translation ($1,$2 -> ?,?)', () => {
  let adapter: TursoAdapter;

  beforeEach(async () => {
    adapter = new TursoAdapter({ url: ':memory:' });
    await adapter.connect();

    // Create test table
    await adapter.execute(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER
      )
    `);
  });

  afterEach(async () => {
    await adapter.close();
  });

  describe('translateParams function', () => {
    describe('basic placeholder translation', () => {
      it('should translate $1 to ?', () => {
        const result = translateParams('SELECT * FROM users WHERE id = $1');
        expect(result.sql).toBe('SELECT * FROM users WHERE id = ?');
      });

      it('should translate multiple sequential placeholders $1, $2, $3', () => {
        const result = translateParams('INSERT INTO users (name, email, age) VALUES ($1, $2, $3)');
        expect(result.sql).toBe('INSERT INTO users (name, email, age) VALUES (?, ?, ?)');
      });

      it('should translate $10 and higher numbered placeholders', () => {
        const sql =
          'SELECT * FROM t WHERE a=$1 AND b=$2 AND c=$3 AND d=$4 AND e=$5 AND f=$6 AND g=$7 AND h=$8 AND i=$9 AND j=$10';
        const result = translateParams(sql);
        expect(result.sql).toBe(
          'SELECT * FROM t WHERE a=? AND b=? AND c=? AND d=? AND e=? AND f=? AND g=? AND h=? AND i=? AND j=?'
        );
      });

      it('should handle placeholders with spaces around them', () => {
        const result = translateParams('SELECT * FROM users WHERE id = $1 AND name = $2');
        expect(result.sql).toBe('SELECT * FROM users WHERE id = ? AND name = ?');
      });
    });

    describe('parameter reordering', () => {
      it('should reorder params when placeholders are out of order', () => {
        const sql = 'INSERT INTO t (b, a) VALUES ($2, $1)';
        const params = ['value_a', 'value_b'];
        const result = translateParams(sql, params);
        expect(result.sql).toBe('INSERT INTO t (b, a) VALUES (?, ?)');
        expect(result.params).toEqual(['value_b', 'value_a']);
      });

      it('should handle repeated use of same placeholder', () => {
        const sql = 'SELECT * FROM t WHERE a = $1 OR b = $1';
        const params = ['test_value'];
        const result = translateParams(sql, params);
        expect(result.sql).toBe('SELECT * FROM t WHERE a = ? OR b = ?');
        expect(result.params).toEqual(['test_value', 'test_value']);
      });

      it('should handle complex reordering $3, $1, $2, $1', () => {
        const sql = 'UPDATE t SET c = $3, a = $1, b = $2 WHERE id = $1';
        const params = ['val_a', 'val_b', 'val_c'];
        const result = translateParams(sql, params);
        expect(result.sql).toBe('UPDATE t SET c = ?, a = ?, b = ? WHERE id = ?');
        expect(result.params).toEqual(['val_c', 'val_a', 'val_b', 'val_a']);
      });

      it('should handle gaps in placeholder numbers $1, $3 (skip $2)', () => {
        const sql = 'SELECT * FROM t WHERE a = $1 AND c = $3';
        const params = ['val_a', 'val_b', 'val_c'];
        const result = translateParams(sql, params);
        expect(result.sql).toBe('SELECT * FROM t WHERE a = ? AND c = ?');
        expect(result.params).toEqual(['val_a', 'val_c']);
      });
    });

    describe('edge cases', () => {
      it('should not translate $1 inside string literals', () => {
        const sql = "SELECT * FROM t WHERE note = 'costs $100' AND id = $1";
        const result = translateParams(sql, [42]);
        expect(result.sql).toContain("'costs $100'");
        expect(result.sql).toMatch(/id = \?$/);
      });

      it('should not translate $1 inside double-quoted identifiers', () => {
        const sql = 'SELECT "$1" FROM t WHERE id = $1';
        const result = translateParams(sql, [42]);
        expect(result.sql).toContain('"$1"');
        expect(result.sql).toMatch(/id = \?$/);
      });

      it('should handle empty params array', () => {
        const sql = 'SELECT * FROM users';
        const result = translateParams(sql, []);
        expect(result.sql).toBe('SELECT * FROM users');
        expect(result.params).toEqual([]);
      });

      it('should handle undefined params', () => {
        const sql = 'SELECT * FROM users WHERE id = $1';
        const result = translateParams(sql);
        expect(result.sql).toBe('SELECT * FROM users WHERE id = ?');
        expect(result.params).toEqual([]);
      });

      it('should preserve ? placeholders (pass-through for SQLite-native queries)', () => {
        const sql = 'SELECT * FROM users WHERE id = ?';
        const result = translateParams(sql, [42]);
        expect(result.sql).toBe('SELECT * FROM users WHERE id = ?');
        expect(result.params).toEqual([42]);
      });

      it('should handle mixed ? and $n placeholders (convert all to ?)', () => {
        // This is an edge case - ideally shouldn't mix, but handle gracefully
        const sql = 'SELECT * FROM t WHERE a = ? AND b = $1';
        const params = [1, 2];
        const result = translateParams(sql, params);
        // Should convert $1 to ? and keep original ?
        // Implementation should handle this consistently
        expect(result.sql).toBe('SELECT * FROM t WHERE a = ? AND b = ?');
      });
    });

    describe('named parameter translation', () => {
      it('should translate $name style named params to positional', () => {
        // Some Postgres clients support $name syntax
        const sql = 'SELECT * FROM users WHERE name = $name AND age = $age';
        const namedParams = { name: 'Alice', age: 30 };
        const result = translateParams(sql, namedParams);
        expect(result.sql).toBe('SELECT * FROM users WHERE name = ? AND age = ?');
        expect(result.params).toEqual(['Alice', 30]);
      });

      it('should handle repeated named params', () => {
        const sql = 'SELECT * FROM t WHERE a = $val OR b = $val';
        const namedParams = { val: 'test' };
        const result = translateParams(sql, namedParams);
        expect(result.sql).toBe('SELECT * FROM t WHERE a = ? OR b = ?');
        expect(result.params).toEqual(['test', 'test']);
      });
    });
  });

  describe('adapter integration with $n placeholders', () => {
    it('should execute query with $1 placeholder', async () => {
      await adapter.execute('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', [
        'Alice',
        'alice@example.com',
        30,
      ]);

      // Query using $1 style (Postgres compat mode)
      const result = await adapter.query('SELECT * FROM users WHERE id = $1', [1], {
        dialect: 'postgres',
      });
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Alice');
    });

    it('should execute INSERT with $1, $2, $3 placeholders', async () => {
      const result = await adapter.execute(
        'INSERT INTO users (name, email, age) VALUES ($1, $2, $3)',
        ['Bob', 'bob@example.com', 25],
        { dialect: 'postgres' }
      );
      expect(result.affected).toBe(1);

      const queryResult = await adapter.query('SELECT * FROM users WHERE email = ?', [
        'bob@example.com',
      ]);
      expect(queryResult.rows[0].name).toBe('Bob');
    });

    it('should handle UPDATE with reordered $n placeholders', async () => {
      await adapter.execute('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', [
        'Charlie',
        'charlie@example.com',
        35,
      ]);

      // Update with out-of-order placeholders
      await adapter.execute(
        'UPDATE users SET name = $2, age = $1 WHERE email = $3',
        [40, 'Charlie Updated', 'charlie@example.com'],
        { dialect: 'postgres' }
      );

      const result = await adapter.query('SELECT * FROM users WHERE email = ?', [
        'charlie@example.com',
      ]);
      expect(result.rows[0].name).toBe('Charlie Updated');
      expect(result.rows[0].age).toBe(40);
    });

    it('should handle DELETE with $1 placeholder', async () => {
      await adapter.execute('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', [
        'Dave',
        'dave@example.com',
        28,
      ]);

      const result = await adapter.execute('DELETE FROM users WHERE email = $1', ['dave@example.com'], {
        dialect: 'postgres',
      });
      expect(result.affected).toBe(1);
    });
  });

  describe('batch operations with $n placeholders', () => {
    it('should handle batch with $n placeholders', async () => {
      const result = await adapter.batch(
        [
          {
            sql: 'INSERT INTO users (name, email, age) VALUES ($1, $2, $3)',
            params: ['User1', 'user1@example.com', 20],
          },
          {
            sql: 'INSERT INTO users (name, email, age) VALUES ($1, $2, $3)',
            params: ['User2', 'user2@example.com', 21],
          },
        ],
        { dialect: 'postgres' }
      );

      expect(result.success).toBe(true);

      const queryResult = await adapter.query('SELECT COUNT(*) as count FROM users');
      expect(queryResult.rows[0].count).toBe(2);
    });
  });

  describe('transactions with $n placeholders', () => {
    it('should handle transaction with $n placeholders', async () => {
      await adapter.execute('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', [
        'Initial',
        'initial@example.com',
        50,
      ]);

      await adapter.transaction(
        async (tx) => {
          await tx.execute('UPDATE users SET age = $1 WHERE email = $2', [51, 'initial@example.com']);
        },
        { dialect: 'postgres' }
      );

      const result = await adapter.query('SELECT age FROM users WHERE email = ?', [
        'initial@example.com',
      ]);
      expect(result.rows[0].age).toBe(51);
    });
  });

  describe('error handling for invalid placeholders', () => {
    it('should throw error for missing params when $n is used', async () => {
      await expect(
        adapter.query('SELECT * FROM users WHERE id = $1', [], { dialect: 'postgres' })
      ).rejects.toThrow(/missing.*param|insufficient.*param/i);
    });

    it('should throw error for placeholder number exceeding params length', async () => {
      await expect(
        adapter.query('SELECT * FROM users WHERE id = $5', [1, 2, 3], { dialect: 'postgres' })
      ).rejects.toThrow(/param.*out of range|missing.*param/i);
    });

    it('should throw error for $0 placeholder (1-indexed)', () => {
      expect(() => translateParams('SELECT * FROM users WHERE id = $0', [1])).toThrow(
        /invalid.*placeholder|zero.*index/i
      );
    });
  });
});
