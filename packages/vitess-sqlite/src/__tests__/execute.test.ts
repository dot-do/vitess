/**
 * TursoAdapter Write Execution Tests
 *
 * Tests for INSERT, UPDATE, DELETE operations against SQLite via Turso/libSQL.
 * Issue: vitess-1bb.5
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { TursoAdapter } from '../index.js';
import type { ExecuteResult } from '@dotdo/vitess-rpc';

describe('TursoAdapter Write Execution', () => {
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
        age INTEGER,
        active INTEGER DEFAULT 1
      )
    `);
  });

  afterEach(async () => {
    await adapter.close();
  });

  describe('INSERT operations', () => {
    it('should insert a single row', async () => {
      const result = await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`
      );
      expect(result.affected).toBe(1);
    });

    it('should return last insert ID', async () => {
      const result = await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`
      );
      expect(result.lastInsertId).toBe(1);
    });

    it('should increment last insert ID for subsequent inserts', async () => {
      await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`
      );
      const result = await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)`
      );
      expect(result.lastInsertId).toBe(2);
    });

    it('should handle parameterized INSERT', async () => {
      const result = await adapter.execute(
        'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
        ['Charlie', 'charlie@example.com', 35]
      );
      expect(result.affected).toBe(1);

      const queryResult = await adapter.query(
        `SELECT * FROM users WHERE email = 'charlie@example.com'`
      );
      expect(queryResult.rows[0].name).toBe('Charlie');
    });

    it('should handle INSERT with NULL values', async () => {
      const result = await adapter.execute(
        'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
        ['Dave', 'dave@example.com', null]
      );
      expect(result.affected).toBe(1);

      const queryResult = await adapter.query(
        `SELECT age FROM users WHERE email = 'dave@example.com'`
      );
      expect(queryResult.rows[0].age).toBeNull();
    });

    it('should handle INSERT with DEFAULT values', async () => {
      await adapter.execute(
        'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
        ['Eve', 'eve@example.com', 28]
      );
      const queryResult = await adapter.query(
        `SELECT active FROM users WHERE email = 'eve@example.com'`
      );
      expect(queryResult.rows[0].active).toBe(1); // Default value
    });

    it('should handle bulk INSERT', async () => {
      const result = await adapter.execute(`
        INSERT INTO users (name, email, age) VALUES
        ('User1', 'user1@example.com', 20),
        ('User2', 'user2@example.com', 21),
        ('User3', 'user3@example.com', 22)
      `);
      expect(result.affected).toBe(3);
    });

    it('should handle INSERT OR REPLACE', async () => {
      await adapter.execute(
        'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
        ['Alice', 'alice@example.com', 30]
      );
      const result = await adapter.execute(
        'INSERT OR REPLACE INTO users (id, name, email, age) VALUES (1, ?, ?, ?)',
        ['Alice Updated', 'alice@example.com', 31]
      );
      expect(result.affected).toBe(1);

      const queryResult = await adapter.query('SELECT * FROM users WHERE id = 1');
      expect(queryResult.rows[0].name).toBe('Alice Updated');
      expect(queryResult.rows[0].age).toBe(31);
    });

    it('should handle INSERT OR IGNORE', async () => {
      await adapter.execute(
        'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
        ['Alice', 'alice@example.com', 30]
      );
      // This should be ignored due to unique constraint on email
      const result = await adapter.execute(
        'INSERT OR IGNORE INTO users (name, email, age) VALUES (?, ?, ?)',
        ['Alice Duplicate', 'alice@example.com', 31]
      );
      expect(result.affected).toBe(0);

      const queryResult = await adapter.query('SELECT COUNT(*) as count FROM users');
      expect(queryResult.rows[0].count).toBe(1);
    });
  });

  describe('UPDATE operations', () => {
    beforeEach(async () => {
      await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`
      );
      await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)`
      );
      await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)`
      );
    });

    it('should update a single row', async () => {
      const result = await adapter.execute(
        `UPDATE users SET age = 31 WHERE email = 'alice@example.com'`
      );
      expect(result.affected).toBe(1);

      const queryResult = await adapter.query(
        `SELECT age FROM users WHERE email = 'alice@example.com'`
      );
      expect(queryResult.rows[0].age).toBe(31);
    });

    it('should update multiple rows', async () => {
      const result = await adapter.execute('UPDATE users SET active = 0 WHERE age >= 30');
      expect(result.affected).toBe(2); // Alice (30) and Charlie (35)
    });

    it('should handle parameterized UPDATE', async () => {
      const result = await adapter.execute('UPDATE users SET name = ? WHERE id = ?', [
        'Alice Smith',
        1,
      ]);
      expect(result.affected).toBe(1);

      const queryResult = await adapter.query('SELECT name FROM users WHERE id = 1');
      expect(queryResult.rows[0].name).toBe('Alice Smith');
    });

    it('should return 0 affected when no rows match', async () => {
      const result = await adapter.execute('UPDATE users SET age = 99 WHERE id = 999');
      expect(result.affected).toBe(0);
    });

    it('should handle UPDATE with expression', async () => {
      await adapter.execute('UPDATE users SET age = age + 1 WHERE id = 1');
      const queryResult = await adapter.query('SELECT age FROM users WHERE id = 1');
      expect(queryResult.rows[0].age).toBe(31);
    });

    it('should handle UPDATE with subquery', async () => {
      await adapter.execute(`
        UPDATE users
        SET age = (SELECT MAX(age) FROM users)
        WHERE id = 1
      `);
      const queryResult = await adapter.query('SELECT age FROM users WHERE id = 1');
      expect(queryResult.rows[0].age).toBe(35);
    });
  });

  describe('DELETE operations', () => {
    beforeEach(async () => {
      await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`
      );
      await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)`
      );
      await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)`
      );
    });

    it('should delete a single row', async () => {
      const result = await adapter.execute(`DELETE FROM users WHERE id = 1`);
      expect(result.affected).toBe(1);

      const queryResult = await adapter.query('SELECT COUNT(*) as count FROM users');
      expect(queryResult.rows[0].count).toBe(2);
    });

    it('should delete multiple rows', async () => {
      const result = await adapter.execute('DELETE FROM users WHERE age >= 30');
      expect(result.affected).toBe(2); // Alice and Charlie
    });

    it('should handle parameterized DELETE', async () => {
      const result = await adapter.execute('DELETE FROM users WHERE email = ?', [
        'bob@example.com',
      ]);
      expect(result.affected).toBe(1);
    });

    it('should return 0 affected when no rows match', async () => {
      const result = await adapter.execute('DELETE FROM users WHERE id = 999');
      expect(result.affected).toBe(0);
    });

    it('should delete all rows with no WHERE clause', async () => {
      const result = await adapter.execute('DELETE FROM users');
      expect(result.affected).toBe(3);

      const queryResult = await adapter.query('SELECT COUNT(*) as count FROM users');
      expect(queryResult.rows[0].count).toBe(0);
    });
  });

  describe('DDL operations', () => {
    it('should create a table', async () => {
      const result = await adapter.execute(`
        CREATE TABLE products (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL
        )
      `);
      expect(result.affected).toBe(0); // DDL doesn't affect rows
    });

    it('should drop a table', async () => {
      await adapter.execute('CREATE TABLE temp_table (id INTEGER)');
      const result = await adapter.execute('DROP TABLE temp_table');
      expect(result.affected).toBe(0);
    });

    it('should create an index', async () => {
      const result = await adapter.execute('CREATE INDEX idx_users_email ON users(email)');
      expect(result.affected).toBe(0);
    });

    it('should alter a table', async () => {
      const result = await adapter.execute('ALTER TABLE users ADD COLUMN status TEXT');
      expect(result.affected).toBe(0);

      // Verify column was added
      const queryResult = await adapter.query('PRAGMA table_info(users)');
      const statusColumn = queryResult.rows.find((r) => r.name === 'status');
      expect(statusColumn).toBeDefined();
    });
  });

  describe('execute result conformance', () => {
    it('should return a valid ExecuteResult object', async () => {
      const result: ExecuteResult = await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Test', 'test@example.com', 20)`
      );
      expect(result).toHaveProperty('affected');
      expect(typeof result.affected).toBe('number');
    });

    it('should optionally include lastInsertId', async () => {
      const result: ExecuteResult = await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Test', 'test@example.com', 20)`
      );
      // lastInsertId is optional but should be present for INSERT
      if (result.lastInsertId !== undefined) {
        expect(typeof result.lastInsertId).toBe('number');
      }
    });
  });
});
