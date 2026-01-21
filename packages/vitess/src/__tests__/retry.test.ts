/**
 * VitessClient Retry Logic Tests
 *
 * Issue: vitess-y6r.13
 * TDD Red Phase - Tests define expected behavior before implementation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { VitessClient, createClient, VitessError } from '../client.js';

// MessageType constants for test responses
const MessageType = {
  HEALTH: 0x21,
  QUERY: 0x01,
  EXECUTE: 0x02,
  RESULT: 0x80,
  ERROR: 0x81,
  ACK: 0x82,
};

describe('VitessClient Retry Logic', () => {
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn();
    vi.stubGlobal('fetch', mockFetch);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  describe('default retry configuration', () => {
    it('should use default maxAttempts of 3', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
      });

      // Fail all 3 attempts
      mockFetch.mockRejectedValue(new Error('Network error'));

      const connectPromise = client.connect();

      // Fast-forward through all retry delays
      await vi.advanceTimersByTimeAsync(100); // First retry backoff
      await vi.advanceTimersByTimeAsync(200); // Second retry backoff
      await vi.advanceTimersByTimeAsync(300); // Third retry backoff

      await expect(connectPromise).rejects.toThrow('Network error');
      expect(mockFetch).toHaveBeenCalledTimes(3);
    });

    it('should use default backoffMs of 100', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
      });

      // Fail first two, succeed on third
      mockFetch
        .mockRejectedValueOnce(new Error('Fail 1'))
        .mockRejectedValueOnce(new Error('Fail 2'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();

      // Advance timer for first backoff (100ms * 1)
      await vi.advanceTimersByTimeAsync(100);
      // Advance timer for second backoff (100ms * 2)
      await vi.advanceTimersByTimeAsync(200);

      await connectPromise;
      expect(client.isConnected()).toBe(true);
    });
  });

  describe('custom retry configuration', () => {
    it('should respect custom maxAttempts', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 5, backoffMs: 50 },
      });

      // Fail 4 times, succeed on 5th
      mockFetch
        .mockRejectedValueOnce(new Error('Fail 1'))
        .mockRejectedValueOnce(new Error('Fail 2'))
        .mockRejectedValueOnce(new Error('Fail 3'))
        .mockRejectedValueOnce(new Error('Fail 4'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();

      // Fast-forward through all retries
      await vi.advanceTimersByTimeAsync(50);   // 50 * 1
      await vi.advanceTimersByTimeAsync(100);  // 50 * 2
      await vi.advanceTimersByTimeAsync(150);  // 50 * 3
      await vi.advanceTimersByTimeAsync(200);  // 50 * 4

      await connectPromise;
      expect(mockFetch).toHaveBeenCalledTimes(5);
      expect(client.isConnected()).toBe(true);
    });

    it('should respect custom backoffMs', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 3, backoffMs: 200 },
      });

      mockFetch
        .mockRejectedValueOnce(new Error('Fail 1'))
        .mockRejectedValueOnce(new Error('Fail 2'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();

      // First retry after 200ms * 1
      await vi.advanceTimersByTimeAsync(200);
      // Second retry after 200ms * 2
      await vi.advanceTimersByTimeAsync(400);

      await connectPromise;
      expect(client.isConnected()).toBe(true);
    });

    it('should handle maxAttempts of 1 (no retries)', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 1, backoffMs: 100 },
      });

      mockFetch.mockRejectedValueOnce(new Error('Network error'));

      await expect(client.connect()).rejects.toThrow('Network error');
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });
  });

  describe('exponential backoff behavior', () => {
    it('should increase backoff exponentially with each attempt', async () => {
      const baseBackoff = 100;
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 4, backoffMs: baseBackoff },
      });

      const callTimes: number[] = [];
      let currentTime = 0;

      mockFetch.mockImplementation(async () => {
        callTimes.push(currentTime);
        throw new Error('Network error');
      });

      const connectPromise = client.connect();

      // Simulate time passing with exponential backoff
      // Attempt 1: immediate
      // Attempt 2: after baseBackoff * 1 = 100ms
      currentTime += baseBackoff * 1;
      await vi.advanceTimersByTimeAsync(baseBackoff * 1);

      // Attempt 3: after baseBackoff * 2 = 200ms
      currentTime += baseBackoff * 2;
      await vi.advanceTimersByTimeAsync(baseBackoff * 2);

      // Attempt 4: after baseBackoff * 3 = 300ms
      currentTime += baseBackoff * 3;
      await vi.advanceTimersByTimeAsync(baseBackoff * 3);

      await expect(connectPromise).rejects.toThrow('Network error');
      expect(mockFetch).toHaveBeenCalledTimes(4);

      // Verify backoff intervals are increasing
      if (callTimes.length >= 3) {
        const interval1 = callTimes[1] - callTimes[0]; // baseBackoff * 1
        const interval2 = callTimes[2] - callTimes[1]; // baseBackoff * 2
        expect(interval2).toBeGreaterThan(interval1);
      }
    });
  });

  describe('retry on network errors', () => {
    it('should retry on fetch network failure', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 3, backoffMs: 50 },
      });

      mockFetch
        .mockRejectedValueOnce(new TypeError('Failed to fetch'))
        .mockRejectedValueOnce(new TypeError('Failed to fetch'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();

      await vi.advanceTimersByTimeAsync(50);
      await vi.advanceTimersByTimeAsync(100);

      await connectPromise;
      expect(client.isConnected()).toBe(true);
      expect(mockFetch).toHaveBeenCalledTimes(3);
    });

    it('should retry on DNS resolution errors', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 2, backoffMs: 25 },
      });

      mockFetch
        .mockRejectedValueOnce(new Error('getaddrinfo ENOTFOUND api.vitess.do'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();
      await vi.advanceTimersByTimeAsync(25);

      await connectPromise;
      expect(client.isConnected()).toBe(true);
    });

    it('should retry on connection timeout', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 2, backoffMs: 25 },
      });

      mockFetch
        .mockRejectedValueOnce(new DOMException('The operation was aborted', 'AbortError'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();
      await vi.advanceTimersByTimeAsync(25);

      await connectPromise;
      expect(client.isConnected()).toBe(true);
    });
  });

  describe('retry on HTTP 5xx errors', () => {
    it('should retry on HTTP 500 Internal Server Error', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 3, backoffMs: 50 },
      });

      mockFetch
        .mockResolvedValueOnce({ ok: false, status: 500, statusText: 'Internal Server Error' })
        .mockResolvedValueOnce({ ok: false, status: 500, statusText: 'Internal Server Error' })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();

      await vi.advanceTimersByTimeAsync(50);
      await vi.advanceTimersByTimeAsync(100);

      await connectPromise;
      expect(client.isConnected()).toBe(true);
    });

    it('should retry on HTTP 502 Bad Gateway', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 2, backoffMs: 25 },
      });

      mockFetch
        .mockResolvedValueOnce({ ok: false, status: 502, statusText: 'Bad Gateway' })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();
      await vi.advanceTimersByTimeAsync(25);

      await connectPromise;
      expect(client.isConnected()).toBe(true);
    });

    it('should retry on HTTP 503 Service Unavailable', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 2, backoffMs: 25 },
      });

      mockFetch
        .mockResolvedValueOnce({ ok: false, status: 503, statusText: 'Service Unavailable' })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();
      await vi.advanceTimersByTimeAsync(25);

      await connectPromise;
      expect(client.isConnected()).toBe(true);
    });

    it('should retry on HTTP 504 Gateway Timeout', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 2, backoffMs: 25 },
      });

      mockFetch
        .mockResolvedValueOnce({ ok: false, status: 504, statusText: 'Gateway Timeout' })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: MessageType.ACK, id: 'test', timestamp: Date.now() }),
        });

      const connectPromise = client.connect();
      await vi.advanceTimersByTimeAsync(25);

      await connectPromise;
      expect(client.isConnected()).toBe(true);
    });
  });

  describe('no retry on client errors', () => {
    it('should not retry on HTTP 400 Bad Request', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 3, backoffMs: 50 },
      });

      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 400,
        statusText: 'Bad Request',
      });

      await expect(client.connect()).rejects.toThrow('HTTP 400');
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    it('should not retry on HTTP 401 Unauthorized', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 3, backoffMs: 50 },
      });

      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 401,
        statusText: 'Unauthorized',
      });

      await expect(client.connect()).rejects.toThrow('HTTP 401');
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    it('should not retry on HTTP 403 Forbidden', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 3, backoffMs: 50 },
      });

      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 403,
        statusText: 'Forbidden',
      });

      await expect(client.connect()).rejects.toThrow('HTTP 403');
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    it('should not retry on HTTP 404 Not Found', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 3, backoffMs: 50 },
      });

      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: 'Not Found',
      });

      await expect(client.connect()).rejects.toThrow('HTTP 404');
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });
  });

  describe('retry across different operations', () => {
    let client: VitessClient;

    beforeEach(async () => {
      vi.useRealTimers(); // Use real timers for these tests

      client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        keyspace: 'main',
        retry: { maxAttempts: 3, backoffMs: 10 }, // Small backoff for faster tests
      });

      // Mock successful connection
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: MessageType.ACK, id: 'health', timestamp: Date.now() }),
      });
      await client.connect();
      mockFetch.mockClear();
    });

    it('should retry query() on transient errors', async () => {
      mockFetch
        .mockRejectedValueOnce(new Error('Connection reset'))
        .mockRejectedValueOnce(new Error('Connection reset'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            type: MessageType.RESULT,
            result: { rows: [{ id: 1 }], rowCount: 1 },
          }),
        });

      const result = await client.query('SELECT * FROM users');
      expect(result.rows).toHaveLength(1);
      expect(mockFetch).toHaveBeenCalledTimes(3);
    });

    it('should retry execute() on transient errors', async () => {
      mockFetch
        .mockRejectedValueOnce(new Error('ECONNRESET'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            type: MessageType.RESULT,
            result: { affected: 1 },
          }),
        });

      const result = await client.execute('UPDATE users SET active = true');
      expect(result.affected).toBe(1);
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    it('should retry batch() on transient errors', async () => {
      mockFetch
        .mockRejectedValueOnce(new Error('Network timeout'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            type: MessageType.RESULT,
            result: { results: [], success: true },
          }),
        });

      const result = await client.batch([{ sql: 'SELECT 1' }]);
      expect(result.success).toBe(true);
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });
  });

  describe('final error after exhausting retries', () => {
    it('should throw last error after all retries exhausted', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 3, backoffMs: 10 },
      });

      mockFetch
        .mockRejectedValueOnce(new Error('Error 1'))
        .mockRejectedValueOnce(new Error('Error 2'))
        .mockRejectedValueOnce(new Error('Error 3 - Final'));

      const connectPromise = client.connect();

      await vi.advanceTimersByTimeAsync(10);
      await vi.advanceTimersByTimeAsync(20);
      await vi.advanceTimersByTimeAsync(30);

      await expect(connectPromise).rejects.toThrow('Error 3 - Final');
    });

    it('should preserve HTTP status in error after retries exhausted', async () => {
      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 2, backoffMs: 10 },
      });

      mockFetch
        .mockResolvedValueOnce({ ok: false, status: 503, statusText: 'Service Unavailable' })
        .mockResolvedValueOnce({ ok: false, status: 503, statusText: 'Service Unavailable' });

      const connectPromise = client.connect();
      await vi.advanceTimersByTimeAsync(10);
      await vi.advanceTimersByTimeAsync(20);

      await expect(connectPromise).rejects.toThrow('HTTP 503');
    });
  });

  describe('retry state isolation', () => {
    it('should reset retry count between operations', async () => {
      vi.useRealTimers();

      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        keyspace: 'main',
        retry: { maxAttempts: 2, backoffMs: 5 },
      });

      // Mock successful connection
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: MessageType.ACK, id: 'health', timestamp: Date.now() }),
      });
      await client.connect();
      mockFetch.mockClear();

      // First query: fail once, succeed
      mockFetch
        .mockRejectedValueOnce(new Error('Transient error'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            type: MessageType.RESULT,
            result: { rows: [], rowCount: 0 },
          }),
        });

      await client.query('SELECT 1');
      expect(mockFetch).toHaveBeenCalledTimes(2);
      mockFetch.mockClear();

      // Second query: should also get fresh retry count
      mockFetch
        .mockRejectedValueOnce(new Error('Transient error'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            type: MessageType.RESULT,
            result: { rows: [], rowCount: 0 },
          }),
        });

      await client.query('SELECT 2');
      expect(mockFetch).toHaveBeenCalledTimes(2); // Fresh 2 attempts, not continuing from previous
    });
  });

  describe('concurrent requests with retries', () => {
    it('should handle concurrent requests with independent retry state', async () => {
      vi.useRealTimers();

      const client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        keyspace: 'main',
        retry: { maxAttempts: 2, backoffMs: 5 },
      });

      // Mock successful connection
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: MessageType.ACK, id: 'health', timestamp: Date.now() }),
      });
      await client.connect();
      mockFetch.mockClear();

      // Set up responses for concurrent queries
      // Query 1: fail, succeed
      // Query 2: succeed immediately
      mockFetch
        .mockRejectedValueOnce(new Error('Error for query 1')) // Query 1, attempt 1
        .mockResolvedValueOnce({                               // Query 2, attempt 1
          ok: true,
          json: async () => ({
            type: MessageType.RESULT,
            result: { rows: [{ q: 2 }], rowCount: 1 },
          }),
        })
        .mockResolvedValueOnce({                               // Query 1, attempt 2
          ok: true,
          json: async () => ({
            type: MessageType.RESULT,
            result: { rows: [{ q: 1 }], rowCount: 1 },
          }),
        });

      const [result1, result2] = await Promise.all([
        client.query('SELECT 1 as q'),
        client.query('SELECT 2 as q'),
      ]);

      // Both should succeed
      expect(result1.rowCount).toBe(1);
      expect(result2.rowCount).toBe(1);
    });
  });
});
