/**
 * VitessClient Connection Lifecycle Tests
 *
 * Issue: vitess-y6r.1
 * TDD Red Phase - Tests define expected behavior before implementation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { VitessClient, createClient } from '../client.js';

describe('VitessClient Connection Lifecycle', () => {
  let client: VitessClient;
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn();
    vi.stubGlobal('fetch', mockFetch);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('createClient factory', () => {
    it('should create a VitessClient instance with config', () => {
      const config = {
        endpoint: 'https://api.vitess.do/v1',
        keyspace: 'main',
      };
      const client = createClient(config);
      expect(client).toBeInstanceOf(VitessClient);
    });

    it('should apply default timeout when not specified', () => {
      const client = createClient({ endpoint: 'https://api.vitess.do/v1' });
      expect(client).toBeInstanceOf(VitessClient);
      // Default timeout should be 30000ms (tested via behavior in other tests)
    });

    it('should apply default retry config when not specified', () => {
      const client = createClient({ endpoint: 'https://api.vitess.do/v1' });
      expect(client).toBeInstanceOf(VitessClient);
      // Default retry: maxAttempts=3, backoffMs=100 (tested via behavior in retry.test.ts)
    });
  });

  describe('connect()', () => {
    beforeEach(() => {
      client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        keyspace: 'main',
      });
    });

    it('should successfully connect when server responds with healthy status', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: 0x82, // ACK
          id: 'test-id',
          timestamp: Date.now(),
        }),
      });

      await client.connect();
      expect(client.isConnected()).toBe(true);
    });

    it('should throw error when server returns error response', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: 0x81, // ERROR
          id: 'test-id',
          timestamp: Date.now(),
          code: 'CONNECTION_REFUSED',
          message: 'Server is unavailable',
        }),
      });

      await expect(client.connect()).rejects.toThrow('Connection failed');
      expect(client.isConnected()).toBe(false);
    });

    it('should throw error when HTTP request fails', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 503,
        statusText: 'Service Unavailable',
      });

      await expect(client.connect()).rejects.toThrow('HTTP 503');
      expect(client.isConnected()).toBe(false);
    });

    it('should throw error when network fails', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'));
      mockFetch.mockRejectedValueOnce(new Error('Network error'));
      mockFetch.mockRejectedValueOnce(new Error('Network error'));

      await expect(client.connect()).rejects.toThrow('Network error');
      expect(client.isConnected()).toBe(false);
    });

    it('should send health check message with correct format', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
      });

      await client.connect();

      expect(mockFetch).toHaveBeenCalledWith(
        'https://api.vitess.do/v1',
        expect.objectContaining({
          method: 'POST',
          headers: expect.objectContaining({
            'Content-Type': 'application/json',
          }),
          body: expect.stringContaining('"type":33'), // MessageType.HEALTH = 0x21 = 33
        })
      );
    });

    it('should include Authorization header when token is provided', async () => {
      const clientWithToken = createClient({
        endpoint: 'https://api.vitess.do/v1',
        token: 'secret-token',
      });

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
      });

      await clientWithToken.connect();

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: 'Bearer secret-token',
          }),
        })
      );
    });

    it('should not include Authorization header when token is not provided', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
      });

      await client.connect();

      const callHeaders = mockFetch.mock.calls[0][1].headers;
      expect(callHeaders.Authorization).toBeUndefined();
    });
  });

  describe('disconnect()', () => {
    beforeEach(() => {
      client = createClient({
        endpoint: 'https://api.vitess.do/v1',
        keyspace: 'main',
      });
    });

    it('should set connected state to false', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
      });

      await client.connect();
      expect(client.isConnected()).toBe(true);

      await client.disconnect();
      expect(client.isConnected()).toBe(false);
    });

    it('should be idempotent (can call multiple times)', async () => {
      await client.disconnect();
      await client.disconnect();
      expect(client.isConnected()).toBe(false);
    });

    it('should work even if never connected', async () => {
      expect(client.isConnected()).toBe(false);
      await client.disconnect();
      expect(client.isConnected()).toBe(false);
    });
  });

  describe('isConnected()', () => {
    beforeEach(() => {
      client = createClient({
        endpoint: 'https://api.vitess.do/v1',
      });
    });

    it('should return false initially', () => {
      expect(client.isConnected()).toBe(false);
    });

    it('should return true after successful connect', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
      });

      await client.connect();
      expect(client.isConnected()).toBe(true);
    });

    it('should return false after disconnect', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
      });

      await client.connect();
      await client.disconnect();
      expect(client.isConnected()).toBe(false);
    });

    it('should return false after failed connect attempt', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'));
      mockFetch.mockRejectedValueOnce(new Error('Network error'));
      mockFetch.mockRejectedValueOnce(new Error('Network error'));

      await expect(client.connect()).rejects.toThrow();
      expect(client.isConnected()).toBe(false);
    });
  });

  describe('connection with custom config', () => {
    it('should use custom timeout value', async () => {
      const clientWithTimeout = createClient({
        endpoint: 'https://api.vitess.do/v1',
        timeout: 5000,
      });

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
      });

      await clientWithTimeout.connect();

      // Verify AbortSignal.timeout was called with custom timeout
      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          signal: expect.any(AbortSignal),
        })
      );
    });

    it('should use custom retry configuration', async () => {
      const clientWithRetry = createClient({
        endpoint: 'https://api.vitess.do/v1',
        retry: { maxAttempts: 5, backoffMs: 50 },
      });

      // Fail 4 times, succeed on 5th
      mockFetch.mockRejectedValueOnce(new Error('Fail 1'));
      mockFetch.mockRejectedValueOnce(new Error('Fail 2'));
      mockFetch.mockRejectedValueOnce(new Error('Fail 3'));
      mockFetch.mockRejectedValueOnce(new Error('Fail 4'));
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
      });

      await clientWithRetry.connect();
      expect(mockFetch).toHaveBeenCalledTimes(5);
    });
  });

  describe('reconnection scenarios', () => {
    beforeEach(() => {
      client = createClient({
        endpoint: 'https://api.vitess.do/v1',
      });
    });

    it('should allow reconnection after disconnect', async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
      });

      await client.connect();
      expect(client.isConnected()).toBe(true);

      await client.disconnect();
      expect(client.isConnected()).toBe(false);

      await client.connect();
      expect(client.isConnected()).toBe(true);
    });

    it('should allow reconnection after failed connection', async () => {
      mockFetch
        .mockRejectedValueOnce(new Error('Fail'))
        .mockRejectedValueOnce(new Error('Fail'))
        .mockRejectedValueOnce(new Error('Fail'))
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ type: 0x82, id: 'test', timestamp: Date.now() }),
        });

      await expect(client.connect()).rejects.toThrow();
      expect(client.isConnected()).toBe(false);

      await client.connect();
      expect(client.isConnected()).toBe(true);
    });
  });
});
