import { describe, expect, it, vi, afterEach } from 'vitest';
import { ApiError, api, parseConnectionString } from './client';

describe('parseConnectionString', () => {
	it('splits a full postgres URI', () => {
		expect(parseConnectionString('postgresql://alice:pw@db.example.com:6543/acme')).toEqual({
			host: 'db.example.com',
			port: '6543',
			database: 'acme',
			user: 'alice',
		});
	});

	it('defaults the port to 5432 when omitted', () => {
		expect(parseConnectionString('postgresql://bob@localhost/shop').port).toBe('5432');
	});

	it('percent-decodes the username', () => {
		expect(parseConnectionString('postgresql://a%40b:pw@localhost/db').user).toBe('a@b');
	});

	it('returns empty fields rather than throwing on junk', () => {
		expect(parseConnectionString('not a uri')).toEqual({
			host: '',
			port: '5432',
			database: '',
			user: '',
		});
	});
});

describe('request error handling', () => {
	afterEach(() => vi.unstubAllGlobals());

	it('throws ApiError carrying the status and server message', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue({
				ok: false,
				status: 500,
				text: async () => JSON.stringify({ error: 'Update query failed' }),
				headers: new Headers(),
			}),
		);
		await expect(api.health()).rejects.toMatchObject({
			name: 'Error',
			status: 500,
			message: 'Update query failed',
		});
	});

	it('falls back to raw text when the body is not JSON', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn().mockResolvedValue({
				ok: false,
				status: 502,
				text: async () => 'upstream died',
				headers: new Headers(),
			}),
		);
		await expect(api.health()).rejects.toThrow('upstream died');
	});

	it('ApiError preserves its status', () => {
		expect(new ApiError(418, 'teapot').status).toBe(418);
	});
});
