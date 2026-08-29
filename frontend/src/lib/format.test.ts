import { describe, expect, it } from 'vitest';
import { cellClass, type ColumnMeta } from './format';

const columns: ColumnMeta[] = [
	{ name: 'id', type: 'integer' },
	{ name: 'salary', type: 'numeric' },
	{ name: 'ratio', type: 'double precision' },
	{ name: 'active', type: 'boolean' },
	{ name: 'name', type: 'character varying' },
	{ name: 'payload', type: 'jsonb' },
];

describe('cellClass', () => {
	it('styles NULL regardless of column type', () => {
		for (const col of ['id', 'name', 'active', 'unknown']) {
			expect(cellClass(null, col, columns)).toBe('italic text-muted-foreground/50');
		}
	});

	it.each([
		['id', 'integer'],
		['salary', 'numeric'],
		['ratio', 'double precision'],
	])('right-aligns %s (%s) with tabular figures', colName => {
		expect(cellClass('1', colName, columns)).toBe('tabular-nums text-right');
	});

	it('centres booleans', () => {
		expect(cellClass('t', 'active', columns)).toBe('text-center');
	});

	it('leaves text and jsonb unstyled', () => {
		expect(cellClass('Alice', 'name', columns)).toBe('');
		expect(cellClass('{"ok": true}', 'payload', columns)).toBe('');
	});

	it('returns no class for a column it does not know', () => {
		expect(cellClass('x', 'nope', columns)).toBe('');
	});

	it('does not throw when columns are missing', () => {
		expect(cellClass('x', 'id', undefined)).toBe('');
		expect(cellClass(null, 'id', undefined)).toBe('italic text-muted-foreground/50');
	});
});
