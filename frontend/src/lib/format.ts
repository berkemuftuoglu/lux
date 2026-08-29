export interface ColumnMeta {
	name: string;
	type: string;
}

// Extracted from DataGrid so the type-to-alignment rules can be tested without
// standing up the whole grid. NULL is styled by absence of a value, not by type,
// so it is checked before the column is looked up at all.
export function cellClass(
	value: string | null,
	colName: string,
	columns: readonly ColumnMeta[] | undefined,
): string {
	if (value === null) return 'italic text-muted-foreground/50';
	const col = columns?.find(c => c.name === colName);
	if (!col) return '';
	const t = col.type.toLowerCase();
	// Postgres reports these as 'double precision' and 'character varying', so exact
	// matches miss: 'double' never equalled the reported type and those columns were
	// left-aligned as if they were text.
	if (
		t.includes('int') ||
		t.includes('numeric') ||
		t.includes('decimal') ||
		t.includes('float') ||
		t.includes('real') ||
		t.includes('double') ||
		t.includes('money')
	) {
		return 'tabular-nums text-right';
	}
	if (t === 'boolean' || t === 'bool') return 'text-center';
	return '';
}
