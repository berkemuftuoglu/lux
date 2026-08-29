<script lang="ts">
	import { api } from '$lib/api/client';
	import { appState } from '$lib/stores/app.svelte';
	import * as Table from '$lib/components/ui/table';
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';
	import { toast } from 'svelte-sonner';
	import { Play, X, Download, Loader2 } from 'lucide-svelte';
	import CodeMirror from 'svelte-codemirror-editor';
	import { sql, PostgreSQL } from '@codemirror/lang-sql';
	import { oneDark } from '@codemirror/theme-one-dark';

	let { onClose, value = $bindable('SELECT 1;') }: { onClose: () => void; value?: string } = $props();
	let resultColumns = $state<string[]>([]);
	let resultRows = $state<(string | null)[][]>([]);
	let affectedRows = $state<number | null>(null);
	let message = $state<string | null>(null);
	let executing = $state(false);
	let durationMs = $state<number | null>(null);

	// Build schema completions for CodeMirror
	const schemaCompletion = $derived.by(() => {
		const tables: Record<string, string[]> = {};
		for (const t of appState.tables) {
			tables[t.name] = t.columns.map(c => c.name);
		}
		return tables;
	});

	async function execute() {
		if (!value.trim()) return;
		executing = true;
		resultColumns = [];
		resultRows = [];
		affectedRows = null;
		message = null;
		durationMs = null;

		const start = performance.now();
		try {
			const result = await api.executeSql(value);
			durationMs = Math.round(performance.now() - start);

			if (result.columns && result.rows) {
				resultColumns = result.columns;
				resultRows = result.rows;
			}
			// BUG: /api/sql returns only row_count/columns/rows — never affected_rows or
			// message — so non-SELECT statements give the user no feedback at all.
			// Reading row_count here would be wrong too: for an UPDATE it is 0 (no rows
			// returned), not the number changed. Needs a backend change to report the
			// command tag's row count. Tracked in TODO.md.
			if (result.row_count !== undefined && resultColumns.length === 0) {
				affectedRows = null;
			}
			// Refresh table data if we modified something
			if (affectedRows !== null && appState.selectedTable) {
				appState.loadTableData();
			}
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Query failed');
		} finally {
			executing = false;
		}
	}

	function handleKeydown(e: KeyboardEvent) {
		if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
			e.preventDefault();
			execute();
		}
	}

	async function exportResults(format: 'csv' | 'json') {
		if (!value.trim()) return;
		try {
			const data = await api.exportSql(value, format);
			const blob = new Blob([data], { type: format === 'csv' ? 'text/csv' : 'application/json' });
			const url = URL.createObjectURL(blob);
			const a = document.createElement('a');
			a.href = url;
			a.download = `query-results.${format}`;
			a.click();
			URL.revokeObjectURL(url);
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Export failed');
		}
	}
</script>

<div class="flex flex-col border-b border-border" role="region" aria-label="SQL Editor" onkeydown={handleKeydown}>
	<!-- Editor toolbar -->
	<div class="flex items-center gap-2 border-b border-border px-3 py-1">
		<span class="text-xs font-medium text-muted-foreground">SQL</span>
		<div class="flex-1"></div>
		<Button variant="ghost" size="sm" class="h-7 gap-1 text-xs" onclick={execute} disabled={executing}>
			{#if executing}
				<Loader2 class="h-3 w-3 animate-spin" />
			{:else}
				<Play class="h-3 w-3" />
			{/if}
			Run
		</Button>
		{#if resultColumns.length > 0 || value.trim().length > 0}
			<Button variant="ghost" size="sm" class="h-7 gap-1 text-xs" onclick={() => exportResults('csv')}>
				<Download class="h-3 w-3" />
				CSV
			</Button>
			<Button variant="ghost" size="sm" class="h-7 gap-1 text-xs" onclick={() => exportResults('json')}>
				<Download class="h-3 w-3" />
				JSON
			</Button>
		{/if}
		<Button variant="ghost" size="icon" class="h-7 w-7" onclick={onClose}>
			<X class="h-3.5 w-3.5" />
		</Button>
	</div>

	<!-- CodeMirror editor -->
	<div class="h-36 overflow-auto border-b border-border">
		<CodeMirror
			bind:value
			lang={sql({ dialect: PostgreSQL, schema: schemaCompletion })}
			theme={oneDark}
			styles={{ '&': { height: '100%', fontSize: '13px' } }}
		/>
	</div>

	<!-- Results -->
	{#if resultColumns.length > 0}
		<div class="max-h-48 overflow-auto">
			<Table.Root>
				<Table.Header>
					<Table.Row class="hover:bg-transparent">
						{#each resultColumns as col (col)}
							<Table.Head class="h-7 whitespace-nowrap text-xs">{col}</Table.Head>
						{/each}
					</Table.Row>
				</Table.Header>
				<Table.Body>
					{#each resultRows as row, i (i)}
						<Table.Row class="h-7 text-xs">
							{#each row as cell, j (j)}
								<Table.Cell class="py-0.5">
									{#if cell === null}
										<span class="italic text-muted-foreground/40">NULL</span>
									{:else}
										{cell}
									{/if}
								</Table.Cell>
							{/each}
						</Table.Row>
					{/each}
				</Table.Body>
			</Table.Root>
		</div>
	{/if}

	<!-- Status line -->
	{#if durationMs !== null || affectedRows !== null || message}
		<div class="flex items-center gap-3 px-3 py-1 text-xs text-muted-foreground">
			{#if durationMs !== null}
				<span>{durationMs}ms</span>
			{/if}
			{#if resultRows.length > 0}
				<span>{resultRows.length} rows</span>
			{/if}
			{#if affectedRows !== null}
				<span>{affectedRows} rows affected</span>
			{/if}
			{#if message}
				<span>{message}</span>
			{/if}
		</div>
	{/if}
</div>
