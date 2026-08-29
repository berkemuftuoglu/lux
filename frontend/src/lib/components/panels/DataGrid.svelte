<script lang="ts">
	import { cellClass as formatCellClass } from '$lib/format';
	import { appState } from '$lib/stores/app.svelte';
	import { api } from '$lib/api/client';
	import * as Table from '$lib/components/ui/table';
	import * as ContextMenu from '$lib/components/ui/context-menu';
	import { Button } from '$lib/components/ui/button';
	import { Input } from '$lib/components/ui/input';
	import { Badge } from '$lib/components/ui/badge';
	import { toast } from 'svelte-sonner';
	import {
		ArrowUp,
		ArrowDown,
		ChevronLeft,
		ChevronRight,
		Loader2,
		Copy,
		Trash2,
		Pencil,
		Search
	} from 'lucide-svelte';

	let editingCell = $state<{ row: number; col: number } | null>(null);
	let editValue = $state('');
	let filters = $state<Record<string, string>>({});

	function getPkColumn(): string | null {
		if (!appState.selectedTable) return null;
		const pk = appState.selectedTable.columns.find(c => c.is_primary_key);
		return pk?.name ?? null;
	}

	function getPkValue(rowIndex: number): string | null {
		const pkCol = getPkColumn();
		if (!pkCol) return null;
		const colIndex = appState.tableColumns.indexOf(pkCol);
		if (colIndex === -1) return null;
		return appState.tableRows[rowIndex]?.[colIndex] ?? null;
	}

	function startEdit(row: number, col: number) {
		if (appState.readOnly) {
			toast.error('Read-only mode is enabled');
			return;
		}
		editingCell = { row, col };
		editValue = appState.tableRows[row][col] ?? '';
	}

	async function saveEdit() {
		if (!editingCell || !appState.selectedTable) return;
		const { row, col } = editingCell;
		const column = appState.tableColumns[col];
		const pkCol = getPkColumn();
		const pkVal = getPkValue(row);

		if (!pkCol || pkVal === null) {
			toast.error('Cannot edit: no primary key');
			editingCell = null;
			return;
		}

		const oldValue = appState.tableRows[row][col];
		const newValue = editValue === '' ? null : editValue;

		if (newValue === oldValue) {
			editingCell = null;
			return;
		}

		// Close UI immediately — user sees the input disappear before the await
		editingCell = null;

		try {
			await api.updateCell({
				table: appState.selectedTable.name,
				column,
				value: newValue,
				pk_column: pkCol,
				pk_value: String(pkVal),
				old_value: oldValue ?? undefined
			});
			await appState.loadTableData();
			toast.success('Cell updated');
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Update failed');
		}
	}

	function cancelEdit() {
		editingCell = null;
	}

	function handleEditKeydown(e: KeyboardEvent) {
		if (e.key === 'Enter') saveEdit();
		if (e.key === 'Escape') cancelEdit();
		if (e.key === 'Tab') {
			e.preventDefault();
			saveEdit();
		}
	}

	async function deleteRow(rowIndex: number) {
		if (!appState.selectedTable) return;
		const pkCol = getPkColumn();
		const pkVal = getPkValue(rowIndex);
		if (!pkCol || pkVal === null) {
			toast.error('Cannot delete: no primary key');
			return;
		}
		if (!confirm(`Delete row where ${pkCol} = ${pkVal}?`)) return;
		try {
			await api.deleteRow({
				table: appState.selectedTable.name,
				pk_column: pkCol,
				pk_value: String(pkVal)
			});
			await appState.loadTableData();
			toast.success('Row deleted');
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Delete failed');
		}
	}

	async function copyValue(value: string | null) {
		await navigator.clipboard.writeText(value ?? 'NULL');
		toast.success('Copied');
	}

	async function copyRowJson(rowIndex: number) {
		const obj: Record<string, string | null> = {};
		appState.tableColumns.forEach((col, i) => {
			obj[col] = appState.tableRows[rowIndex][i];
		});
		await navigator.clipboard.writeText(JSON.stringify(obj, null, 2));
		toast.success('Row copied as JSON');
	}

	async function setNull(rowIndex: number, colIndex: number) {
		if (!appState.selectedTable) return;
		const column = appState.tableColumns[colIndex];
		const pkCol = getPkColumn();
		const pkVal = getPkValue(rowIndex);
		if (!pkCol || pkVal === null) return;
		const oldValue = appState.tableRows[rowIndex][colIndex];
		try {
			await api.updateCell({
				table: appState.selectedTable.name,
				column,
				value: null,
				pk_column: pkCol,
				pk_value: String(pkVal),
				old_value: oldValue ?? undefined
			});
			await appState.loadTableData();
			toast.success('Set to NULL');
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed');
		}
	}

	async function findReplaceInColumn(colIndex: number) {
		if (!appState.selectedTable) return;
		if (appState.readOnly) {
			toast.error('Read-only mode is enabled');
			return;
		}
		const column = appState.tableColumns[colIndex];
		const find = prompt(`Find in column "${column}":`);
		if (find === null || find === '') return;
		const replace = prompt('Replace with:');
		if (replace === null) return;

		try {
			const preview = await api.bulkUpdate(appState.selectedTable.name, {
				column,
				find,
				replace,
				force: false
			});
			if (!preview.affected_rows && preview.affected_rows !== 0) return;
			if (!confirm(`Update ${preview.affected_rows} row(s)?`)) return;
			await api.bulkUpdate(appState.selectedTable.name, {
				column,
				find,
				replace,
				force: true
			});
			await appState.loadTableData();
			toast.success(`Updated ${preview.affected_rows} row(s)`);
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Bulk update failed');
		}
	}

	function cellClass(value: string | null, colName: string): string {
		return formatCellClass(value, colName, appState.selectedTable?.columns);
	}

	$effect(() => {
		// Reset filters when table changes
		if (appState.selectedTable) {
			filters = {};
		}
	});

	const pageStart = $derived(appState.currentOffset + 1);
	const pageEnd = $derived(Math.min(appState.currentOffset + appState.tableRows.length, appState.totalRows));
</script>

<div class="flex flex-1 flex-col overflow-hidden">
	<!-- Toolbar -->
	<div class="flex items-center gap-2 border-b border-border px-3 py-1.5">
		<Badge variant="outline" class="text-xs font-normal">
			{appState.selectedTable?.name}
		</Badge>
		<span class="text-xs text-muted-foreground">
			{appState.totalRows.toLocaleString()} rows
		</span>
		<div class="flex-1"></div>

		{#if appState.loading}
			<Loader2 class="h-3.5 w-3.5 animate-spin text-muted-foreground" />
		{/if}

		<!-- Pagination -->
		<div class="flex items-center gap-1">
			<span class="text-xs text-muted-foreground">
				{pageStart}-{pageEnd} of {appState.totalRows.toLocaleString()}
			</span>
			<Button
				variant="ghost"
				size="icon"
				class="h-7 w-7"
				disabled={appState.currentOffset === 0}
				onclick={() => appState.prevPage()}
			>
				<ChevronLeft class="h-3.5 w-3.5" />
			</Button>
			<Button
				variant="ghost"
				size="icon"
				class="h-7 w-7"
				disabled={!appState.hasMore}
				onclick={() => appState.nextPage()}
			>
				<ChevronRight class="h-3.5 w-3.5" />
			</Button>
		</div>
	</div>

	<!-- Grid -->
	<div class="flex-1 overflow-auto">
		<Table.Root>
			<Table.Header>
				<Table.Row class="hover:bg-transparent">
					{#each appState.tableColumns as col, i (col)}
						<Table.Head
							class="h-8 cursor-pointer select-none whitespace-nowrap text-xs font-medium hover:bg-accent"
							onclick={() => appState.sort(col)}
						>
							<span class="flex items-center gap-1">
								{col}
								{#if appState.sortColumn === col}
									{#if appState.sortDirection === 'asc'}
										<ArrowUp class="h-3 w-3" />
									{:else}
										<ArrowDown class="h-3 w-3" />
									{/if}
								{/if}
							</span>
						</Table.Head>
					{/each}
				</Table.Row>
			</Table.Header>
			<Table.Body>
				{#each appState.tableRows as row, rowIndex (rowIndex)}
					<Table.Row class="h-8 text-xs hover:bg-muted/50">
						{#each row as cell, colIndex (colIndex)}
							<ContextMenu.Root>
								<ContextMenu.Trigger>
									{#snippet child({ props })}
										<Table.Cell
											{...props}
											class="max-w-[300px] truncate py-1 {cellClass(cell, appState.tableColumns[colIndex])}"
											ondblclick={() => startEdit(rowIndex, colIndex)}
										>
											{#if editingCell?.row === rowIndex && editingCell?.col === colIndex}
												<Input
													class="h-6 rounded-none border-primary text-xs"
													bind:value={editValue}
													onblur={saveEdit}
													onkeydown={handleEditKeydown}
													autofocus
												/>
											{:else if cell === null}
												<span class="italic text-muted-foreground/40">NULL</span>
											{:else}
												{cell}
											{/if}
										</Table.Cell>
									{/snippet}
								</ContextMenu.Trigger>
								<ContextMenu.Content>
									<ContextMenu.Item onclick={() => copyValue(cell)}>
										<Copy class="mr-2 h-3.5 w-3.5" />
										Copy value
									</ContextMenu.Item>
									<ContextMenu.Item onclick={() => copyRowJson(rowIndex)}>
										<Copy class="mr-2 h-3.5 w-3.5" />
										Copy row as JSON
									</ContextMenu.Item>
									<ContextMenu.Separator />
									<ContextMenu.Item onclick={() => startEdit(rowIndex, colIndex)}>
										<Pencil class="mr-2 h-3.5 w-3.5" />
										Edit cell
									</ContextMenu.Item>
									{#if !appState.readOnly}
										<ContextMenu.Item onclick={() => setNull(rowIndex, colIndex)}>
											Set to NULL
										</ContextMenu.Item>
										<ContextMenu.Item onclick={() => findReplaceInColumn(colIndex)}>
											<Search class="mr-2 h-3.5 w-3.5" />
											Find and replace in this column
										</ContextMenu.Item>
										<ContextMenu.Separator />
										<ContextMenu.Item class="text-destructive" onclick={() => deleteRow(rowIndex)}>
											<Trash2 class="mr-2 h-3.5 w-3.5" />
											Delete row
										</ContextMenu.Item>
									{/if}
								</ContextMenu.Content>
							</ContextMenu.Root>
						{/each}
					</Table.Row>
				{/each}

				{#if appState.tableRows.length === 0 && !appState.loading}
					<Table.Row>
						<Table.Cell colspan={appState.tableColumns.length} class="h-24 text-center text-sm text-muted-foreground">
							No data
						</Table.Cell>
					</Table.Row>
				{/if}
			</Table.Body>
		</Table.Root>
	</div>
</div>
