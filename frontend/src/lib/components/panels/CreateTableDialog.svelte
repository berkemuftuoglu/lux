<script lang="ts">
	import { api } from '$lib/api/client';
	import { appState } from '$lib/stores/app.svelte';
	import * as Dialog from '$lib/components/ui/dialog';
	import { Input } from '$lib/components/ui/input';
	import { Label } from '$lib/components/ui/label';
	import { Button } from '$lib/components/ui/button';
	import * as Select from '$lib/components/ui/select';
	import { Switch } from '$lib/components/ui/switch';
	import { toast } from 'svelte-sonner';
	import { Plus, Trash2 } from 'lucide-svelte';

	let { open = $bindable(false) }: { open: boolean } = $props();

	interface ColumnDef {
		name: string;
		type: string;
		primaryKey: boolean;
		notNull: boolean;
	}

	let tableName = $state('');
	let columns = $state<ColumnDef[]>([
		{ name: 'id', type: 'SERIAL', primaryKey: true, notNull: true }
	]);

	const pgTypes = [
		'SERIAL', 'BIGSERIAL', 'INTEGER', 'BIGINT', 'SMALLINT',
		'TEXT', 'VARCHAR(255)', 'CHAR(1)',
		'BOOLEAN',
		'TIMESTAMP', 'TIMESTAMPTZ', 'DATE', 'TIME',
		'NUMERIC', 'DECIMAL', 'REAL', 'DOUBLE PRECISION',
		'UUID', 'JSONB', 'JSON', 'BYTEA'
	];

	function addColumn() {
		columns = [...columns, { name: '', type: 'TEXT', primaryKey: false, notNull: false }];
	}

	function removeColumn(index: number) {
		columns = columns.filter((_, i) => i !== index);
	}

	async function createTable() {
		if (!tableName.trim()) {
			toast.error('Table name is required');
			return;
		}
		if (columns.length === 0) {
			toast.error('Add at least one column');
			return;
		}
		if (columns.some(c => !c.name.trim())) {
			toast.error('All columns must have names');
			return;
		}

		const colDefs = columns.map(c => {
			let def = `"${c.name}" ${c.type}`;
			if (c.primaryKey) def += ' PRIMARY KEY';
			if (c.notNull && !c.primaryKey) def += ' NOT NULL';
			return def;
		}).join(', ');

		const sql = `CREATE TABLE "${tableName}" (${colDefs})`;

		try {
			await api.executeSql(sql);
			toast.success(`Table "${tableName}" created`);
			await appState.loadSchema();
			open = false;
			tableName = '';
			columns = [{ name: 'id', type: 'SERIAL', primaryKey: true, notNull: true }];
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Create failed');
		}
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-lg">
		<Dialog.Header>
			<Dialog.Title>Create Table</Dialog.Title>
		</Dialog.Header>

		<div class="space-y-4 py-2">
			<div>
				<Label class="text-xs">Table Name</Label>
				<Input bind:value={tableName} class="mt-1 h-8 text-sm" placeholder="my_table" />
			</div>

			<div>
				<div class="mb-2 flex items-center justify-between">
					<Label class="text-xs">Columns</Label>
					<Button variant="ghost" size="sm" class="h-6 gap-1 text-[10px]" onclick={addColumn}>
						<Plus class="h-3 w-3" />
						Add
					</Button>
				</div>

				<div class="space-y-2">
					{#each columns as col, i (i)}
						<div class="flex items-center gap-2">
							<Input
								bind:value={col.name}
								class="h-7 flex-1 text-xs"
								placeholder="column_name"
							/>
							<select
								bind:value={col.type}
								class="h-7 rounded-md border border-input bg-background px-2 text-xs"
							>
								{#each pgTypes as t (t)}
									<option value={t}>{t}</option>
								{/each}
							</select>
							<label class="flex items-center gap-1 text-[10px] text-muted-foreground">
								<input type="checkbox" bind:checked={col.primaryKey} class="h-3 w-3" />
								PK
							</label>
							<label class="flex items-center gap-1 text-[10px] text-muted-foreground">
								<input type="checkbox" bind:checked={col.notNull} class="h-3 w-3" />
								NN
							</label>
							{#if columns.length > 1}
								<Button variant="ghost" size="icon" class="h-6 w-6" onclick={() => removeColumn(i)}>
									<Trash2 class="h-3 w-3 text-destructive" />
								</Button>
							{/if}
						</div>
					{/each}
				</div>
			</div>
		</div>

		<Dialog.Footer>
			<Button variant="outline" onclick={() => open = false}>Cancel</Button>
			<Button onclick={createTable}>Create Table</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
