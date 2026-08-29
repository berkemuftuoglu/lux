<script lang="ts">
	import { api } from '$lib/api/client';
	import { appState } from '$lib/stores/app.svelte';
	import * as Dialog from '$lib/components/ui/dialog';
	import { Input } from '$lib/components/ui/input';
	import { Label } from '$lib/components/ui/label';
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';
	import { toast } from 'svelte-sonner';
	import { Loader2 } from 'lucide-svelte';

	let { open = $bindable(false) }: { open: boolean } = $props();

	let values = $state<Record<string, string>>({});
	let inserting = $state(false);

	$effect(() => {
		if (open && appState.selectedTable) {
			const v: Record<string, string> = {};
			for (const col of appState.selectedTable.columns) {
				v[col.name] = '';
			}
			values = v;
		}
	});

	function placeholderFor(col: { is_nullable?: boolean; column_default?: string | null }): string {
		if (col.column_default) return `default: ${col.column_default}`;
		return col.is_nullable ? "leave empty for default, or type 'NULL'" : 'required';
	}

	async function insert() {
		if (!appState.selectedTable) return;
		inserting = true;

		const payload: Record<string, string | null> = {};
		for (const col of appState.selectedTable.columns) {
			const v = values[col.name];
			if (v === undefined || v === '') continue;
			if (v.trim().toUpperCase() === 'NULL') {
				payload[col.name] = null;
			} else {
				payload[col.name] = v;
			}
		}

		try {
			await api.insertRow({ table: appState.selectedTable.name, values: payload });
			toast.success('Row inserted');
			await appState.loadTableData();
			open = false;
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Insert failed');
		} finally {
			inserting = false;
		}
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-md">
		<Dialog.Header>
			<Dialog.Title>Insert Row</Dialog.Title>
			<Dialog.Description>
				{appState.selectedTable?.name}
			</Dialog.Description>
		</Dialog.Header>

		<div class="max-h-80 space-y-2 overflow-y-auto py-2">
			{#if appState.selectedTable}
				{#each appState.selectedTable.columns as col (col.name)}
					<div class="grid grid-cols-3 items-center gap-2">
						<div class="flex items-center gap-1">
							<Label class="truncate text-xs">{col.name}</Label>
							<Badge variant="outline" class="h-4 px-1 text-[9px]">{col.type}</Badge>
						</div>
						<Input
							bind:value={values[col.name]}
							class="col-span-2 h-7 text-xs"
							placeholder={placeholderFor(col)}
						/>
					</div>
				{/each}
			{/if}
		</div>

		<Dialog.Footer>
			<Button variant="outline" onclick={() => open = false}>Cancel</Button>
			<Button onclick={insert} disabled={inserting}>
				{#if inserting}
					<Loader2 class="mr-2 h-4 w-4 animate-spin" />
				{/if}
				Insert
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
