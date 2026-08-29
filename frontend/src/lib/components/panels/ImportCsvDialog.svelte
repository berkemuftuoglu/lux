<script lang="ts">
	import { api } from '$lib/api/client';
	import { appState } from '$lib/stores/app.svelte';
	import * as Dialog from '$lib/components/ui/dialog';
	import { Button } from '$lib/components/ui/button';
	import { Label } from '$lib/components/ui/label';
	import { Switch } from '$lib/components/ui/switch';
	import { toast } from 'svelte-sonner';
	import { Upload, Loader2 } from 'lucide-svelte';

	let { open = $bindable(false) }: { open: boolean } = $props();

	let csvContent = $state('');
	let fileName = $state('');
	let importing = $state(false);
	let hasHeader = $state(true);

	function handleFile(e: Event) {
		const input = e.target as HTMLInputElement;
		const file = input.files?.[0];
		if (!file) return;
		fileName = file.name;
		const reader = new FileReader();
		reader.onload = () => {
			csvContent = reader.result as string;
		};
		reader.readAsText(file);
	}

	async function doImport() {
		if (!csvContent.trim() || !appState.selectedTable) return;
		importing = true;
		try {
			const result = await api.importCsv(
				appState.selectedTable.name,
				csvContent,
				hasHeader
			);
			toast.success(`Imported ${result.imported} rows`);
			await appState.loadTableData();
			open = false;
			csvContent = '';
			fileName = '';
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Import failed');
		} finally {
			importing = false;
		}
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-md">
		<Dialog.Header>
			<Dialog.Title>Import CSV</Dialog.Title>
			<Dialog.Description>
				Import CSV data into {appState.selectedTable?.name}
			</Dialog.Description>
		</Dialog.Header>

		<div class="space-y-4 py-2">
			<div>
				<Label class="text-xs">CSV File</Label>
				<label class="mt-1 flex cursor-pointer items-center justify-center gap-2 rounded-md border-2 border-dashed border-border px-4 py-6 transition-colors hover:border-primary/50">
					<Upload class="h-5 w-5 text-muted-foreground" />
					<span class="text-sm text-muted-foreground">
						{fileName || 'Click to select a CSV file'}
					</span>
					<input type="file" accept=".csv" class="hidden" onchange={handleFile} />
				</label>
			</div>

			<div class="flex items-center gap-2">
				<Switch bind:checked={hasHeader} id="has-header" />
				<Label for="has-header" class="text-xs">First row is header</Label>
			</div>

			{#if csvContent}
				<div>
					<Label class="text-xs">Preview (first 5 lines)</Label>
					<pre class="mt-1 max-h-32 overflow-auto rounded bg-muted p-2 text-[11px]">{csvContent.split('\n').slice(0, 5).join('\n')}</pre>
				</div>
			{/if}
		</div>

		<Dialog.Footer>
			<Button variant="outline" onclick={() => open = false}>Cancel</Button>
			<Button onclick={doImport} disabled={!csvContent.trim() || importing}>
				{#if importing}
					<Loader2 class="mr-2 h-4 w-4 animate-spin" />
					Importing...
				{:else}
					Import
				{/if}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
