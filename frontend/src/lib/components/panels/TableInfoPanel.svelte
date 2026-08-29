<script lang="ts">
	import { api } from '$lib/api/client';
	import { appState } from '$lib/stores/app.svelte';
	import { Button } from '$lib/components/ui/button';
	import { Separator } from '$lib/components/ui/separator';
	import { toast } from 'svelte-sonner';
	import { Info, Copy, HardDrive } from 'lucide-svelte';

	let ddl = $state('');
	let stats = $state<{ total_size: string; table_size: string; index_size: string; row_estimate: number } | null>(null);
	let loadedFor = $state('');

	async function load() {
		if (!appState.selectedTable) return;
		const key = appState.selectedTable.name;
		if (loadedFor === key) return;

		try {
			const [ddlResult, statsResult] = await Promise.all([
				api.getTableDdl(appState.selectedTable.name),
				api.getTableStats(appState.selectedTable.name)
			]);
			ddl = ddlResult.ddl;
			stats = statsResult;
			loadedFor = key;
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to load table info');
		}
	}

	async function copyDdl() {
		await navigator.clipboard.writeText(ddl);
		toast.success('DDL copied');
	}

	$effect(() => {
		if (appState.selectedTable) load();
	});
</script>

<div class="flex flex-col gap-3 p-3">
	{#if stats}
		<div class="grid grid-cols-2 gap-2 text-xs">
			<div class="flex items-center gap-1.5 text-muted-foreground">
				<HardDrive class="h-3 w-3" />
				Total size
			</div>
			<span class="text-right font-mono">{stats.total_size}</span>

			<span class="text-muted-foreground">Table size</span>
			<span class="text-right font-mono">{stats.table_size}</span>

			<span class="text-muted-foreground">Index size</span>
			<span class="text-right font-mono">{stats.index_size}</span>

			<span class="text-muted-foreground">Row estimate</span>
			<span class="text-right font-mono">{stats.row_estimate.toLocaleString()}</span>
		</div>
	{/if}

	{#if ddl}
		<Separator />
		<div>
			<div class="flex items-center justify-between">
				<span class="text-xs font-medium text-muted-foreground">DDL</span>
				<Button variant="ghost" size="sm" class="h-6 gap-1 text-[10px]" onclick={copyDdl}>
					<Copy class="h-3 w-3" />
					Copy
				</Button>
			</div>
			<pre class="mt-1 max-h-48 overflow-auto rounded bg-muted p-2 text-[11px]">{ddl}</pre>
		</div>
	{/if}
</div>
