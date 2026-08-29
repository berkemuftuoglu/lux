<script lang="ts">
	import { api } from '$lib/api/client';
	import { appState } from '$lib/stores/app.svelte';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { Button } from '$lib/components/ui/button';
	import { Badge } from '$lib/components/ui/badge';
	import { toast } from 'svelte-sonner';
	import { BookOpen, Undo2 } from 'lucide-svelte';

	interface JournalEntry {
		id: number;
		timestamp: number;
		table: string;
		operation: string;
		column: string;
		old_value: string;
		new_value: string;
		pk_column: string;
		pk_value: string;
		undone: boolean;
	}

	let entries = $state<JournalEntry[]>([]);
	let loaded = $state(false);

	async function load() {
		try {
			const data = await api.getJournal();
			entries = data.entries;
			loaded = true;
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to load journal');
		}
	}

	async function undo(entry: JournalEntry) {
		if (entry.undone) {
			toast.error('Already discarded');
			return;
		}
		try {
			await api.undoJournal(entry.id);
			toast.success('Discarded change');
			await load();
			if (appState.selectedTable && appState.selectedTable.name === entry.table) {
				await appState.loadTableData();
			}
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Discard failed');
		}
	}

	function opColor(op: string): 'default' | 'secondary' | 'destructive' {
		if (op === 'delete') return 'destructive';
		if (op === 'insert') return 'default';
		return 'secondary';
	}

	$effect(() => {
		if (!loaded) load();
	});
</script>

<div class="flex flex-col">
	<div class="flex items-center gap-2 border-b border-border px-3 py-1.5">
		<BookOpen class="h-3.5 w-3.5 text-muted-foreground" />
		<span class="text-xs font-medium">Pending Changes</span>
		<span class="text-[10px] text-muted-foreground">{entries.length} changes</span>
		<div class="flex-1"></div>
		<Button variant="ghost" size="sm" class="h-6 text-[10px]" onclick={load}>Refresh</Button>
	</div>
	<ScrollArea class="max-h-64">
		<div class="p-1">
			{#each entries as entry (entry.id)}
				<div class="group flex items-start gap-2 rounded px-2 py-1.5 hover:bg-accent {entry.undone ? 'opacity-40' : ''}">
					<div class="flex flex-1 flex-col gap-0.5">
						<div class="flex items-center gap-1.5">
							<Badge variant={opColor(entry.operation)} class="h-4 px-1 text-[10px]">
								{entry.operation}
							</Badge>
							<span class="text-[11px] text-foreground">{entry.table}.{entry.column}</span>
						</div>
						{#if entry.operation === 'update'}
							<div class="flex items-center gap-1 text-[10px] text-muted-foreground">
								<span class="line-through">{entry.old_value || 'NULL'}</span>
								<span>&rarr;</span>
								<span class="text-foreground">{entry.new_value || 'NULL'}</span>
							</div>
						{/if}
					</div>
					<Button
						variant="ghost"
						size="icon"
						class="h-6 w-6 opacity-0 group-hover:opacity-100"
						onclick={() => undo(entry)}
						title="Discard this pending change (reverts the cell to its pre-edit value)"
						aria-label="Discard pending change"
					>
						<Undo2 class="h-3 w-3" />
					</Button>
				</div>
			{/each}
			{#if entries.length === 0 && loaded}
				<p class="py-4 text-center text-xs text-muted-foreground">No changes recorded</p>
			{/if}
		</div>
	</ScrollArea>
</div>
