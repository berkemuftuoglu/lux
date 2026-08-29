<script lang="ts">
	import { api } from '$lib/api/client';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { Button } from '$lib/components/ui/button';
	import { toast } from 'svelte-sonner';
	import { History, Clock, Copy, Play } from 'lucide-svelte';

	let { onLoadQuery }: { onLoadQuery: (sql: string) => void } = $props();

	interface HistoryEntry {
		sql: string;
		timestamp: number;
		duration_ms: number;
		row_count: number | null;
		is_error: boolean;
		error?: string;
	}

	let entries = $state<HistoryEntry[]>([]);
	let loaded = $state(false);

	async function load() {
		try {
			const data = await api.getHistory();
			entries = data.entries;
			loaded = true;
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to load history');
		}
	}

	function formatTime(ts: number): string {
		const d = new Date(ts * 1000);
		return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
	}

	async function copyQuery(sql: string) {
		await navigator.clipboard.writeText(sql);
		toast.success('Copied');
	}

	function rerun(sql: string) {
		onLoadQuery(sql);
	}

	$effect(() => {
		if (!loaded) load();
	});
</script>

<div class="flex flex-col">
	<div class="flex items-center gap-2 border-b border-border px-3 py-1.5">
		<History class="h-3.5 w-3.5 text-muted-foreground" />
		<span class="text-xs font-medium">History</span>
		<div class="flex-1"></div>
		<Button variant="ghost" size="sm" class="h-6 text-[10px]" onclick={load}>Refresh</Button>
	</div>
	<ScrollArea class="max-h-64">
		<div class="p-1">
			{#each entries as entry (`${entry.timestamp}-${entry.sql.slice(0, 32)}`)}
				<div
					class="group flex w-full flex-col gap-0.5 rounded px-2 py-1.5 text-left transition-colors hover:bg-accent cursor-pointer"
					role="button"
					tabindex="0"
					onclick={() => onLoadQuery(entry.sql)}
					onkeydown={(e: KeyboardEvent) => { if (e.key === 'Enter') onLoadQuery(entry.sql); }}
				>
					<code class="line-clamp-2 text-[11px] text-foreground">{entry.sql}</code>
					<div class="flex items-center gap-2 text-[10px] text-muted-foreground">
						<span class="flex items-center gap-1">
							<Clock class="h-2.5 w-2.5" />
							{formatTime(entry.timestamp)}
						</span>
						<span>{entry.duration_ms}ms</span>
						<div class="ml-auto flex items-center gap-1 opacity-0 group-hover:opacity-100">
							<span
								role="button"
								tabindex="0"
								title="Re-run query"
								onclick={(e: MouseEvent) => { e.stopPropagation(); rerun(entry.sql); }}
								onkeydown={(e: KeyboardEvent) => { if (e.key === 'Enter') { e.stopPropagation(); rerun(entry.sql); } }}
							>
								<Play class="h-3 w-3" />
							</span>
							<span
								role="button"
								tabindex="0"
								onclick={(e: MouseEvent) => { e.stopPropagation(); copyQuery(entry.sql); }}
								onkeydown={(e: KeyboardEvent) => { if (e.key === 'Enter') { e.stopPropagation(); copyQuery(entry.sql); } }}
							>
								<Copy class="h-3 w-3" />
							</span>
						</div>
					</div>
				</div>
			{/each}
			{#if entries.length === 0 && loaded}
				<p class="py-4 text-center text-xs text-muted-foreground">No history yet</p>
			{/if}
		</div>
	</ScrollArea>
</div>
