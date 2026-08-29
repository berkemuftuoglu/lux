<script lang="ts">
	import { api, parseConnectionString } from '$lib/api/client';
	import { appState } from '$lib/stores/app.svelte';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { Button } from '$lib/components/ui/button';
	import { toast } from 'svelte-sonner';
	import { Database, Trash2 } from 'lucide-svelte';

	interface SavedConnectionRaw {
		id: number;
		name: string;
		color: string;
		conninfo: string;
	}

	interface SavedConnectionView extends SavedConnectionRaw {
		host: string;
		port: string;
		database: string;
		user: string;
	}

	let connections = $state<SavedConnectionView[]>([]);
	let loaded = $state(false);

	async function load() {
		try {
			const data = await api.getConnections();
			connections = data.connections.map((c) => ({ ...c, ...parseConnectionString(c.conninfo) }));
			loaded = true;
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to load connections');
		}
	}

	async function connectTo(conn: SavedConnectionView) {
		appState.setConnectionColor(conn.color || '#3b82f6');
		await appState.connectRaw(conn.conninfo);
	}

	async function remove(id: number) {
		try {
			await api.deleteConnection(id);
			connections = connections.filter((c) => c.id !== id);
			toast.success('Connection deleted');
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Delete failed');
		}
	}

	$effect(() => {
		if (!loaded) load();
	});
</script>

<div class="flex flex-col">
	<div class="flex items-center gap-2 border-b border-border px-3 py-1.5">
		<Database class="h-3.5 w-3.5 text-muted-foreground" />
		<span class="text-xs font-medium">Saved Connections</span>
	</div>
	<ScrollArea class="max-h-48">
		<div class="p-1">
			{#each connections as conn (conn.id)}
				<div class="group flex items-center gap-2 rounded px-2 py-1.5 hover:bg-accent">
					<div
						class="h-3 w-3 shrink-0 rounded-full"
						style="background-color: {conn.color || '#3b82f6'}"
					></div>
					<button
						class="flex flex-1 flex-col text-left"
						onclick={() => connectTo(conn)}
					>
						<span class="text-xs font-medium">{conn.name}</span>
						<span class="text-[10px] text-muted-foreground">
							{conn.host}:{conn.port}/{conn.database}
						</span>
					</button>
					<Button
						variant="ghost"
						size="icon"
						class="h-6 w-6 opacity-0 group-hover:opacity-100"
						onclick={() => remove(conn.id)}
					>
						<Trash2 class="h-3 w-3 text-destructive" />
					</Button>
				</div>
			{/each}
			{#if connections.length === 0 && loaded}
				<p class="py-4 text-center text-xs text-muted-foreground">No saved connections</p>
			{/if}
		</div>
	</ScrollArea>
</div>
