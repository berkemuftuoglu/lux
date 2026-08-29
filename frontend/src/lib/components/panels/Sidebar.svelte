<script lang="ts">
	import { appState } from '$lib/stores/app.svelte';
	import { Input } from '$lib/components/ui/input';
	import { Button } from '$lib/components/ui/button';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { Badge } from '$lib/components/ui/badge';
	import { Separator } from '$lib/components/ui/separator';
	import {
		Database,
		Search,
		Table2,
		Key,
		Link,
		ChevronRight,
		ChevronDown,
		Terminal,
		Plug
	} from 'lucide-svelte';

	let { onConnect, onToggleSql }: { onConnect: () => void; onToggleSql: () => void } = $props();
	let expandedTables = $state<Set<string>>(new Set());

	function toggleExpand(tableName: string) {
		const next = new Set(expandedTables);
		if (next.has(tableName)) {
			next.delete(tableName);
		} else {
			next.add(tableName);
		}
		expandedTables = next;
	}

	function selectTable(table: (typeof appState.tables)[0]) {
		appState.selectTable(table);
	}

	function prettyName(name: string): string {
		return name.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
	}
</script>

<div class="flex h-full flex-col bg-sidebar text-sidebar-foreground">
	<!-- Header -->
	<div class="flex items-center gap-2 border-b border-border px-3 py-2">
		<Database class="h-4 w-4 text-primary" />
		<span class="text-sm font-semibold">Lux</span>
		<div class="flex-1"></div>
		<Button variant="ghost" size="icon" class="h-7 w-7" onclick={onToggleSql} title="SQL Editor (Ctrl+L)">
			<Terminal class="h-3.5 w-3.5" />
		</Button>
		<Button variant="ghost" size="icon" class="h-7 w-7" onclick={onConnect} title="Connect">
			<Plug class="h-3.5 w-3.5" />
		</Button>
	</div>

	{#if appState.connectionStatus === 'connected'}
		<!-- Search -->
		<div class="px-3 py-2">
			<div class="relative">
				<Search class="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
				<Input
					type="text"
					placeholder="Search tables..."
					class="h-8 pl-8 text-xs"
					bind:value={appState.tableSearch}
				/>
			</div>
		</div>

		<Separator />

		<!-- Table tree -->
		<ScrollArea class="flex-1">
			<div class="p-2">
				{#each appState.filteredTables as table (table.name)}
					{@const isSelected = appState.selectedTable?.name === table.name}
					{@const isExpanded = expandedTables.has(table.name)}

					<!-- Table row -->
					<div
						class="group flex w-full items-center gap-1.5 rounded-md px-2 py-1 text-left text-sm transition-colors hover:bg-sidebar-accent cursor-pointer {isSelected ? 'bg-sidebar-accent text-sidebar-accent-foreground' : ''}"
						role="button"
						tabindex="0"
						onclick={() => selectTable(table)}
						onkeydown={(e: KeyboardEvent) => { if (e.key === 'Enter') selectTable(table); }}
					>
						<span
							class="flex h-4 w-4 shrink-0 items-center justify-center rounded hover:bg-sidebar-accent cursor-pointer"
							role="button"
							tabindex="0"
							onclick={(e: MouseEvent) => { e.stopPropagation(); toggleExpand(table.name); }}
							onkeydown={(e: KeyboardEvent) => { if (e.key === 'Enter') { e.stopPropagation(); toggleExpand(table.name); } }}
						>
							{#if isExpanded}
								<ChevronDown class="h-3 w-3" />
							{:else}
								<ChevronRight class="h-3 w-3" />
							{/if}
						</span>
						<Table2 class="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
						<span class="flex-1 truncate text-xs">{prettyName(table.name)}</span>
						<Badge variant="secondary" class="h-4 px-1 text-[10px] font-normal">
							{table.columns.length} cols
						</Badge>
					</div>

					<!-- Expanded columns -->
					{#if isExpanded}
						<div class="ml-6 border-l border-border pl-2">
							{#each table.columns as col (col.name)}
								<div class="flex items-center gap-1.5 py-0.5 text-xs text-muted-foreground">
									{#if col.is_primary_key}
										<Key class="h-3 w-3 text-yellow-500" />
									{:else if col.fk_target_table}
										<Link class="h-3 w-3 text-blue-400" />
									{:else}
										<span class="h-3 w-3"></span>
									{/if}
									<span class="truncate">{col.name}</span>
									<span class="ml-auto text-[10px] text-muted-foreground/60">{col.type}</span>
								</div>
							{/each}
						</div>
					{/if}
				{/each}

				{#if appState.filteredTables.length === 0 && appState.tableSearch}
					<p class="px-2 py-4 text-center text-xs text-muted-foreground">No tables found</p>
				{/if}
			</div>
		</ScrollArea>
	{:else}
		<!-- Not connected -->
		<div class="flex flex-1 flex-col items-center justify-center gap-3 px-4">
			<Database class="h-8 w-8 text-muted-foreground/40" />
			<p class="text-center text-xs text-muted-foreground">Connect to a PostgreSQL database to get started</p>
			<Button variant="outline" size="sm" onclick={onConnect}>
				<Plug class="mr-2 h-3.5 w-3.5" />
				Connect
			</Button>
		</div>
	{/if}
</div>
