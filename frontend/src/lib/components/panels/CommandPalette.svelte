<script lang="ts">
	import { appState } from '$lib/stores/app.svelte';
	import * as Command from '$lib/components/ui/command';
	import * as Dialog from '$lib/components/ui/dialog';
	import { Table2, Terminal, Database, Settings, Shield } from 'lucide-svelte';

	let { open = $bindable(false) }: { open: boolean } = $props();

	function selectTable(table: (typeof appState.tables)[0]) {
		appState.selectTable(table);
		open = false;
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="overflow-hidden p-0 shadow-lg sm:max-w-lg">
		<Command.Root>
			<Command.Input placeholder="Search tables, columns, actions..." />
			<Command.List>
				<Command.Empty>No results found.</Command.Empty>

				{#if appState.tables.length > 0}
					<Command.Group heading="Tables">
						{#each appState.tables as table (table.name)}
							<Command.Item onSelect={() => selectTable(table)}>
								<Table2 class="mr-2 h-4 w-4" />
								<span>{table.name}</span>
								<span class="ml-auto text-xs text-muted-foreground">{table.columns.length} cols</span>
							</Command.Item>
						{/each}
					</Command.Group>
				{/if}

				<Command.Group heading="Actions">
					<Command.Item onSelect={() => { appState.toggleReadOnly(); open = false; }}>
						<Shield class="mr-2 h-4 w-4" />
						<span>Toggle read-only mode</span>
					</Command.Item>
				</Command.Group>
			</Command.List>
		</Command.Root>
	</Dialog.Content>
</Dialog.Root>
