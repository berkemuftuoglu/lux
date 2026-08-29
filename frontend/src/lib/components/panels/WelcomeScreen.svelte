<script lang="ts">
	import { appState } from '$lib/stores/app.svelte';
	import { Button } from '$lib/components/ui/button';
	import { Database, Plug, Terminal, Keyboard } from 'lucide-svelte';

	let { onConnect }: { onConnect: () => void } = $props();
</script>

<div class="flex flex-1 flex-col items-center justify-center gap-6 bg-background">
	<div class="flex flex-col items-center gap-3">
		<div class="flex h-16 w-16 items-center justify-center rounded-2xl bg-primary/10">
			<Database class="h-8 w-8 text-primary" />
		</div>
		<h1 class="text-2xl font-semibold tracking-tight">Lux</h1>
		<p class="max-w-sm text-center text-sm text-muted-foreground">
			A lightweight, local-first PostgreSQL client. Connect to get started.
		</p>
	</div>

	{#if appState.connectionStatus !== 'connected'}
		<Button onclick={onConnect} size="lg" class="gap-2">
			<Plug class="h-4 w-4" />
			Connect to Database
		</Button>
	{:else}
		<p class="text-sm text-muted-foreground">Select a table from the sidebar to begin</p>
	{/if}

	<div class="mt-8 grid grid-cols-3 gap-6 text-center text-xs text-muted-foreground">
		<div class="flex flex-col items-center gap-1.5">
			<Keyboard class="h-4 w-4" />
			<span>Ctrl+K</span>
			<span>Command palette</span>
		</div>
		<div class="flex flex-col items-center gap-1.5">
			<Terminal class="h-4 w-4" />
			<span>Ctrl+L</span>
			<span>SQL editor</span>
		</div>
		<div class="flex flex-col items-center gap-1.5">
			<Plug class="h-4 w-4" />
			<span>Ctrl+N</span>
			<span>New connection</span>
		</div>
	</div>
</div>
