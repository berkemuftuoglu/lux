<script lang="ts">
	import { appState } from '$lib/stores/app.svelte';
	import { Button } from '$lib/components/ui/button';
	import { Switch } from '$lib/components/ui/switch';
	import { Database, Wifi, WifiOff, Shield, ShieldOff } from 'lucide-svelte';

	const statusColor = $derived(
		appState.connectionStatus === 'connected' ? 'bg-green-500' :
		appState.connectionStatus === 'connecting' ? 'bg-yellow-500' :
		appState.connectionStatus === 'error' ? 'bg-red-500' :
		'bg-muted-foreground/30'
	);

	const statusText = $derived(
		appState.connectionStatus === 'connected' ? 'Connected' :
		appState.connectionStatus === 'connecting' ? 'Connecting...' :
		appState.connectionStatus === 'error' ? 'Connection error' :
		'Disconnected'
	);
</script>

<footer class="flex h-7 items-center gap-3 border-t border-border bg-sidebar px-3 text-xs text-muted-foreground">
	<!-- Connection status -->
	<div class="flex items-center gap-1.5">
		<div class="h-2 w-2 rounded-full {statusColor}"></div>
		<span>{statusText}</span>
	</div>

	{#if appState.latencyMs !== null}
		<span>{appState.latencyMs}ms</span>
	{/if}

	<div class="flex-1"></div>

	{#if appState.connectionStatus === 'connected'}
		<!-- Read-only toggle -->
		<button
			class="flex items-center gap-1.5 rounded px-1.5 py-0.5 transition-colors hover:bg-accent"
			onclick={() => appState.toggleReadOnly()}
		>
			{#if appState.readOnly}
				<Shield class="h-3 w-3 text-yellow-500" />
				<span class="text-yellow-500">Read-only</span>
			{:else}
				<ShieldOff class="h-3 w-3" />
				<span>Read-write</span>
			{/if}
		</button>
	{/if}

	<span>Lux</span>
</footer>
