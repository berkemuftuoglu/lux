<script lang="ts">
	import { api } from '$lib/api/client';
	import { appState } from '$lib/stores/app.svelte';
	import * as Dialog from '$lib/components/ui/dialog';
	import { Input } from '$lib/components/ui/input';
	import { Label } from '$lib/components/ui/label';
	import { Button } from '$lib/components/ui/button';
	import { Tabs, TabsContent, TabsList, TabsTrigger } from '$lib/components/ui/tabs';
	import { Loader2 } from 'lucide-svelte';
	import { toast } from 'svelte-sonner';

	let { open = $bindable(false) }: { open: boolean } = $props();

	let host = $state('localhost');
	let port = $state('5432');
	let database = $state('');
	let user = $state('postgres');
	let password = $state('');
	let connectionString = $state('');
	let connectionName = $state('');
	let selectedColor = $state('#3b82f6');
	let mode = $state('fields');

	const colorPalette = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#a855f7', '#06b6d4'];

	async function handleConnect() {
		if (mode === 'fields') {
			await appState.connect({ host, port, database, user, password });
		} else {
			await appState.connectRaw(connectionString);
		}
		if (appState.connectionStatus === 'connected') {
			open = false;
		}
	}

	async function saveAndConnect() {
		if (!connectionName.trim()) {
			toast.error('Connection name is required');
			return;
		}
		try {
			await api.saveConnection({
				name: connectionName.trim(),
				color: selectedColor,
				host,
				port,
				database,
				user,
				password
			});
			toast.success('Connection saved');
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Save failed');
			return;
		}
		await handleConnect();
	}

	function handleKeydown(e: KeyboardEvent) {
		if (e.key === 'Enter') {
			handleConnect();
		}
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-md">
		<Dialog.Header>
			<Dialog.Title>Connect to PostgreSQL</Dialog.Title>
			<Dialog.Description>Enter your database connection details.</Dialog.Description>
		</Dialog.Header>

		<Tabs bind:value={mode}>
			<TabsList class="w-full">
				<TabsTrigger value="fields" class="flex-1">Fields</TabsTrigger>
				<TabsTrigger value="string" class="flex-1">Connection String</TabsTrigger>
			</TabsList>

			<TabsContent value="fields">
				<div class="grid gap-3 py-4" role="form" aria-label="Connection fields" onkeydown={handleKeydown}>
					<div class="grid grid-cols-4 items-center gap-3">
						<Label class="text-right text-xs">Host</Label>
						<Input bind:value={host} class="col-span-3 h-8 text-sm" placeholder="localhost" />
					</div>
					<div class="grid grid-cols-4 items-center gap-3">
						<Label class="text-right text-xs">Port</Label>
						<Input bind:value={port} class="col-span-3 h-8 text-sm" placeholder="5432" />
					</div>
					<div class="grid grid-cols-4 items-center gap-3">
						<Label class="text-right text-xs">Database</Label>
						<Input bind:value={database} class="col-span-3 h-8 text-sm" placeholder="mydb" />
					</div>
					<div class="grid grid-cols-4 items-center gap-3">
						<Label class="text-right text-xs">User</Label>
						<Input bind:value={user} class="col-span-3 h-8 text-sm" placeholder="postgres" />
					</div>
					<div class="grid grid-cols-4 items-center gap-3">
						<Label class="text-right text-xs">Password</Label>
						<Input type="password" bind:value={password} class="col-span-3 h-8 text-sm" />
					</div>
					<div class="grid grid-cols-4 items-center gap-3">
						<Label class="text-right text-xs">Name</Label>
						<Input bind:value={connectionName} class="col-span-3 h-8 text-sm" placeholder="My DB (optional — to save)" />
					</div>
					<div class="grid grid-cols-4 items-center gap-3">
						<Label class="text-right text-xs">Color</Label>
						<div class="col-span-3 flex gap-1.5">
							{#each colorPalette as color (color)}
								<button
									class="h-5 w-5 rounded-full border-2 transition-all {selectedColor === color ? 'border-foreground scale-110' : 'border-transparent'}"
									style="background-color: {color}"
									onclick={() => selectedColor = color}
									aria-label="Select color {color}"
								></button>
							{/each}
						</div>
					</div>
				</div>
			</TabsContent>

			<TabsContent value="string">
				<div class="py-4" role="form" aria-label="Connection string" onkeydown={handleKeydown}>
					<Label class="text-xs">Connection String</Label>
					<Input
						bind:value={connectionString}
						class="mt-1.5 h-8 text-sm font-mono"
						placeholder="postgresql://user:pass@host:5432/db"
					/>
				</div>
			</TabsContent>
		</Tabs>

		<Dialog.Footer>
			<Button variant="outline" onclick={() => open = false}>Cancel</Button>
			{#if mode === 'fields'}
				<Button
					variant="outline"
					onclick={saveAndConnect}
					disabled={appState.connectionStatus === 'connecting'}
				>
					Save and Connect
				</Button>
			{/if}
			<Button
				onclick={handleConnect}
				disabled={appState.connectionStatus === 'connecting'}
			>
				{#if appState.connectionStatus === 'connecting'}
					<Loader2 class="mr-2 h-4 w-4 animate-spin" />
					Connecting...
				{:else}
					Connect
				{/if}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
