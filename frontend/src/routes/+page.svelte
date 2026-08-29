<script lang="ts">
	import { appState } from '$lib/stores/app.svelte';
	import Sidebar from '$lib/components/panels/Sidebar.svelte';
	import DataGrid from '$lib/components/panels/DataGrid.svelte';
	import SqlEditor from '$lib/components/panels/SqlEditor.svelte';
	import ConnectDialog from '$lib/components/panels/ConnectDialog.svelte';
	import StatusBar from '$lib/components/panels/StatusBar.svelte';
	import CommandPalette from '$lib/components/panels/CommandPalette.svelte';
	import WelcomeScreen from '$lib/components/panels/WelcomeScreen.svelte';
	import HistoryPanel from '$lib/components/panels/HistoryPanel.svelte';
	import JournalPanel from '$lib/components/panels/JournalPanel.svelte';
	import SavedConnections from '$lib/components/panels/SavedConnections.svelte';
	import CreateTableDialog from '$lib/components/panels/CreateTableDialog.svelte';
	import ImportCsvDialog from '$lib/components/panels/ImportCsvDialog.svelte';
	import InsertRowDialog from '$lib/components/panels/InsertRowDialog.svelte';
	import TableInfoPanel from '$lib/components/panels/TableInfoPanel.svelte';
	import { Button } from '$lib/components/ui/button';
	import { Separator } from '$lib/components/ui/separator';
	import * as Tooltip from '$lib/components/ui/tooltip';
	import {
		Table2,
		Terminal,
		History,
		BookOpen,
		Database,
		Info,
		Plus,
		Upload,
		Download,
		Trash2
	} from 'lucide-svelte';
	import { toast } from 'svelte-sonner';
	import { api } from '$lib/api/client';

	let showConnect = $state(false);
	let showCommandPalette = $state(false);
	let showSql = $state(false);
	let showCreateTable = $state(false);
	let showImportCsv = $state(false);
	let showInsertRow = $state(false);

	type SidePanel = 'tables' | 'history' | 'journal' | 'connections' | 'info';
	let activePanel = $state<SidePanel>('tables');

	let sqlEditorValue = $state('');

	function handleKeydown(e: KeyboardEvent) {
		if (e.key === 'k' && (e.metaKey || e.ctrlKey)) {
			e.preventDefault();
			showCommandPalette = !showCommandPalette;
		}
		if (e.key === 'l' && (e.metaKey || e.ctrlKey)) {
			e.preventDefault();
			showSql = true;
		}
		if (e.key === 'n' && (e.metaKey || e.ctrlKey)) {
			e.preventDefault();
			showConnect = true;
		}
	}

	function loadQueryIntoEditor(sql: string) {
		sqlEditorValue = sql;
		showSql = true;
	}

	async function exportTable(format: 'csv' | 'json') {
		if (!appState.selectedTable) return;
		try {
			const data = await api.exportTable(appState.selectedTable.name, format);
			const blob = new Blob([data], { type: format === 'csv' ? 'text/csv' : 'application/json' });
			const url = URL.createObjectURL(blob);
			const a = document.createElement('a');
			a.href = url;
			a.download = `${appState.selectedTable.name}.${format}`;
			a.click();
			URL.revokeObjectURL(url);
			toast.success(`Exported as ${format.toUpperCase()}`);
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Export failed');
		}
	}

	async function truncateTable() {
		if (!appState.selectedTable) return;
		if (!confirm(`Truncate "${appState.selectedTable.name}"? This will delete ALL rows.`)) return;
		try {
			await api.truncateTable(appState.selectedTable.name);
			toast.success('Table truncated');
			await appState.loadTableData();
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Truncate failed');
		}
	}
</script>

<svelte:window onkeydown={handleKeydown} />

<div class="flex h-screen flex-col bg-background text-foreground">
	<!-- Connection color stripe -->
	{#if appState.connectionStatus === 'connected'}
		<div class="h-0.5" style="background-color: {appState.connectionColor}"></div>
	{/if}

	<div class="flex flex-1 overflow-hidden">
		<!-- Icon rail -->
		{#if appState.connectionStatus === 'connected'}
			<div class="flex w-10 flex-col items-center gap-1 border-r border-border bg-sidebar py-2">
				<Tooltip.Root>
					<Tooltip.Trigger>
						<Button
							variant={activePanel === 'tables' ? 'secondary' : 'ghost'}
							size="icon"
							class="h-8 w-8"
							onclick={() => activePanel = 'tables'}
						>
							<Table2 class="h-4 w-4" />
						</Button>
					</Tooltip.Trigger>
					<Tooltip.Content side="right">Tables</Tooltip.Content>
				</Tooltip.Root>

				<Tooltip.Root>
					<Tooltip.Trigger>
						<Button
							variant={activePanel === 'history' ? 'secondary' : 'ghost'}
							size="icon"
							class="h-8 w-8"
							onclick={() => activePanel = 'history'}
						>
							<History class="h-4 w-4" />
						</Button>
					</Tooltip.Trigger>
					<Tooltip.Content side="right">Query History</Tooltip.Content>
				</Tooltip.Root>

				<Tooltip.Root>
					<Tooltip.Trigger>
						<Button
							variant={activePanel === 'journal' ? 'secondary' : 'ghost'}
							size="icon"
							class="h-8 w-8"
							onclick={() => activePanel = 'journal'}
						>
							<BookOpen class="h-4 w-4" />
						</Button>
					</Tooltip.Trigger>
					<Tooltip.Content side="right">Change Journal</Tooltip.Content>
				</Tooltip.Root>

				<Tooltip.Root>
					<Tooltip.Trigger>
						<Button
							variant={activePanel === 'connections' ? 'secondary' : 'ghost'}
							size="icon"
							class="h-8 w-8"
							onclick={() => activePanel = 'connections'}
						>
							<Database class="h-4 w-4" />
						</Button>
					</Tooltip.Trigger>
					<Tooltip.Content side="right">Saved Connections</Tooltip.Content>
				</Tooltip.Root>

				{#if appState.selectedTable}
					<Separator class="my-1 w-6" />
					<Tooltip.Root>
						<Tooltip.Trigger>
							<Button
								variant={activePanel === 'info' ? 'secondary' : 'ghost'}
								size="icon"
								class="h-8 w-8"
								onclick={() => activePanel = 'info'}
							>
								<Info class="h-4 w-4" />
							</Button>
						</Tooltip.Trigger>
						<Tooltip.Content side="right">Table Info</Tooltip.Content>
					</Tooltip.Root>
				{/if}

				<div class="flex-1"></div>

				<Tooltip.Root>
					<Tooltip.Trigger>
						<Button variant="ghost" size="icon" class="h-8 w-8" onclick={() => showSql = !showSql}>
							<Terminal class="h-4 w-4" />
						</Button>
					</Tooltip.Trigger>
					<Tooltip.Content side="right">SQL Editor (Ctrl+L)</Tooltip.Content>
				</Tooltip.Root>
			</div>
		{/if}

		<!-- Side panel -->
		{#if appState.connectionStatus === 'connected'}
			<div class="w-56 border-r border-border bg-sidebar">
				{#if activePanel === 'tables'}
					<Sidebar
						onConnect={() => showConnect = true}
						onToggleSql={() => showSql = !showSql}
					/>
				{:else if activePanel === 'history'}
					<HistoryPanel onLoadQuery={loadQueryIntoEditor} />
				{:else if activePanel === 'journal'}
					<JournalPanel />
				{:else if activePanel === 'connections'}
					<SavedConnections />
				{:else if activePanel === 'info'}
					<TableInfoPanel />
				{/if}
			</div>
		{/if}

		<!-- Main content -->
		<div class="flex flex-1 flex-col overflow-hidden">
			{#if appState.connectionStatus !== 'connected'}
				<WelcomeScreen onConnect={() => showConnect = true} />
			{:else if !appState.selectedTable && !showSql}
				<WelcomeScreen onConnect={() => showConnect = true} />
			{:else}
				<!-- Table action bar -->
				{#if appState.selectedTable}
					<div class="flex items-center gap-1 border-b border-border px-2 py-1">
						<span class="text-xs font-medium">
							{appState.selectedTable.name}
						</span>
						<div class="flex-1"></div>
						{#if !appState.readOnly}
							<Tooltip.Root>
								<Tooltip.Trigger>
									<Button variant="ghost" size="icon" class="h-7 w-7" onclick={() => showInsertRow = true}>
										<Plus class="h-3.5 w-3.5" />
									</Button>
								</Tooltip.Trigger>
								<Tooltip.Content>Insert Row</Tooltip.Content>
							</Tooltip.Root>
							<Tooltip.Root>
								<Tooltip.Trigger>
									<Button variant="ghost" size="icon" class="h-7 w-7" onclick={() => showImportCsv = true}>
										<Upload class="h-3.5 w-3.5" />
									</Button>
								</Tooltip.Trigger>
								<Tooltip.Content>Import CSV</Tooltip.Content>
							</Tooltip.Root>
						{/if}
						<Tooltip.Root>
							<Tooltip.Trigger>
								<Button variant="ghost" size="icon" class="h-7 w-7" onclick={() => exportTable('csv')}>
									<Download class="h-3.5 w-3.5" />
								</Button>
							</Tooltip.Trigger>
							<Tooltip.Content>Export CSV</Tooltip.Content>
						</Tooltip.Root>
						{#if !appState.readOnly}
							<Tooltip.Root>
								<Tooltip.Trigger>
									<Button variant="ghost" size="icon" class="h-7 w-7" onclick={truncateTable}>
										<Trash2 class="h-3.5 w-3.5 text-destructive" />
									</Button>
								</Tooltip.Trigger>
								<Tooltip.Content>Truncate Table</Tooltip.Content>
							</Tooltip.Root>
						{/if}
					</div>
				{/if}

				<!-- SQL Editor (collapsible top panel) -->
				{#if showSql}
					<SqlEditor onClose={() => showSql = false} bind:value={sqlEditorValue} />
				{/if}

				<!-- Data grid -->
				{#if appState.selectedTable}
					<DataGrid />
				{/if}
			{/if}
		</div>
	</div>

	<!-- Status bar -->
	<StatusBar />
</div>

<!-- Dialogs -->
<ConnectDialog bind:open={showConnect} />
<CommandPalette bind:open={showCommandPalette} />
<CreateTableDialog bind:open={showCreateTable} />
<ImportCsvDialog bind:open={showImportCsv} />
<InsertRowDialog bind:open={showInsertRow} />
