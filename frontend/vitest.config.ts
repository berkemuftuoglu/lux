import { defineConfig } from 'vitest/config';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import { svelteTesting } from '@testing-library/svelte/vite';

export default defineConfig({
	plugins: [svelte({ hot: false }), svelteTesting()],
	test: {
		environment: 'jsdom',
		globals: true,
		include: ['src/**/*.test.ts'],
		setupFiles: ['./vitest-setup.ts'],
	},
	resolve: {
		alias: { $lib: new URL('./src/lib', import.meta.url).pathname },
	},
});
