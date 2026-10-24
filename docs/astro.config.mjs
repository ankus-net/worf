import { defineConfig } from 'astro/config';
import { satteri } from '@astrojs/markdown-satteri';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://ankus-net.github.io',
  base: '/worf',
  outDir: '../artifacts/docs',
  cacheDir: './node_modules/.astro-cache',
  markdown: {
    processor: satteri(),
  },
  integrations: [
    starlight({
      title: 'worf',
      description: 'Logical replication for PostgreSQL, built with C# and .NET Native AOT.',
      disable404Route: true,
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/ankus-net/worf',
        },
      ],
      customCss: ['./src/styles/custom.css'],
      sidebar: [
        {
          label: 'Getting started',
          items: [
            { label: 'Installation', slug: 'getting-started/installation' },
            { label: 'Quick setup', slug: 'getting-started/quick-setup' },
          ],
        },
        {
          label: 'Usage',
          items: [
            { label: 'Node management', slug: 'nodes' },
            { label: 'Subscription management', slug: 'subscriptions' },
            { label: 'Replication sets', slug: 'replication-sets' },
            { label: 'Row filtering', slug: 'row-filtering' },
            { label: 'Additional functions', slug: 'functions' },
            { label: 'Subscribers from base backups', slug: 'create-subscriber' },
            { label: 'Synchronous replication', slug: 'synchronous-replication' },
            { label: 'Conflicts', slug: 'conflicts' },
            { label: 'Conflict history', slug: 'conflict-history' },
            { label: 'Bidirectional replication', slug: 'bidirectional-replication' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'Configuration options', slug: 'configuration' },
            { label: 'Limitations and restrictions', slug: 'limitations' },
            { label: 'Windows', slug: 'windows' },
          ],
        },
      ],
    }),
  ],
});
