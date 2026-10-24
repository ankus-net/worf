---
title: worf
description: Logical replication for PostgreSQL, built with C# and .NET Native AOT.
template: splash
head:
  - tag: title
    content: worf
hero:
  tagline: Logical replication for PostgreSQL, built with C# and .NET Native AOT
  actions:
    - text: Get started
      link: ./getting-started/installation/
      icon: right-arrow
---

```sql
SELECT worf.create_subscription(
    subscription_name := 'subscription1',
    provider_dsn := 'host=providerhost port=5432 dbname=db'
);
```

<div class="feature-strip">
  <div class="feature-item">
    <span class="feature-key">Replication sets</span>
    <span class="feature-label">Choose the tables, actions, rows and columns each subscriber receives.</span>
  </div>
  <div class="feature-item">
    <span class="feature-key">Initial sync</span>
    <span class="feature-label">Copy structure and data from the slot's snapshot, or resynchronize one table.</span>
  </div>
  <div class="feature-item">
    <span class="feature-key">Conflicts</span>
    <span class="feature-label">Five resolutions, logging and a queryable history.</span>
  </div>
  <div class="feature-item">
    <span class="feature-key">Topologies</span>
    <span class="feature-label">Multiple upstreams, cascades and bidirectional replication with origin filtering.</span>
  </div>
  <div class="feature-item">
    <span class="feature-key">DDL</span>
    <span class="feature-label">Replicate DDL with replicate_ddl_command, and TRUNCATE with the table.</span>
  </div>
</div>

<p class="platform-note">PostgreSQL 13 through 19 on Linux, macOS and Windows</p>
