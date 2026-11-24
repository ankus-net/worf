# worf

worf provides logical streaming replication for PostgreSQL, using a
publish/subscribe model. It is built with C# and .NET Native AOT on
[Ankus](https://github.com/ankus-net/ankus). See the
[documentation](https://ankus-net.github.io/worf/).

We use the following terms to describe data streams between nodes, deliberately
reused from the earlier Slony technology:
* Nodes - PostgreSQL database instances
* Providers and Subscribers - roles taken by Nodes
* Replication Set - a collection of tables

worf runs on PostgreSQL 15 through 19, on Linux x64, macOS ARM64 and x64, and
Windows x64. Provider and subscriber nodes may run different PostgreSQL major
versions (see [Limitations](https://ankus-net.github.io/worf/limitations/#postgresql-version-differences)).

Use cases supported are:
* Upgrades between major versions (given the above restrictions)
* Full database replication
* Selective replication of sets of tables using replication sets
* Selective replication of table rows at either publisher or subscriber side (row_filter)
* Selective replication of table columns at publisher side
* Data gather/merge from multiple upstream servers

Architectural details:
* worf works on a per-database level, not whole server level like
  physical streaming replication
* One Provider may feed multiple Subscribers without incurring additional disk
  write overhead
* One subscriber can merge changes from several origins, and nodes can all
  write to the same tables, with conflicts between their changes detected and
  resolved automatically as configured.
* Cascading replication is implemented in the form of changeset forwarding.

## Requirements

- PostgreSQL 15 or newer on every node, with worf installed and
  `CREATE EXTENSION worf` run in each replicated database.
- Replicated tables with the same schema, name, columns and primary key on
  every node.

See [Installation](https://ankus-net.github.io/worf/getting-started/installation/) and
[Limitations](https://ankus-net.github.io/worf/limitations/) for details.

## Installation

### Packages

Each [release](https://github.com/ankus-net/worf/releases) has a package for
every supported PostgreSQL major version and platform, named
`worf-<version>-pg<major>-<platform>`:

- `linux-x64`, `macos-arm64` and `macos-x64`: a `.tar.gz` archive
- `windows-x64`: a `.zip` archive and an `.msi` installer

Every package holds the extension library, its control and SQL files, and
`worf_create_subscriber`, laid out as `bin/`, `lib/` and `share/extension/`.
Copy each directory's files into the matching directory of your PostgreSQL
installation, as `pg_config --bindir`, `--pkglibdir` and `--sharedir` report
them:

```sh
cp bin/* "$(pg_config --bindir)/"
cp lib/* "$(pg_config --pkglibdir)/"
cp share/extension/* "$(pg_config --sharedir)/extension/"
```

You might need to use `sudo` for these steps. A package works with any build of
the PostgreSQL major version it names. On Windows, the MSI finds your PostgreSQL
installation and installs the files there; see [Windows](https://ankus-net.github.io/worf/windows/).
`SHA256SUMS` lists every file's checksum.

### From source code

worf builds with the [.NET 10 SDK](https://dotnet.microsoft.com/download) and
[Ankus](https://ankus.net), which needs LLVM Clang 20 or later and the server
headers of the PostgreSQL release you build for. From a clone of this
repository:

```sh
dotnet tool restore
dotnet ankus init --pg17 "$(command -v pg_config)"
dotnet ankus install --pg 17 --project src/Worf/Worf.csproj
```

`ankus install` builds the extension with Native AOT and installs it into that
PostgreSQL installation. `dotnet ankus package --pg 17 --project src/Worf/Worf.csproj --output dist`
instead writes the files to `dist`. Build `worf_create_subscriber` with
`dotnet publish src/Worf.CreateSubscriber -c Release -r <runtime identifier>`,
and install it beside your PostgreSQL programs.

## Quick start

In `postgresql.conf` on every node:

```ini
wal_level = 'logical'
max_worker_processes = 10
max_replication_slots = 10
max_wal_senders = 10
shared_preload_libraries = 'worf'
```

On the provider:

```sql
CREATE EXTENSION worf;
SELECT worf.create_node(node_name := 'provider1', dsn := 'host=providerhost port=5432 dbname=db');
SELECT worf.replication_set_add_all_tables('default', ARRAY['public']);
```

On the subscriber:

```sql
CREATE EXTENSION worf;
SELECT worf.create_node(node_name := 'subscriber1', dsn := 'host=thishost port=5432 dbname=db');
SELECT worf.create_subscription(subscription_name := 'subscription1', provider_dsn := 'host=providerhost port=5432 dbname=db');
SELECT worf.wait_for_subscription_sync_complete('subscription1');
```

See [Quick setup](https://ankus-net.github.io/worf/getting-started/quick-setup/) for the full steps.

## Documentation

- [Node management](https://ankus-net.github.io/worf/nodes/)
- [Subscription management](https://ankus-net.github.io/worf/subscriptions/)
- [Replication sets](https://ankus-net.github.io/worf/replication-sets/)
- [Row filtering](https://ankus-net.github.io/worf/row-filtering/)
- [Additional functions](https://ankus-net.github.io/worf/functions/)
- [Subscribers from base backups](https://ankus-net.github.io/worf/create-subscriber/)
- [Synchronous replication](https://ankus-net.github.io/worf/synchronous-replication/)
- [Conflicts](https://ankus-net.github.io/worf/conflicts/) and [conflict history](https://ankus-net.github.io/worf/conflict-history/)
- [Bidirectional replication](https://ankus-net.github.io/worf/bidirectional-replication/)
- [Sequence ranges](https://ankus-net.github.io/worf/sequence-ranges/)
- [Configuration options](https://ankus-net.github.io/worf/configuration/)
- [Limitations and restrictions](https://ankus-net.github.io/worf/limitations/)
- [Windows](https://ankus-net.github.io/worf/windows/)
