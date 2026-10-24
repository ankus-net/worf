---
title: Subscribers from base backups
description: Create a subscriber from a base backup of the provider with worf_create_subscriber.
---

In addition to the SQL-level node and subscription creation, worf also
supports creating a subscriber by cloning the provider with `pg_basebackup` and
starting it up as a worf subscriber. This is done with the
`worf_create_subscriber` tool; see the `--help` output.

Unlike `worf.create_subscription`'s data sync options, this clone ignores
replication sets and copies all tables on all databases. However, it's often
much faster, especially over high-bandwidth links.

```console
worf_create_subscriber -D /path/to/new/datadir \
    --subscriber-name=subscriber1 \
    --provider-dsn='host=providerhost dbname=db' \
    --subscriber-dsn='host=thishost dbname=db'
```

## Options

| Option | Meaning |
| --- | --- |
| `-D`, `--pgdata=DIRECTORY` | Data directory for the new node: an empty or missing directory, or one made with `pg_basebackup -X stream` |
| `--databases` | Comma-separated list of databases to replicate |
| `-n`, `--subscriber-name=NAME` | Name of the new subscriber |
| `--subscriber-dsn=CONNSTR` | Connection string to the new subscriber |
| `--provider-dsn=CONNSTR` | Connection string to the provider |
| `--replication-sets=SETS` | Comma-separated replication set names |
| `--apply-delay=DELAY` | Apply delay in seconds (default 0) |
| `--drop-slot-if-exists` | Drop a replication slot of the same name |
| `-s`, `--stop` | Stop the server once initialization is done |
| `-v` | Increase logging verbosity |
| `--extra-basebackup-args` | Additional arguments for `pg_basebackup`; safe options are `-T`, `-c` and `--waldir` |
| `--text-types` | Replicate all columns as text |
| `--hba-conf` | Path to the new `pg_hba.conf` |
| `--postgresql-conf` | Path to the new `postgresql.conf` |
| `--recovery-conf` | Path to the template recovery configuration |

`worf_create_subscriber` runs the `pg_basebackup` and `pg_ctl` beside it, so
install it in the PostgreSQL installation's `bin` directory.
