---
title: Subscribers from base backups
description: Create a subscriber from a base backup of the provider with worf_create_subscriber.
---

In addition to the SQL-level node and subscription creation, worf also
supports creating a subscriber by cloning the provider with `pg_basebackup` and
starting it up as a worf subscriber. This is done with the
`worf_create_subscriber` tool; see the `--help` output. It connects through
the installation's libpq, so connection strings, service files, password files
and `PG*` environment variables work as in `psql`.

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

`worf_create_subscriber` runs the `pg_basebackup`, `pg_ctl` and `pg_controldata`
beside it, so install it in the PostgreSQL installation's `bin` directory.

A run that fails, or is stopped with Ctrl-C or `SIGTERM`, cancels what it waits
for, a connection or a command on a server that doesn't answer too, rolls back,
and stops the server it started, putting back the data directory's settings, the
`--recovery-conf` template's too, the `postgresql.conf` and `pg_hba.conf` it
copied over, and keeping a `standby.signal` it had, if the server hadn't yet left
recovery, as its cluster state shows once stopped. A stop while `pg_basebackup`
runs ends it, and removes what it wrote, in the WAL directory `--waldir` names and
the tablespace directories `-T` maps too, those it made with them; one while
`pg_ctl` starts or stops the server waits for it, and then stops the server. Once a database's subscription has committed, the
others can't roll back with it, so a stop waits for the run to set them up, and
changes nothing. A killed run leaves its server running, and its transaction rolls
back once the server finds it gone. The subscription keeps the position its
restore point reached in the same transaction, which its apply worker starts
from, and its replication origin gets progress only as the worker applies, so
neither leaves progress for a later origin. A run on a data directory whose server
is running fails, as does one on a data directory whose timeline is past the
provider's, as an earlier run's promotion leaves it, or that has run as a server
of its own, out of recovery, as one started without its `standby.signal` does,
which writes WAL the provider doesn't have, or, once it has started, whose
latest checkpoint isn't a checkpoint record of the provider's WAL with the redo
point its `pg_control` holds, as one that did so and stopped in recovery later
has, or lies in WAL the provider has removed; each leaves the data directory as
it is. The run reads that record from the provider's `pg_wal` with
`pg_read_binary_file`, and can't tell when the provider doesn't let it.
