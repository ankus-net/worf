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

`worf_create_subscriber` runs the `pg_basebackup`, `pg_ctl` and
`pg_controldata` next to it, so install it in the PostgreSQL installation's
`bin` directory.

## The server it starts

The server that the tool leaves running logs to `worf_subscriber.log` in the
data directory. It doesn't hold the tool's output open, so a shell pipe that
reads the tool's output doesn't wait for the server to stop. If the server
stops as it starts, or never comes up, the run fails after 30 seconds.

## Failures and interruptions

If a run fails, or is stopped with Ctrl-C or `SIGTERM`, it cancels what it is
waiting for, rolls back, and stops the server it started. It puts back the
settings files it changed in the data directory, and keeps `standby.signal` if
the server hadn't left recovery yet.

- Stopped during `pg_basebackup`, it ends the backup and removes what the backup
  wrote, including the `--waldir` and `-T` directories it created.
- Stopped while `pg_ctl` starts or stops the server, it waits for that to
  finish, then stops the server.
- Once one database's subscription has committed, the others can't roll back
  with it, so a stop waits for the run to finish setting them up.

A failed or stopped run drops the replication slots it created on the provider
for databases it hadn't subscribed yet. It leaves, with a warning, a slot that
another run with the same subscriber name created in its place, a slot that a
client has already read from, and a slot that is in use.

A killed run, or one that can't reach the provider while it cleans up, leaves
its slot behind. That slot holds back the provider's WAL. A later run with the
same subscriber name fails until the slot is dropped, or until it runs with
`--drop-slot-if-exists`. A killed run also leaves its server running; its
transaction rolls back once the server notices it's gone.

## Reusing a data directory

`-D` can name a directory made earlier with `pg_basebackup -X stream`, as long
as its server isn't running. The tool checks that the directory's WAL is the
provider's, and refuses it, unchanged, if:

- its latest checkpoint is past the provider's WAL;
- its WAL or timeline history differs from the provider's, as for a standby
  that was promoted or started without its `standby.signal`;
- it shares no WAL with what the provider still has, so the two can't be
  compared;
- the tool can't read the provider's WAL to compare. It lists the provider's
  WAL with `pg_ls_waldir` and reads it with `pg_read_binary_file`, so the
  run's role needs those, and membership in `pg_monitor`.

To keep the provider's WAL available for a standby, use a physical replication
slot, as `pg_basebackup -C -S` creates, or set `wal_keep_size`.
