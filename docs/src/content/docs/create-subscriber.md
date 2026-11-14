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
beside it, so install it in the PostgreSQL installation's `bin` directory. The
server a run leaves running writes its log to `worf_subscriber.log` in the data
directory, and holds none of the run's output open, so a caller that reads the
run's output to its end, as a shell pipe does, doesn't wait for the server to
stop: `pg_ctl` gets output of its own, which the run writes as it comes. A
catch-up server that stops as it starts, or never comes up, fails the run after
30 seconds.

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
changes nothing. A failed or stopped run drops the replication slots it made on
the provider for the databases it hadn't subscribed yet, but only the slot it
made as it made it, which the slot's confirmed position shows: one another run
with the same subscriber name made in its place, its own once a client has
confirmed what it streamed from it, or one a walsender streams, stays, with a
warning. A killed run, or one that can't reach
the provider as it cleans up, leaves its slot, which holds back the provider's
WAL and catalog rows, and a later run with the same subscriber name fails until
the slot is dropped, or runs with `--drop-slot-if-exists`. A killed run leaves its
server running, and its transaction rolls back once the server finds it gone. The subscription keeps the position its
restore point reached in the same transaction, which its apply worker starts
from, and its replication origin gets progress only as the worker applies, so
neither leaves progress for a later origin.
A run on a data directory whose server is running fails, as does one whose WAL
isn't the provider's, which leaves the data directory as it is. A directory
whose latest checkpoint is past the provider's WAL is refused whatever the run's
role may read, and one whose WAL can't be compared with the provider's, as when
the run's role isn't a member of `pg_monitor`, is refused once its own records
are checked. The run lists the WAL the provider still has with `pg_ls_waldir`,
and requires the directory's timeline history files to be the provider's, though
the directory never reached their timelines, as a standby that fetched a history
it couldn't follow has, its WAL on a timeline the provider has left to end by
the provider's switch from that timeline, which an earlier run's promotion, or a
server started without its `standby.signal`, writes past, though a record the
directory holds only part of may start past it, as a sibling standby's may,
since recovery that follows the provider never replays it; and, from its latest
checkpoint's redo point on, its records to each link back to the one before,
whole as their checksums show, passing over a record that a crash cut short and
the server overwrote as it started again, as recovery does, and every byte of
its WAL that the provider still has too, up to the provider's switch, to be the
provider's, as a standby's are and those of one that left the provider's WAL
aren't. A directory whose WAL shares none with what the provider still has, as
one whose WAL ends where the provider's begins, can't be told from one that left
it, and is refused too, with a hint to keep the provider's WAL from a standby's
position with a physical replication slot, as `pg_basebackup -C -S` makes, or
`wal_keep_size`. The run reads the provider's WAL with `pg_read_binary_file`,
and a directory it can't check so, as when the provider doesn't let it, is refused.
