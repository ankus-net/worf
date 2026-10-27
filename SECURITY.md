# Security

Report a vulnerability privately through GitHub Security Advisories for the `ankus-net/worf` repository: open the
Security tab and choose "Report a vulnerability". Please do not open a public issue for one. Fixes go into `main`
and ship in the next release.

## What to report

worf runs inside PostgreSQL server processes, with the server's operating-system account, as a C extension does. A
report is welcome when worf does something an administrator did not ask for, for example when:

- a subscriber applies changes, or a provider sends rows, outside the replication sets, row filters and column lists
  configured for them;
- a replicated DDL command, a sequence value or a value from the protocol is run as SQL text other than what the
  provider executed;
- a function skips a privilege check it documents, such as the table privileges and row-level security the apply
  enforces, or PostgreSQL's `output_plugin_libraries`;
- a connection string, a password or other credential reaches a log, an error message or another node;
- `worf_create_subscriber` runs or loads something other than the PostgreSQL programs it names.

## The embedded .NET runtime

worf embeds the patched .NET Native AOT runtime of the Ankus release it is built with, so a .NET security release
reaches worf only with a worf release built on an Ankus release that carries it. Upgrade worf after each such release.

## Network use

worf collects no telemetry. It connects only to the nodes whose connection strings you give it.
