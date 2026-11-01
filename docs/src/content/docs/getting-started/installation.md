---
title: Installation
description: Install worf from a release package or build it from source.
---

To use worf the provider and subscriber must be running PostgreSQL 15 or newer.

The `worf` extension must be installed on both provider and subscriber.
You must `CREATE EXTENSION worf` on both.

Tables on the provider and subscriber must have the same names and be in the
same schema. Future revisions may add mapping features.

Tables on the provider and subscriber must have the same columns. A column may
have another data type on the subscriber, as while a change of its type is on
its way. Its values, in the initial copy too, are read with the provider's type
and converted as an assignment does, as `ALTER COLUMN ... TYPE` without `USING`
would, or else through text, which the column type's input reads with the
column's type modifier. An enum's values are its labels. Types that aren't
PostgreSQL's own are found by schema and name, and used when defined the same:
of the same kind, with the same input and receive functions and length, or the
same subtype, element or attribute types in order. A type made of enums, such
as an enum's array, is used only for a column made of the same enums, since the
provider's labels may be ones they lack. Values of a type the subscriber lacks,
defines differently or doesn't use are read by the column type's input when
sent as text (in the initial copy, with `force_text_transfer`, between major
versions, for composites and arrays of types that aren't PostgreSQL's own, and
for domains and ranges of anything but PostgreSQL's own plain types),
and fail with error 42804 when sent in binary. Values convert under the
subscriber's settings, such as `TimeZone` and `bytea_output`, however they're
sent. The initial copy passes the text of a value that needs no conversion,
such as an integer going into a wider one or `varchar` into `text`, straight to
the column. A value that doesn't convert fails with PostgreSQL's error, which
`worf.exception_behaviour` handles, in the initial copy too. Values of `money`,
or made of it, going into a column that holds money, fail with error 55000 when
the provider's `lc_monetary` counts other fraction digits than the subscriber's,
since the stored amounts then differ, or formats money in text that the
subscriber's reads as another amount. A provider whose `lc_monetary` changes
ends its stream at the next transaction, so that the subscriber reconnects and
checks again. An older worf
doesn't send or read column types, or say how it formats money, so columns
replicated through an older provider, subscriber or relay node need the same
types, and money the same `lc_monetary`. `CHECK` constraints, `NOT NULL`
constraints, etc., must be the same or weaker (more permissive) on the
subscriber than the provider.
A subscriber ignores a column it dropped in rows from a provider that still
has it.

Tables must have the same `PRIMARY KEY`s. It is not recommended to add additional
`UNIQUE` constraints other than the `PRIMARY KEY` (see below).

Some additional requirements are covered in [Limitations and Restrictions](../../limitations/).

## Installation

## Packages

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

    cp bin/* "$(pg_config --bindir)/"
    cp lib/* "$(pg_config --pkglibdir)/"
    cp share/extension/* "$(pg_config --sharedir)/extension/"

You might need to use `sudo` for these steps. A package works with any build of
the PostgreSQL major version it names. On Windows, the MSI finds your PostgreSQL
installation and installs the files there; see [docs/windows.md](../../windows/).
`SHA256SUMS` lists every file's checksum.

## From source code

worf builds with the [.NET 10 SDK](https://dotnet.microsoft.com/download) and
[Ankus](https://ankus.net), which needs LLVM Clang 20 or later and the server
headers of the PostgreSQL release you build for. From a clone of this
repository:

    dotnet tool restore
    dotnet ankus init --pg17 "$(command -v pg_config)"
    dotnet ankus install --pg 17 --project src/Worf/Worf.csproj

`ankus install` builds the extension with Native AOT and installs it into that
PostgreSQL installation. `dotnet ankus package --pg 17 --project src/Worf/Worf.csproj --output dist`
instead writes the files to `dist`. Build `worf_create_subscriber` with
`dotnet publish src/Worf.CreateSubscriber -c Release -r <runtime identifier>`,
and install it beside your PostgreSQL programs.
