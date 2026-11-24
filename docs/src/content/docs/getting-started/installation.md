---
title: Installation
description: Install worf from a release package or build it from source.
---

## Requirements

- The provider and subscriber run PostgreSQL 15 or newer.
- worf is installed on both, and `CREATE EXTENSION worf` has been run on both.
- Replicated tables have the same schema, name, columns and primary key on both.
- `CHECK`, `NOT NULL` and other constraints on the subscriber are the same as
  on the provider, or looser. Avoid extra `UNIQUE` constraints; see
  [Limitations](../../limitations/).

## Column types

A column can have a different type on the subscriber, for example while a type
change is on its way. Values are converted the way `ALTER COLUMN ... TYPE`
without `USING` would convert them, or else through their text form. A value
that doesn't convert fails with PostgreSQL's error, which
`worf.exception_behaviour` handles, in the initial copy too. Values convert
under the subscriber's settings, such as `TimeZone` and `bytea_output`.

Types that aren't built into PostgreSQL are matched by schema and name, and
treated as the same type only if they are defined the same way on both nodes.
Enums are matched by their labels, so a type built on an enum, such as an
enum's array, is only used for a column built on the same enum. Values of a
type the subscriber doesn't have, or defines differently, must be sent as text:
they are when `force_text_transfer` is on, in the initial copy, and between
major versions. Sent in binary, they fail with error `42804`.

`money` values fail with error `55000` if the provider's and subscriber's
`lc_monetary` use a different number of decimal places, since the amounts
would differ. If only the format differs, values sent in binary still
replicate, but values sent as text, as in the initial copy, fail with `55000`.
worf retries these errors until the settings agree, and checks again when
`lc_monetary` changes. Older worf versions don't send this information, so
columns replicated through an older node need the same types, and `money`
needs the same `lc_monetary`.

A subscriber ignores a column it dropped when the provider still has it.

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

```sh
cp bin/* "$(pg_config --bindir)/"
cp lib/* "$(pg_config --pkglibdir)/"
cp share/extension/* "$(pg_config --sharedir)/extension/"
```

You might need to use `sudo` for these steps. A package works with any build of
the PostgreSQL major version it names. On Windows, the MSI finds your PostgreSQL
installation and installs the files there; see [docs/windows.md](../../windows/).
`SHA256SUMS` lists every file's checksum.

## From source code

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
