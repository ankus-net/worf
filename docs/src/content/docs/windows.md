---
title: Windows
description: Install worf on Windows, and how it behaves there.
---

## Installing

### MSI

Run `worf-<version>-pg<major>-windows-x64.msi` for your PostgreSQL major version.
It finds the PostgreSQL installation in the registry, or lets you choose its
directory, and installs:

- `bin\worf_create_subscriber.exe`
- `lib\worf.dll`
- `share\extension\worf.control` and `share\extension\worf--*.sql`

Remove it from **Apps** in Settings.

### ZIP

Extract `worf-<version>-pg<major>-windows-x64.zip` and copy its directories into
the PostgreSQL installation:

```powershell
$PG_DIR = "C:\Program Files\PostgreSQL\17"
Copy-Item "bin\*.exe" "$PG_DIR\bin\"
Copy-Item "lib\*.dll" "$PG_DIR\lib\"
Copy-Item "share\extension\*" "$PG_DIR\share\extension\"
```

Then configure PostgreSQL as the [README](../getting-started/quick-setup/) describes,
restart the service, and run `CREATE EXTENSION worf;`.

## Behavior

worf behaves on Windows as it does elsewhere, with these differences:

- Workers wait in shorter intervals, because process startup and signaling are
  slower on Windows: the supervisor rechecks every 30 seconds, the manager every
  second, and waits for sync and slot progress every 200 milliseconds.
- Apply delay adds 100 milliseconds to each wait, since Windows timers are
  accurate only to about 15 milliseconds.
- Structure synchronization runs the server's own `pg_dump.exe` and
  `pg_restore.exe`, so paths with spaces, such as `C:\Program Files`, work.
- With `worf.temp_directory` empty, dumps go to the directory in `TEMP`.
