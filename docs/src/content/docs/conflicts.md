---
title: Conflicts
description: How worf detects and resolves conflicts.
---

In case the node is subscribed to multiple providers, or when local writes
happen on a subscriber, conflicts can arise for the incoming changes. These
are automatically detected and can be acted on depending on the configuration.

The configuration of the conflicts resolver is done via the
`worf.conflict_resolution` setting.

The resolved conflicts are logged using the log level set using
`worf.conflict_log_level`. This parameter defaults to `LOG`. If set to
lower level than `log_min_messages` the resolved conflicts won't appear in
the server log.
