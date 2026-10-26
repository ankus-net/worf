// Each test starts its own PostgreSQL clusters on free ports, so test classes run side by side.
[assembly: Parallelize(Scope = ExecutionScope.ClassLevel)]
