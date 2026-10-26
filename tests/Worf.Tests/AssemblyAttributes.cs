// Each test starts its own PostgreSQL clusters on free ports, so tests run side by side.
[assembly: Parallelize(Scope = ExecutionScope.MethodLevel)]
