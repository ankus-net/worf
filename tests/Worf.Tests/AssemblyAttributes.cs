// Each test starts its own PostgreSQL clusters on free ports, so tests run side by side, more than the runners have
// processors, since they mostly wait on their servers.
[assembly: Parallelize(Workers = 6, Scope = ExecutionScope.MethodLevel)]
