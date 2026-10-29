// Each test starts its own PostgreSQL clusters on free ports, so tests run side by side, more than the runners have
// processors, since they mostly wait on their servers. With six at a time, every worker on the macOS runners stayed
// busy until the last test started.
[assembly: Parallelize(Workers = 10, Scope = ExecutionScope.MethodLevel)]
