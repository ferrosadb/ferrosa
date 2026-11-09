# ferrosa-plpgsql — FMEA

| Failure mode | Effect | Mitigation |
|---|---|---|
| Unsupported construct accepted as a no-op | Migration silently skips work | Parser refuses by name with `0A000`; tests pin each refused keyword |
| Variable substituted into a name position | Broken SQL or wrong column | Name-position rule; tests for INSERT target list, SET targets, utility statements |
| Assignment of a different type kept mistyped | Later comparisons wrong | `42804` refusal (no assignment casts yet) |
| Unbounded result read | Memory growth | `FETCH_BATCH` batches; one-row reads for EXISTS/scalars |
| Cursor leak on error | Host resources held | `close` on every path (`a_host_error_stops_the_run_and_closes_the_cursor`) |
