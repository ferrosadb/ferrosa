# ferrosa-plpgsql — overview

Body text → `lexer` (tokens with spans) → `parser` (slots, lowered expressions, refusals) →
`Program` → `interp` (DO path) or `assemblyscript` (CREATE FUNCTION path, compiled by
`ferrosa-udf`). Both runtimes use only `Host`.

Invariants:
- A construct outside the subset never reaches a runtime: the parser refuses it.
- Each variable has exactly one slot; a placeholder number is reused for repeated mentions.
- A query result is read in bounded batches; `EXISTS` and scalar reads stop after one row and
  close the cursor on every path, including errors.
