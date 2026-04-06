# TODO: SPARQL — OPTIONAL, RDF* Execution, Turtle Serialization

**Severity:** Medium
**Component:** ferrosa-sparql
**Files:** `planner.rs:336`, `rdf_star.rs:70`, `results.rs:129`, `property_path.rs:161`

## Issue

Four SPARQL features are partially implemented:

1. **OPTIONAL (LeftJoin)**: Only the left side is evaluated. Right side patterns are ignored with a warning. `OPTIONAL { ?s ?p ?o }` returns nothing from the optional block.

2. **RDF* execution**: Parser accepts `<< ?s ?p ?o >> ?prop ?val` (via sparql-12 flag) but the executor stub returns inner bindings without joining edge_annotations. Annotation values are never populated.

3. **Turtle serialization**: `text/turtle` content negotiation returns simplified N-Triples format, not proper Turtle with prefix grouping.

4. **Nested property paths**: Operators like `(foaf:knows | foaf:follows)+` with alternation are not supported. Only simple closure operators (`+`, `*`, `?`) on single predicates work.

## Impact

Queries return incomplete or incorrectly formatted results without errors.
