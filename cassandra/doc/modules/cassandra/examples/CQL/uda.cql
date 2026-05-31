-- User-defined aggregate built from WASM state/final functions. Ferrosa runs
-- UDF/UDA bodies as WebAssembly components (inline hex placeholder below).
CREATE OR REPLACE FUNCTION test.averageState(state tuple<int,bigint>, val int)
    CALLED ON NULL INPUT
    RETURNS tuple<int,bigint>
    LANGUAGE wasm
    AS '0x0061736d0d000100';

CREATE OR REPLACE FUNCTION test.averageFinal (state tuple<int,bigint>)
    CALLED ON NULL INPUT
    RETURNS double
    LANGUAGE wasm
    AS '0x0061736d0d000100';

CREATE OR REPLACE AGGREGATE test.average(int)
    SFUNC averageState
    STYPE tuple<int,bigint>
    FINALFUNC averageFinal
    INITCOND (0, 0);

CREATE TABLE test.atable (
    pk int PRIMARY KEY,
    val int
);

INSERT INTO test.atable (pk, val) VALUES (1,1);
INSERT INTO test.atable (pk, val) VALUES (2,2);
INSERT INTO test.atable (pk, val) VALUES (3,3);
INSERT INTO test.atable (pk, val) VALUES (4,4);

SELECT test.average(val) FROM atable;
