-- Ferrosa runs user-defined functions as WebAssembly components, not Java.
-- The function body is the compiled WASM module: inline hex (a tiny placeholder
-- below), or `AS FILE '/path.wasm'` / `AS URL '...' WITH SHA256 = '...'`.
CREATE OR REPLACE FUNCTION somefunction(somearg int, anotherarg text, complexarg frozen<some_udt>, listarg list<int>)
    RETURNS NULL ON NULL INPUT
    RETURNS text
    LANGUAGE wasm
    AS '0x0061736d0d000100';

CREATE FUNCTION IF NOT EXISTS akeyspace.fname(somearg int)
    CALLED ON NULL INPUT
    RETURNS text
    LANGUAGE wasm
    AS '0x0061736d0d000100';
