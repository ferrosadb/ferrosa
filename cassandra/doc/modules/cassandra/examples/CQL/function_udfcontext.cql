CREATE TYPE custom_type (txt text, i int);

-- A WASM UDF that returns a UDT value. Ferrosa runs UDFs as WebAssembly
-- components (inline hex placeholder body below); the WASM module builds and
-- returns the custom_type value.
CREATE FUNCTION fct_using_udt ( somearg int )
    RETURNS NULL ON NULL INPUT
    RETURNS custom_type
    LANGUAGE wasm
    AS '0x0061736d0d000100';
