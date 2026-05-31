-- WASM UDFs (Ferrosa does not run Java). The body is the compiled WASM module
-- as inline hex (placeholder below).
CREATE FUNCTION some_function ( arg int )
    RETURNS NULL ON NULL INPUT
    RETURNS int
    LANGUAGE wasm
    AS '0x0061736d0d000100';

SELECT some_function(column) FROM atable;
UPDATE atable SET col = some_function(?) WHERE pk = 1;

CREATE TYPE custom_type (txt text, i int);
CREATE FUNCTION fct_using_udt ( udtarg frozen<custom_type> )
    RETURNS NULL ON NULL INPUT
    RETURNS text
    LANGUAGE wasm
    AS '0x0061736d0d000100';
