--Setup extension
CREATE SCHEMA "PGSS";
CREATE EXTENSION pg_stat_statements WITH SCHEMA "PGSS";
CREATE EXTENSION btree_gist;
CREATE SCHEMA "PoWA";
CREATE EXTENSION powa WITH SCHEMA "PoWA";

-- Test created ojects
SELECT * FROM "PoWA".powa_functions ORDER BY name, operation, priority, function_name;
