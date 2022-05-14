-- General setup
\set SHOW_CONTEXT never

-- registering a remote server should have registered all default db modules
SELECT * FROM "PoWA".powa_db_module_config
ORDER BY srvid, db_module COLLATE "C";

-- Can't deactivate a specific db on an "all databases" config
SELECT * FROM "PoWA".powa_deactivate_db_module(1, 'pg_stat_user_functions', ARRAY['test']);
SELECT enabled, dbnames FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_user_functions';

-- Activating a specifc db on an "all databases" config switch to that db only
SELECT * FROM "PoWA".powa_activate_db_module(1, 'pg_stat_user_functions', ARRAY['d1']);
SELECT enabled, dbnames FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_user_functions';

-- Activating a specifc db on an specific db config replace that database
SELECT * FROM "PoWA".powa_activate_db_module(1, 'pg_stat_user_functions', ARRAY['d2']);
SELECT enabled, dbnames FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_user_functions';

-- Deactivating without specific database switches back to "all db", and mark it as disabled
SELECT * FROM "PoWA".powa_deactivate_db_module(1, 'pg_stat_user_functions');
SELECT enabled, dbnames FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_user_functions';

-- Activating with multiple db switches back to enabled and setup the datbases
SELECT * FROM "PoWA".powa_activate_db_module(1, 'pg_stat_user_functions', ARRAY['d1', 'd3', 'd4']);
SELECT enabled, dbnames FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_user_functions';

-- Deactivating a specific db will just remove that db
SELECT * FROM "PoWA".powa_deactivate_db_module(1, 'pg_stat_user_functions', ARRAY['d3']);
SELECT enabled, dbnames FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_user_functions';

-- Can't deactivate a non existing specific db
SELECT * FROM "PoWA".powa_deactivate_db_module(1, 'pg_stat_user_functions', ARRAY['d3']);
SELECT enabled, dbnames FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_user_functions';

-- Deactivating all remaining db will switch back to "all db", and mark it as disabled
SELECT * FROM "PoWA".powa_deactivate_db_module(1, 'pg_stat_user_functions', ARRAY['d1', 'd4']);
SELECT enabled, dbnames FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_user_functions';

-- Deactivating a deactivated db module is a noop
SELECT * FROM "PoWA".powa_deactivate_db_module(1, 'pg_stat_user_functions', ARRAY['d1', 'd4']);
SELECT enabled, dbnames FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_user_functions';

-- Deactivating a known but not configured db module isn't supported
DELETE FROM "PoWA".powa_db_module_config WHERE srvid = 1 AND db_module = 'pg_stat_all_indexes';
SELECT * FROM "PoWA".powa_deactivate_db_module(1, 'pg_stat_all_indexes');

-----------------------------------------------------------
-- Test the query source API, with different major versions
-----------------------------------------------------------

-- pg 13.1 should see n_ins_since_vacuum but not last_seq_scan and other fields
-- introduced in pg16
SELECT * FROM "PoWA".powa_db_functions(1, 130001)
ORDER BY db_module COLLATE "C", operation COLLATE "C";

-- Check that we don't see n_ins_since_vacuum on pg13-
SELECT query_source FROM "PoWA".powa_db_functions(1, 120012)
WHERE db_module = 'pg_stat_all_tables' AND operation = 'snapshot';
