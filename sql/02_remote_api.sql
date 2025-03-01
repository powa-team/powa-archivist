-- General setup
\set SHOW_CONTEXT never

-- Check API
SELECT "PoWA".powa_register_server(hostname => '127.0.0.1',
    extensions => '{pg_qualstats}');
SELECT COUNT(*) FROM "PoWA".powa_servers;
SELECT hostname FROM "PoWA".powa_servers WHERE id = 1;

-- Check missing powa_statements FK for pg_qualstats doesn't prevent snapshot
INSERT INTO "PoWA".powa_qualstats_src_tmp(srvid, ts, uniquequalnodeid, dbid, userid,
    qualnodeid, occurences, execution_count, nbfiltered,
    mean_err_estimate_ratio, mean_err_estimate_num,
    queryid, constvalues, quals)
    SELECT 1, now(), 1, 1, 1,
        1, 1000, 1, 0,
        0, 0,
        123456789, '{}', ARRAY[(1259,1,607,'i')::"PoWA".qual_type];
SELECT count(*) FROM "PoWA".powa_qualstats_src_tmp;
SELECT "PoWA".powa_qualstats_snapshot(1);
SELECT count(*) FROM "PoWA".powa_qualstats_src_tmp;
SELECT count(*) FROM "PoWA".powa_qualstats_quals_history_current WHERE srvid = 1;

-- Check snapshot of regular quals
INSERT INTO "PoWA".powa_databases(srvid, oid, datname, dropped)
    VALUES (1, 16384, 'postgres', NULL);
INSERT INTO "PoWA".powa_statements(srvid, queryid, dbid, userid, query)
    VALUES(1, 123456789, 16384, 10, 'query with qual');
INSERT INTO "PoWA".powa_qualstats_src_tmp(srvid, ts, uniquequalnodeid, dbid, userid,
    qualnodeid, occurences, execution_count, nbfiltered,
    mean_err_estimate_ratio, mean_err_estimate_num,
    queryid, constvalues, quals)
    SELECT 1, now(), 1, 16384, 10,
        1, 1000, 1, 0,
        0, 0,
        123456789, '{}', ARRAY[(1259,1,607,'i')::"PoWA".qual_type];
SELECT count(*) FROM "PoWA".powa_qualstats_src_tmp;
SELECT "PoWA".powa_qualstats_snapshot(1);
SELECT count(*) FROM "PoWA".powa_qualstats_src_tmp;
SELECT count(*) FROM "PoWA".powa_qualstats_quals_history_current WHERE srvid = 1;

-- activate / deactivate extension
SELECT * FROM "PoWA".powa_functions
WHERE name IN ('pg_database', 'pg_stat_statements', 'pg_stat_kcache', 'pg_qualstats', 'some_extension')
ORDER BY srvid, name, operation, function_name;
SELECT * FROM "PoWA".powa_activate_extension(1, 'pg_stat_kcache');
SELECT * FROM "PoWA".powa_activate_extension(1, 'some_extension');
SELECT * FROM "PoWA".powa_functions
WHERE name IN ('pg_database', 'pg_stat_statements', 'pg_stat_kcache', 'pg_qualstats', 'some_extension')
ORDER BY srvid, name, operation, function_name;
SELECT * FROM "PoWA".powa_deactivate_extension(1, 'pg_stat_kcache');
SELECT * FROM "PoWA".powa_deactivate_extension(1, 'some_extension');
SELECT * FROM "PoWA".powa_functions
WHERE name IN ('pg_database', 'pg_stat_statements', 'pg_stat_kcache', 'pg_qualstats', 'some_extension')
ORDER BY srvid, name, operation, function_name;

SELECT alias FROM "PoWA".powa_servers WHERE id = 1;
SELECT * FROM "PoWA".powa_configure_server(0, '{"somekey": "someval"}');
SELECT * FROM "PoWA".powa_configure_server(1, '{"somekey": "someval"}');
SELECT * FROM "PoWA".powa_configure_server(1, '{"alias": "test server"}');

SELECT alias FROM "PoWA".powa_servers WHERE id = 1;

-- Test reset function
SELECT * from "PoWA".powa_reset(1);

-- Test remove server removal
BEGIN;
SELECT * from "PoWA".powa_delete_and_purge_server(1);

-- and rollback it as we later test the content of tables with a registered
-- remote server
ROLLBACK;
