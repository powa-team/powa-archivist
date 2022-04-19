-- General setup
\set SHOW_CONTEXT never

--Setup extension
CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION btree_gist;
CREATE EXTENSION powa;
-- Aggregate data every 5 snapshots
SET powa.coalesce = 5;

-- Test created ojects
SELECT * FROM powa_functions ORDER BY module, operation;

-- test C SRFs
SELECT COUNT(*) = 0
FROM pg_database,
LATERAL powa_stat_user_functions(oid) f
WHERE datname = current_database();

-- on pg15+ the function is a no-op, and this function will be deprecated soon
-- anyway
SELECT COUNT(*) >= 0
FROM pg_database,
LATERAL powa_stat_all_rel(oid)
WHERE datname = current_database();

-- Test snapshot
SELECT 1, COUNT(*) = 0 FROM powa_user_functions_history_current;
SELECT 1, COUNT(*) = 0 FROM powa_all_relations_history_current;
SELECT 1, COUNT(*) = 0 FROM powa_statements_history_current;
SELECT 1, COUNT(*) = 0 FROM powa_statements_history_current_db;
SELECT 1, COUNT(*) = 0 FROM powa_user_functions_history;
SELECT 1, COUNT(*) = 0 FROM powa_all_relations_history;
SELECT 1, COUNT(*) = 0 FROM powa_statements_history;
SELECT 1, COUNT(*) = 0 FROM powa_statements_history;

SELECT powa_take_snapshot();

SELECT 2, COUNT(*) >= 0 FROM powa_user_functions_history_current;
SELECT 2, COUNT(*) >= 0 FROM powa_all_relations_history_current;
SELECT 2, COUNT(*) > 0 FROM powa_statements_history_current;
SELECT 2, COUNT(*) > 0 FROM powa_statements_history_current_db;
SELECT 2, COUNT(*) >= 0 FROM powa_user_functions_history;
SELECT 2, COUNT(*) = 0 FROM powa_all_relations_history;
SELECT 2, COUNT(*) = 0 FROM powa_statements_history;
SELECT 2, COUNT(*) = 0 FROM powa_statements_history;

SELECT powa_take_snapshot();
SELECT powa_take_snapshot();
SELECT powa_take_snapshot();
-- This snapshot will trigger the aggregate
SELECT powa_take_snapshot();

SELECT 3, COUNT(*) >= 0 FROM powa_user_functions_history_current;
SELECT 3, COUNT(*) >= 0 FROM powa_all_relations_history_current;
SELECT 3, COUNT(*) > 0 FROM powa_statements_history_current;
SELECT 3, COUNT(*) > 0 FROM powa_statements_history_current_db;
SELECT 3, COUNT(*) >= 0 FROM powa_user_functions_history;
SELECT 3, COUNT(*) >= 0 FROM powa_all_relations_history;
SELECT 3, COUNT(*) > 0 FROM powa_statements_history;
SELECT 3, COUNT(*) > 0 FROM powa_statements_history;

-- Test reset function
SELECT * from powa_reset(0);

SELECT 4, COUNT(*) = 0 FROM powa_user_functions_history_current;
SELECT 4, COUNT(*) = 0 FROM powa_all_relations_history_current;
SELECT 4, COUNT(*) = 0 FROM powa_statements_history_current;
SELECT 4, COUNT(*) = 0 FROM powa_statements_history_current_db;
SELECT 4, COUNT(*) = 0 FROM powa_user_functions_history;
SELECT 4, COUNT(*) = 0 FROM powa_all_relations_history;
SELECT 4, COUNT(*) = 0 FROM powa_statements_history;
SELECT 4, COUNT(*) = 0 FROM powa_statements_history;

-- Check API
SELECT powa_register_server(hostname => '127.0.0.1',
    extensions => '{pg_qualstats}');
SELECT COUNT(*) FROM powa_servers;
SELECT hostname FROM powa_servers WHERE id = 1;

-- Check missing powa_statements FK for pg_qualstats doesn't prevent snapshot
INSERT INTO public.powa_qualstats_src_tmp(srvid, ts, uniquequalnodeid, dbid, userid,
    qualnodeid, occurences, execution_count, nbfiltered,
    mean_err_estimate_ratio, mean_err_estimate_num,
    queryid, constvalues, quals)
    SELECT 1, now(), 1, 1, 1,
        1, 1000, 1, 0,
        0, 0,
        123456789, '{}', ARRAY[(1259,1,607,'i')::qual_type];
SELECT count(*) FROM public.powa_qualstats_src_tmp;
SELECT public.powa_qualstats_snapshot(1);
SELECT count(*) FROM public.powa_qualstats_src_tmp;
SELECT count(*) FROM public.powa_qualstats_quals_history_current WHERE srvid = 1;

-- Check snapshot of regular quals
INSERT INTO public.powa_databases(srvid, oid, datname, dropped)
    VALUES (1, 16384, 'postgres', NULL);
INSERT INTO public.powa_statements(srvid, queryid, dbid, userid, query)
    VALUES(1, 123456789, 16384, 10, 'query with qual');
INSERT INTO public.powa_qualstats_src_tmp(srvid, ts, uniquequalnodeid, dbid, userid,
    qualnodeid, occurences, execution_count, nbfiltered,
    mean_err_estimate_ratio, mean_err_estimate_num,
    queryid, constvalues, quals)
    SELECT 1, now(), 1, 16384, 10,
        1, 1000, 1, 0,
        0, 0,
        123456789, '{}', ARRAY[(1259,1,607,'i')::qual_type];
SELECT count(*) FROM public.powa_qualstats_src_tmp;
SELECT public.powa_qualstats_snapshot(1);
SELECT count(*) FROM public.powa_qualstats_src_tmp;
SELECT count(*) FROM public.powa_qualstats_quals_history_current WHERE srvid = 1;

-- activate / deactivate extension
SELECT * FROM public.powa_functions ORDER BY srvid, module, operation, function_name;
SELECT * FROM public.powa_activate_extension(1, 'pg_stat_kcache');
SELECT * FROM public.powa_activate_extension(1, 'some_extension');
SELECT * FROM public.powa_functions ORDER BY srvid, module, operation, function_name;
SELECT * FROM public.powa_deactivate_extension(1, 'pg_stat_kcache');
SELECT * FROM public.powa_deactivate_extension(1, 'some_extension');
SELECT * FROM public.powa_functions ORDER BY srvid, module, operation, function_name;

SELECT alias FROM public.powa_servers WHERE id = 1;
SELECT * FROM public.powa_configure_server(0, '{"somekey": "someval"}');
SELECT * FROM public.powa_configure_server(1, '{"somekey": "someval"}');
SELECT * FROM public.powa_configure_server(1, '{"alias": "test server"}');

SELECT alias FROM public.powa_servers WHERE id = 1;

-- Test reset function
SELECT * from powa_reset(1);

-- Check remote server removal
DELETE FROM public.powa_servers WHERE id = 1;
