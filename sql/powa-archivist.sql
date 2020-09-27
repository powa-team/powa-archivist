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

SELECT COUNT(*) > 10
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
SELECT 2, COUNT(*) > 0 FROM powa_all_relations_history_current;
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
SELECT 3, COUNT(*) > 0 FROM powa_all_relations_history_current;
SELECT 3, COUNT(*) > 0 FROM powa_statements_history_current;
SELECT 3, COUNT(*) > 0 FROM powa_statements_history_current_db;
SELECT 3, COUNT(*) >= 0 FROM powa_user_functions_history;
SELECT 3, COUNT(*) > 0 FROM powa_all_relations_history;
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
INSERT INTO powa_qualstats_src_tmp(srvid, ts, uniquequalnodeid, dbid, userid,
    qualnodeid, occurences, execution_count, nbfiltered,
    mean_err_estimate_ratio, mean_err_estimate_num,
    queryid, constvalues, quals)
    SELECT 1, now(), 1, 1, 1,
        1, 1000, 1, 0,
        0, 0,
        1, '{}', ARRAY[(1259,1,607,'i')::qual_type];
SELECT count(*) FROM powa_qualstats_src_tmp;
SELECT powa_qualstats_snapshot(1);
SELECT count(*) FROM powa_qualstats_src_tmp;
