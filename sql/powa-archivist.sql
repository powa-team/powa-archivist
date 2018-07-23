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
SELECT COUNT(*) = 0 FROM powa_user_functions_history_current;
SELECT COUNT(*) = 0 FROM powa_all_relations_history_current;
SELECT COUNT(*) = 0 FROM powa_statements_history_current;
SELECT COUNT(*) = 0 FROM powa_statements_history_current_db;
SELECT COUNT(*) = 0 FROM powa_user_functions_history;
SELECT COUNT(*) = 0 FROM powa_all_relations_history;
SELECT COUNT(*) = 0 FROM powa_statements_history;
SELECT COUNT(*) = 0 FROM powa_statements_history;

SELECT powa_take_snapshot();

SELECT COUNT(*) >= 0 FROM powa_user_functions_history_current;
SELECT COUNT(*) > 0 FROM powa_all_relations_history_current;
SELECT COUNT(*) > 0 FROM powa_statements_history_current;
SELECT COUNT(*) > 0 FROM powa_statements_history_current_db;
SELECT COUNT(*) = 0 FROM powa_user_functions_history;
SELECT COUNT(*) = 0 FROM powa_all_relations_history;
SELECT COUNT(*) = 0 FROM powa_statements_history;
SELECT COUNT(*) = 0 FROM powa_statements_history;

SELECT powa_take_snapshot();
SELECT powa_take_snapshot();
SELECT powa_take_snapshot();
-- This snapshot will trigger the aggregate
SELECT powa_take_snapshot();

SELECT COUNT(*) = 0 FROM powa_user_functions_history_current;
SELECT COUNT(*) = 0 FROM powa_all_relations_history_current;
SELECT COUNT(*) = 0 FROM powa_statements_history_current;
SELECT COUNT(*) = 0 FROM powa_statements_history_current_db;
SELECT COUNT(*) >= 0 FROM powa_user_functions_history;
SELECT COUNT(*) > 0 FROM powa_all_relations_history;
SELECT COUNT(*) > 0 FROM powa_statements_history;
SELECT COUNT(*) > 0 FROM powa_statements_history;

-- Test reset function
SELECT * from powa_reset();

SELECT COUNT(*) = 0 FROM powa_user_functions_history_current;
SELECT COUNT(*) = 0 FROM powa_all_relations_history_current;
SELECT COUNT(*) = 0 FROM powa_statements_history_current;
SELECT COUNT(*) = 0 FROM powa_statements_history_current_db;
SELECT COUNT(*) = 0 FROM powa_user_functions_history;
SELECT COUNT(*) = 0 FROM powa_all_relations_history;
SELECT COUNT(*) = 0 FROM powa_statements_history;
SELECT COUNT(*) = 0 FROM powa_statements_history;
