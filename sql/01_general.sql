-- General setup
\set SHOW_CONTEXT never

-- Check the relations that aren't dumped
-- we ignore *_src_tmp are those should never be dumped
WITH ext AS (
    SELECT c.oid, c.relname
    FROM pg_depend d
    JOIN pg_extension e ON d.refclassid = 'pg_extension'::regclass
        AND e.oid = d.refobjid
        AND e.extname = 'powa'
    JOIN pg_class c ON d.classid = 'pg_class'::regclass
        AND c.oid = d.objid
    WHERE c.relkind != 'v'
),
dmp AS (
    SELECT unnest(extconfig) AS oid
    FROM pg_extension
    WHERE extname = 'powa'
)
SELECT ext.relname
FROM ext
LEFT JOIN dmp USING (oid)
WHERE dmp.oid IS NULL
AND ext.relname NOT LIKE '%src_tmp'
ORDER BY ext.relname::text COLLATE "C";

-- Check that no *_src_tmp table are dumped
WITH ext AS (
    SELECT c.oid, c.relname
    FROM pg_depend d
    JOIN pg_extension e ON d.refclassid = 'pg_extension'::regclass
        AND e.oid = d.refobjid
        AND e.extname = 'powa'
    JOIN pg_class c ON d.classid = 'pg_class'::regclass
        AND c.oid = d.objid
    WHERE c.relkind != 'v'
),
dmp AS (
    SELECT unnest(extconfig) AS oid
    FROM pg_extension
    WHERE extname = 'powa'
)
SELECT ext.relname
FROM ext
LEFT JOIN dmp USING (oid)
WHERE dmp.oid IS NOT NULL
AND ext.relname LIKE '%src_tmp'
ORDER BY ext.relname::text COLLATE "C";

-- Check for object that aren't in the "PoWA" schema
WITH ext AS (
    SELECT pg_describe_object(classid, objid, objsubid) AS descr
    FROM pg_depend d
    JOIN pg_extension e ON d.refclassid = 'pg_extension'::regclass
        AND e.oid = d.refobjid
        AND e.extname = 'powa'
)
SELECT descr
FROM ext
WHERE descr NOT LIKE '%"PoWA"%'
ORDER BY descr COLLATE "C";

-- check (mins|maxs)_in_range columns not marked as STORAGE MAIN
WITH ext AS (
    SELECT c.oid, c.relname
    FROM pg_depend d
    JOIN pg_extension e ON d.refclassid = 'pg_extension'::regclass
        AND e.oid = d.refobjid
        AND e.extname = 'powa'
    JOIN pg_class c ON d.classid = 'pg_class'::regclass
        AND c.oid = d.objid
    WHERE c.relkind != 'v'
)
SELECT ext.relname, a.attname
FROM ext
JOIN pg_attribute a ON a.attrelid = ext.oid
WHERE a.attname ~ '(mins|maxs)'
AND a.attstorage != 'm'
ORDER BY ext.relname::text COLLATE "C", a.attname::text COLLATe "C";

-- Aggregate data every 5 snapshots
SET powa.coalesce = 5;

-- test C SRFs
SELECT COUNT(*) = 0
FROM pg_database,
LATERAL "PoWA".powa_stat_user_functions(oid) f
WHERE datname = current_database();

-- on pg15+ the function is a no-op, and this function will be deprecated soon
-- anyway
SELECT COUNT(*) >= 0
FROM pg_database,
LATERAL "PoWA".powa_stat_all_rel(oid)
WHERE datname = current_database();

-- Test snapshot
SELECT 1, COUNT(*) = 0 FROM "PoWA".powa_user_functions_history_current;
SELECT 1, COUNT(*) = 0 FROM "PoWA".powa_all_tables_history_current;
SELECT 1, COUNT(*) = 0 FROM "PoWA".powa_statements_history_current;
SELECT 1, COUNT(*) = 0 FROM "PoWA".powa_statements_history_current_db;
SELECT 1, COUNT(*) = 0 FROM "PoWA".powa_user_functions_history;
SELECT 1, COUNT(*) = 0 FROM "PoWA".powa_all_tables_history;
SELECT 1, COUNT(*) = 0 FROM "PoWA".powa_statements_history;
SELECT 1, COUNT(*) = 0 FROM "PoWA".powa_statements_history;

SELECT "PoWA".powa_take_snapshot();

SELECT 2, COUNT(*) >= 0 FROM "PoWA".powa_user_functions_history_current;
SELECT 2, COUNT(*) >= 0 FROM "PoWA".powa_all_tables_history_current;
SELECT 2, COUNT(*) > 0 FROM "PoWA".powa_statements_history_current;
SELECT 2, COUNT(*) > 0 FROM "PoWA".powa_statements_history_current_db;
SELECT 2, COUNT(*) >= 0 FROM "PoWA".powa_user_functions_history;
SELECT 2, COUNT(*) = 0 FROM "PoWA".powa_all_tables_history;
SELECT 2, COUNT(*) = 0 FROM "PoWA".powa_statements_history;
SELECT 2, COUNT(*) = 0 FROM "PoWA".powa_statements_history;

SELECT "PoWA".powa_take_snapshot();
SELECT "PoWA".powa_take_snapshot();
SELECT "PoWA".powa_take_snapshot();
-- This snapshot will trigger the aggregate
SELECT "PoWA".powa_take_snapshot();

SELECT 3, COUNT(*) >= 0 FROM "PoWA".powa_user_functions_history_current;
SELECT 3, COUNT(*) >= 0 FROM "PoWA".powa_all_tables_history_current;
SELECT 3, COUNT(*) > 0 FROM "PoWA".powa_statements_history_current;
SELECT 3, COUNT(*) > 0 FROM "PoWA".powa_statements_history_current_db;
SELECT 3, COUNT(*) >= 0 FROM "PoWA".powa_user_functions_history;
SELECT 3, COUNT(*) >= 0 FROM "PoWA".powa_all_tables_history;
SELECT 3, COUNT(*) > 0 FROM "PoWA".powa_statements_history;
SELECT 3, COUNT(*) > 0 FROM "PoWA".powa_statements_history;

-- Test reset function
SELECT * from "PoWA".powa_reset(0);

SELECT 4, COUNT(*) = 0 FROM "PoWA".powa_user_functions_history_current;
SELECT 4, COUNT(*) = 0 FROM "PoWA".powa_all_tables_history_current;
SELECT 4, COUNT(*) = 0 FROM "PoWA".powa_statements_history_current;
SELECT 4, COUNT(*) = 0 FROM "PoWA".powa_statements_history_current_db;
SELECT 4, COUNT(*) = 0 FROM "PoWA".powa_user_functions_history;
SELECT 4, COUNT(*) = 0 FROM "PoWA".powa_all_tables_history;
SELECT 4, COUNT(*) = 0 FROM "PoWA".powa_statements_history;
SELECT 4, COUNT(*) = 0 FROM "PoWA".powa_statements_history;
