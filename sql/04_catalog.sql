-- General setup
\set SHOW_CONTEXT never

-- Check the source query retrieval
SELECT * FROM "PoWA".powa_catalog_src_query('pg_class', 90600);

-- check pg_database catalog snapshot.  We just insert 2 databases to make
-- things simpler
INSERT INTO "PoWA".powa_catalog_database_src_tmp
    SELECT 1, *
    FROM "PoWA".powa_catalog_database_src(0) src
    WHERE src.datname = current_database();
INSERT INTO "PoWA".powa_catalog_database_src_tmp
    SELECT 1, *
    FROM "PoWA".powa_catalog_database_src(0) src
    WHERE src.datname != current_database()
    AND src.datname != 'test'
    LIMIT 1;
SELECT "PoWA".powa_catalog_database_snapshot(1);

-- there shouldn't be a refresh time
SELECT count(*) FILTER (WHERE last_refresh IS NULL) = 2 AS ok,
       count(*) FILTER (WHERE last_refresh IS NOT NULL) AS nb_with_refresh
FROM "PoWA".powa_catalog_databases
WHERE srvid = 1;

-- check the rest of the per-database catalog snapshot
DO $_$
DECLARE
    v_ok boolean;
    v_num integer;
    v_nb_rec integer;
    v_nb_rec2 integer;
    v_catname text;
    v_prefix text;
    v_src_tmp text;
    v_query text;
BEGIN
    SELECT setting INTO v_num
    FROM pg_settings
    WHERE name = 'server_version_num';

    FOR v_catname IN SELECT catname FROM "PoWA".powa_catalogs ORDER BY priority
    LOOP
        -- get the necessary object name
        SELECT 'powa_catalog_' || replace(v_catname, 'pg_', '') INTO v_prefix;
        SELECT v_prefix || '_src_tmp'
            INTO v_src_tmp;
        SELECT "PoWA".powa_catalog_src_query(v_catname, v_num)
            INTO v_query;

        -- there shouldn't be any stored data for this catalog
        EXECUTE format('SELECT count(*) = 0, count(*) FROM "PoWA".%I', v_prefix)
            INTO v_ok, v_nb_rec;

        IF NOT v_ok THEN
            RAISE WARNING 'catalog % already has stored data (% rows) in %',
                v_catname, v_nb_rec, v_prefix;
        END IF;

        -- manually insert some data for this catalog for 2 different
        -- databases, with the same content
        EXECUTE format('INSERT INTO "PoWA".%I
            SELECT 1 AS srvid, d.oid AS dbid, src.*
            FROM (%s) src
            CROSS JOIN "PoWA".powa_catalog_databases d',
            v_src_tmp, v_query);

        -- snapshot the given catalog
        PERFORM "PoWA".powa_catalog_generic_snapshot(1, v_catname);

        -- there should now be stored data for this catalog
        EXECUTE format('SELECT count(*) > 0, count(*) FROM "PoWA".%I', v_prefix)
            INTO v_ok, v_nb_rec;

        IF NOT v_ok THEN
            RAISE WARNING 'catalog % does not have stored data in %',
                v_catname, v_prefix;
        END IF;

        -- source table should now be empty
        EXECUTE format('SELECT count(*) = 0, count(*) FROM "PoWA".%I', v_src_tmp)
            INTO v_ok, v_nb_rec;

        IF NOT v_ok THEN
            RAISE WARNING 'source table % for catalog % still has % rows',
                v_src_tmp, v_catname, v_nb_rec;
        END IF;

        -- There should be records for 2 databases
        EXECUTE format('SELECT count(DISTINCT dbid) = 2, count(DISTINCT dbid)
            FROM "PoWA".%I', v_prefix)
            INTO v_ok, v_nb_rec;

        IF NOT v_ok THEN
            RAISE WARNING 'table % for catalog % does not have record for 2 databases (found %)',
                v_prefix, v_catname, v_nb_rec;
        END IF;

        -- both databases should have the same number of records
        EXECUTE format('SELECT (
            SELECT count(*) FROM "PoWA".%1$I c
            JOIN pg_database d ON c.dbid = d.oid
            WHERE srvid = 1 AND d.datname = current_database()
            ) = (
            SELECT count(*) FROM "PoWA".%1$I c
            JOIN pg_database d ON c.dbid = d.oid
            WHERE srvid = 1 AND d.datname != current_database()
            ),(
            SELECT count(*) FROM "PoWA".%1$I c
            JOIN pg_database d ON c.dbid = d.oid
            WHERE srvid = 1 AND d.datname = current_database()
            ), (
            SELECT count(*) FROM "PoWA".%1$I c
            JOIN pg_database d ON c.dbid = d.oid
            WHERE srvid = 1 AND d.datname != current_database()
            )
            ', v_prefix, v_nb_rec, v_nb_rec2) INTO v_ok;

        IF NOT v_ok THEN
            RAISE WARNING 'table % for catalog % does not have the same number of records for the 2 databases: % vs %',
                v_prefix, v_catname, v_nb_rec, v_nb_rec2;
        END IF;

        -- the refresh time should have been saved only if this is pg_class
        -- catalog
        SELECT count(*) INTO v_nb_rec
        FROM "PoWA".powa_catalog_databases
        WHERE srvid = 1
        AND last_refresh IS NULL;
        IF v_catname = 'pg_class' THEN
            IF v_nb_rec != 0 THEN
                RAISE WARNING 'last_refresh was not saved when processing pg_class, % records without a refresh time',
                    v_nb_rec;
            END IF;
        ELSE
            IF v_nb_rec = 0 THEN
                RAISE WARNING 'last_refresh was saved when processing %, % records without a refresh time',
                    v_catname, v_nb_rec;
            END IF;
        END IF;

        -- snapshot the given catalog again without source data, nothing should
        -- happen
        PERFORM "PoWA".powa_catalog_generic_snapshot(1, v_catname);

        -- there should still be stored data for this catalog
        EXECUTE format('SELECT count(*) > 0, count(*) FROM "PoWA".%I', v_prefix)
            INTO v_ok, v_nb_rec;

        IF NOT v_ok THEN
            RAISE WARNING 'catalog % does not have stored data in %',
                v_catname, v_prefix;
        END IF;

        -- re-add some data in the src table, but for 1 db only, and snapshot
        -- again
        EXECUTE format('INSERT INTO "PoWA".%I
            SELECT 1 AS srvid, d.oid AS dbid, src.*
            FROM (%s) src
            JOIN "PoWA".powa_catalog_databases d
                ON d.datname = current_database()',
            v_src_tmp, v_query);
        PERFORM "PoWA".powa_catalog_generic_snapshot(1, v_catname);

        -- both databases should still have the same number of records
        EXECUTE format('SELECT (
            SELECT count(*) FROM "PoWA".%1$I c
            JOIN pg_database d ON c.dbid = d.oid
            WHERE srvid = 1 AND d.datname = current_database()
            ) = (
            SELECT count(*) FROM "PoWA".%1$I c
            JOIN pg_database d ON c.dbid = d.oid
            WHERE srvid = 1 AND d.datname != current_database()
            ),(
            SELECT count(*) FROM "PoWA".%1$I c
            JOIN pg_database d ON c.dbid = d.oid
            WHERE srvid = 1 AND d.datname = current_database()
            ), (
            SELECT count(*) FROM "PoWA".%1$I c
            JOIN pg_database d ON c.dbid = d.oid
            WHERE srvid = 1 AND d.datname != current_database()
            )
            ', v_prefix, v_nb_rec, v_nb_rec2) INTO v_ok;

        IF NOT v_ok THEN
            RAISE WARNING 'table % for catalog % does not have the same number of records for the 2 databases: % vs %',
                v_prefix, v_catname, v_nb_rec, v_nb_rec2;
        END IF;

        -- re-add some data in the src table to later test the reset
        EXECUTE format('INSERT INTO "PoWA".%I
            SELECT 1 AS srvid, d.oid AS dbid, src.*
            FROM (%s) src
            CROSS JOIN "PoWA".powa_catalog_databases d',
            v_src_tmp, v_query);
    END LOOP;
END;
$_$ LANGUAGE plpgsql;

SELECT catname, substr(query_source, 1, 12) AS query_source, tmp_table,
    array_upper(excluded_dbnames, 1) AS nb_excluded
FROM "PoWA".powa_catalog_functions(1, 150000)
WHERE catname = 'pg_class';

-- Check the refresh interval filtering
INSERT INTO "PoWA".powa_catalog_databases(srvid, oid, datname, last_refresh)
    VALUES (1, 1, 'test', now() - interval '1 month');

-- default interval should exclude test database
WITH e AS (SELECT DISTINCT unnest(excluded_dbnames) AS excluded
FROM "PoWA".powa_catalog_functions(1, 150000))
SELECT coalesce(array_agg(excluded), '{}') AS excluded_dbnames
FROM e
WHERE excluded = 'test';
-- 15 days interval should not exclude test database
WITH e AS (SELECT DISTINCT unnest(excluded_dbnames) AS excluded
FROM "PoWA".powa_catalog_functions(1, 150000, '15 days'))
SELECT coalesce(array_agg(excluded), '{}') AS excluded_dbnames
FROM e
WHERE excluded = 'test';
