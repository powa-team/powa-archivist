-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION powa" to load this file. \quit

SET LOCAL statement_timeout = 0;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL standard_conforming_strings = on;
SET LOCAL client_min_messages = warning;
SET LOCAL search_path = pg_catalog;

-- This table cannot be dumped as postgres doesn't offer a way to restore only
-- some columns.  Even if it was possible, we couldn't assume that users would
-- choose to restore the roles too.
CREATE TABLE @extschema@.powa_roles (
    powa_role name NOT NULL PRIMARY KEY,
    rolname name,
    CHECK (powa_role IN ('powa_admin', 'powa_read_all_data',
                         'powa_read_all_metrics', 'powa_write_all_data',
                         'powa_snapshot', 'powa_signal_backend'))
);

INSERT INTO @extschema@.powa_roles
    (powa_role,               rolname) VALUES
    ('powa_admin',            NULL),
    ('powa_read_all_data',    NULL),
    ('powa_read_all_metrics', NULL),
    ('powa_write_all_data',   NULL),
    ('powa_snapshot',         NULL),
    ('powa_signal_backend',   NULL);

CREATE FUNCTION @extschema@.setup_powa_roles(
    IN reuse_existing_role bool default FALSE,
    IN admin_role text DEFAULT 'powa_admin',
    IN read_all_data_role text DEFAULT 'powa_read_all_data',
    IN read_all_metrics_role text DEFAULT 'powa_read_all_metrics',
    IN write_all_data_role text DEFAULT 'powa_write_all_data',
    IN snapshot_role text DEFAULT 'powa_snapshot',
    IN signal_backend_role text DEFAULT 'powa_signal_backend'
) RETURNS void
AS $$
DECLARE
    v_roles name[];
    v_role name;
    v_rec record;
    v_nb integer;
BEGIN
    v_roles = ARRAY [admin_role, read_all_data_role, read_all_metrics_role,
                     write_all_data_role, snapshot_role, signal_backend_role];

    -- Preliminary sanity checks for the reuse_existing_role case
    IF (reuse_existing_role) THEN
        SELECT count(*) INTO v_nb
        FROM @extschema@.powa_roles
        WHERE rolname IS NOT NULL;

        IF (v_nb != 0) THEN
            RAISE EXCEPTION 'Cannot chang existing roles';
        END IF;

        FOR v_rec in SELECT *
                     FROM pg_catalog.pg_roles
                     WHERE rolname = ANY (v_roles)
        LOOP
            IF v_rec.rolsuper THEN
                RAISE EXCEPTION 'Existing role % is a superuser', v_rec.rolname;
            ELSIF v_rec.rolcreaterole THEN
                RAISE EXCEPTION 'Existing role % can create role', v_rec.rolname;
            ELSIF v_rec.rolcreatedb THEN
                RAISE EXCEPTION 'Existing role % can create db', v_rec.rolname;
            ELSIF v_rec.rolcanlogin THEN
                RAISE EXCEPTION 'Existing role % can login', v_rec.rolname;
            ELSIF v_rec.rolreplication THEN
                RAISE EXCEPTION 'Existing role % is a replication role', v_rec.rolname;
            ELSIF v_rec.rolbypassrls THEN
                RAISE EXCEPTION 'Existing role % can bypass RLS', v_rec.rolname;
            END IF;
        END LOOP;
    END IF;

    -- Update the powa_roles information.  This will be rollbacked if any issue
    -- is later found with it
    UPDATE @extschema@.powa_roles
        SET rolname = CASE
            WHEN powa_role = 'powa_admin' THEN admin_role
            WHEN powa_role = 'powa_read_all_data' THEN read_all_data_role
            WHEN powa_role = 'powa_read_all_metrics' THEN read_all_metrics_role
            WHEN powa_role = 'powa_write_all_data' THEN write_all_data_role
            WHEN powa_role = 'powa_snapshot' THEN snapshot_role
            WHEN powa_role = 'powa_signal_backend' THEN signal_backend_role
        END;

    IF (reuse_existing_role) THEN
        SELECT count(*) INTO v_nb
        FROM @extschema@.powa_roles p
        LEFT JOIN pg_catalog.pg_roles r USING (rolname)
        WHERE r.rolname IS NULL;

        IF (v_nb != 0) THEN
            RAISE EXCEPTION 'Cannot reuse existing powa roles unless all roles already exist';
        END IF;
    ELSE
        SELECT count(*) INTO v_nb
        FROM pg_catalog.pg_roles
        WHERE rolname = ANY (v_roles);

        IF v_nb != 0 THEN
            RAISE EXCEPTION 'Some roles already exists';
        END IF;

        FOREACH v_role IN ARRAY v_roles LOOP
            EXECUTE format('CREATE ROLE %I NOLOGIN', v_role);
        END LOOP;
    END IF;

    -- Final add all the required ACL based on the up-to-date powa_roles table
    PERFORM @extschema@.powa_grant();
END;
$$ LANGUAGE plpgsql STRICT
SET search_path = pg_catalog; /* end if setup_powa_roles */

CREATE TABLE @extschema@.powa_servers (
    id serial PRIMARY KEY,
    hostname text NOT NULL,
    alias text,
    port integer NOT NULL,
    username text NOT NULL,
    password text,
    dbname text NOT NULL,
    frequency integer NOT NULL default 300 CHECK (frequency = -1 OR frequency >= 5),
    powa_coalesce integer NOT NULL default 100 CHECK (powa_coalesce >= 5),
    retention interval NOT NULL default '1 day'::interval,
    allow_ui_connection boolean NOT NULL default true,
    version text,
    UNIQUE (hostname, port),
    UNIQUE(alias)
);
INSERT INTO @extschema@.powa_servers VALUES (0, '', '<local>', 0, '', NULL, '', -1, 100, '0 second');

CREATE TABLE @extschema@.powa_extensions (
    extname text NOT NULL PRIMARY KEY,
    external boolean NOT NULL default true,
    added_manually boolean NOT NULL default true
);

INSERT INTO @extschema@.powa_extensions
    (extname,              external, added_manually) VALUES
    ('powa',               false,    false),
    ('hypopg',             false,    false),
    ('pg_stat_statements', false,    false),
    ('pg_qualstats',       false,    false),
    ('pg_stat_kcache',     false,    false),
    ('pg_track_settings',  true,     false),
    ('pg_wait_sampling',   false,    false);

CREATE TABLE @extschema@.powa_extension_functions (
    extname text NOT NULL,
    operation text NOT NULL,
    function_name text NOT NULL,
    query_source text default NULL,
    query_cleanup text default NULL,
    added_manually boolean NOT NULL default true,
    priority numeric NOT NULL default 10,
    PRIMARY KEY (extname, operation, function_name),
    CHECK (operation IN ('snapshot','aggregate','purge','reset')),
    FOREIGN KEY (extname) REFERENCES @extschema@.powa_extensions(extname)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO @extschema@.powa_extension_functions
    (extname,              operation,   function_name,                         query_source,                     query_cleanup,                                added_manually, priority) VALUES
    ('pg_stat_statements', 'snapshot',  'powa_databases_snapshot',             'powa_databases_src',             NULL,                                         false,          -3),
    ('pg_stat_statements', 'snapshot',  'powa_statements_snapshot',            'powa_statements_src',            NULL,                                         false,          -2),
    ('pg_stat_statements', 'aggregate', 'powa_statements_aggregate',           NULL,                             NULL,                                         false,          default),
    ('pg_stat_statements', 'purge',     'powa_statements_purge',               NULL,                             NULL,                                         false,          default),
    ('pg_stat_statements', 'purge',     'powa_databases_purge',                NULL,                             NULL,                                         false,          default),
    ('pg_stat_statements', 'reset',     'powa_statements_reset',               NULL,                             NULL,                                         false,          default),
    ('pg_qualstats',       'snapshot',  'powa_qualstats_snapshot',             'powa_qualstats_src',             'SELECT {pg_qualstats}.pg_qualstats_reset()', false,          default),
    ('pg_qualstats',       'aggregate', 'powa_qualstats_aggregate',            NULL,                             NULL,                                         false,          default),
    ('pg_qualstats',       'purge',     'powa_qualstats_purge',                NULL,                             NULL,                                         false,          default),
    ('pg_qualstats',       'reset',     'powa_qualstats_reset',                NULL,                             NULL,                                         false,          default),
    ('pg_stat_kcache',     'snapshot',  'powa_kcache_snapshot',                'powa_kcache_src',                NULL,                                         false,          -1),
    ('pg_stat_kcache',     'aggregate', 'powa_kcache_aggregate',               NULL,                             NULL,                                         false,          default),
    ('pg_stat_kcache',     'purge',     'powa_kcache_purge',                   NULL,                             NULL,                                         false,          default),
    ('pg_stat_kcache',     'reset',     'powa_kcache_reset',                   NULL,                             NULL,                                         false,          default),
    ('pg_track_settings',  'snapshot',  'pg_track_settings_snapshot_settings', 'pg_track_settings_settings_src', NULL,                                         false,          default),
    ('pg_track_settings',  'snapshot',  'pg_track_settings_snapshot_rds',      'pg_track_settings_rds_src',      NULL,                                         false,          default),
    ('pg_track_settings',  'snapshot',  'pg_track_settings_snapshot_reboot',   'pg_track_settings_reboot_src',   NULL,                                         false,          default),
    ('pg_track_settings',  'reset',     'pg_track_settings_reset',             NULL,                             NULL,                                         false,          default),
    ('pg_wait_sampling',   'snapshot',  'powa_wait_sampling_snapshot',         'powa_wait_sampling_src',         NULL,                                         false,          default),
    ('pg_wait_sampling',   'aggregate', 'powa_wait_sampling_aggregate',        NULL,                             NULL,                                         false,          default),
    ('pg_wait_sampling',   'purge',     'powa_wait_sampling_purge',            NULL,                             NULL,                                         false,          default),
    ('pg_wait_sampling',   'reset',     'powa_wait_sampling_reset',            NULL,                             NULL,                                         false,          default);

CREATE TABLE @extschema@.powa_extension_config (
    srvid integer NOT NULL,
    extname text NOT NULL,
    version text,
    enabled bool NOT NULL default true,
    added_manually boolean NOT NULL default true,
    PRIMARY KEY (srvid, extname),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (extname) REFERENCES @extschema@.powa_extensions(extname)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO @extschema@.powa_extension_config (srvid, extname, added_manually) VALUES
    (0, 'powa', false),
    (0, 'pg_stat_statements', false);

-- This is for cluster-wide core views, we don't support custom datasources
-- here.
CREATE TABLE @extschema@.powa_modules (
    module text NOT NULL PRIMARY KEY,
    min_version integer NOT NULL DEFAULT 0
);

-- we only manually insert data in this table (and the other related tables)
-- for modules with a custom implementation.  For the rest of the modules, this
-- is done automatically with calls to powa_generic_module_setup()
INSERT INTO @extschema@.powa_modules (module) VALUES
    ('pg_database'),
    ('pg_role');

CREATE TABLE @extschema@.powa_module_config (
    srvid integer NOT NULL,
    module text NOT NULL,
    enabled bool NOT NULL default true,
    added_manually boolean NOT NULL default true,
    PRIMARY KEY (srvid, module),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (module) REFERENCES @extschema@.powa_modules(module)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO @extschema@.powa_module_config (srvid, module, added_manually) VALUES
    (0, 'pg_database', false),
    (0, 'pg_role', false);

CREATE TABLE @extschema@.powa_module_functions (
    module text NOT NULL,
    operation text NOT NULL,
    function_name text NOT NULL,
    query_source text default NULL,
    PRIMARY KEY (module, operation),
    CHECK (operation IN ('snapshot','aggregate','purge','reset')),
    FOREIGN KEY (module) REFERENCES @extschema@.powa_modules (module)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO @extschema@.powa_module_functions (module, operation, function_name, query_source) VALUES
    ('pg_database',      'snapshot',  'powa_catalog_database_snapshot', 'powa_catalog_database_src'),
    ('pg_database',      'reset',     'powa_catalog_database_reset',    NULL),
    ('pg_role',          'snapshot',  'powa_catalog_role_snapshot',     'powa_catalog_role_src'),
    ('pg_role',          'reset',     'powa_catalog_role_reset',        NULL);

CREATE VIEW @extschema@.powa_functions AS
    SELECT srvid, 'extension' AS kind, extname AS name, operation, external,
        function_name, query_source, query_cleanup, enabled, priority
    FROM @extschema@.powa_extensions e
    JOIN @extschema@.powa_extension_functions f USING (extname)
    JOIN @extschema@.powa_extension_config c USING (extname)
    UNION ALL
    SELECT srvid, 'module' AS kind, module AS name, operation, false,
        function_name, query_source, NULL, enabled, 100
    FROM @extschema@.powa_modules m
    JOIN @extschema@.powa_module_functions f USING (module)
    JOIN @extschema@.powa_module_config c USING (module)
    WHERE current_setting('server_version_num')::int >= m.min_version;

CREATE TABLE @extschema@.powa_db_modules (
    db_module text NOT NULL PRIMARY KEY,
    tmp_table text NOT NULL,
    external boolean NOT NULL default true,
    added_manually boolean NOT NULL default true
);

INSERT INTO @extschema@.powa_db_modules (db_module, tmp_table, external, added_manually) VALUES
    ('pg_stat_all_indexes', '@extschema@.powa_all_indexes_src_tmp', false, false),
    ('pg_stat_all_tables', '@extschema@.powa_all_tables_src_tmp', false, false),
    ('pg_stat_user_functions', '@extschema@.powa_user_functions_src_tmp', false, false);

-- No default rows for this table as this is a remote-server only feature.
-- A NULL dbnames means that the module is activated for all databases,
-- otherwise the module is only activated for the specified database names.
CREATE TABLE @extschema@.powa_db_module_config (
    srvid integer NOT NULL,
    db_module text NOT NULL,
    dbnames text[],
    enabled boolean NOT NULL default true,
    PRIMARY KEY (srvid, db_module),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (db_module) REFERENCES @extschema@.powa_db_modules(db_module)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE @extschema@.powa_db_module_functions (
    db_module text NOT NULL,
    operation text NOT NULL,
    function_name text NOT NULL,
    added_manually boolean NOT NULL default true,
    priority numeric NOT NULL default 10,
    CHECK (operation IN ('snapshot','aggregate','purge','reset')),
    PRIMARY KEY (db_module, operation),
    FOREIGN KEY (db_module) REFERENCES @extschema@.powa_db_modules(db_module)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO @extschema@.powa_db_module_functions
    (db_module,                operation,   function_name,                   added_manually) VALUES
    ('pg_stat_all_tables',     'snapshot',  'powa_all_tables_snapshot',      false),
    ('pg_stat_all_tables',     'aggregate', 'powa_all_tables_aggregate',     false),
    ('pg_stat_all_tables',     'purge',     'powa_all_tables_purge',         false),
    ('pg_stat_all_tables',     'reset',     'powa_all_tables_reset',         false),
    ('pg_stat_all_indexes',    'snapshot',  'powa_all_indexes_snapshot',     false),
    ('pg_stat_all_indexes',    'aggregate', 'powa_all_indexes_aggregate',    false),
    ('pg_stat_all_indexes',    'purge',     'powa_all_indexes_purge',        false),
    ('pg_stat_all_indexes',    'reset',     'powa_all_indexes_reset',        false),
    ('pg_stat_user_functions', 'snapshot',  'powa_user_functions_snapshot',  false),
    ('pg_stat_user_functions', 'aggregate', 'powa_user_functions_aggregate', false),
    ('pg_stat_user_functions', 'purge',     'powa_user_functions_purge',     false),
    ('pg_stat_user_functions', 'reset',     'powa_user_functions_reset',     false);

CREATE TABLE @extschema@.powa_db_module_src_queries (
    db_module text NOT NULL,
    min_version integer NOT NULL,
    added_manually boolean NOT NULL default true,
    query_source text NOT NULL,
    PRIMARY KEY (db_module, min_version),
    FOREIGN KEY (db_module) REFERENCES @extschema@.powa_db_modules(db_module)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO @extschema@.powa_db_module_src_queries
    (db_module, min_version, added_manually, query_source) VALUES
    -- pg_stat_all_tables
    ('pg_stat_all_tables', 0, false,
     'SELECT relid, pg_table_size(relid) AS tbl_size,
        seq_scan, NULL AS last_seq_scan, seq_tup_read,
        idx_scan, NULL AS last_idx_scan, idx_tup_fetch,
        n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, 0 AS n_tup_newpage_upd,
        n_live_tup, n_dead_tup, n_mod_since_analyze, 0 AS n_ins_since_vacuum,
        last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
        vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
        heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit,
        toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit
     FROM pg_catalog.pg_stat_all_tables st
     JOIN pg_catalog.pg_statio_all_tables sit USING (relid)'),
    -- pg_stat_all_tables pg13+, n_ins_since_vacuum added
    ('pg_stat_all_tables', 130000, false,
     'SELECT relid, pg_table_size(relid) AS tbl_size,
        seq_scan, NULL AS last_seq_scan, seq_tup_read,
        idx_scan, NULL AS last_idx_scan, idx_tup_fetch,
        n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, 0 AS n_tup_newpage_upd,
        n_live_tup, n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum,
        last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
        vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
        heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit,
        toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit
     FROM pg_catalog.pg_stat_all_tables st
     JOIN pg_catalog.pg_statio_all_tables sit USING (relid)'),
    -- pg_stat_all_tables pg16+, last_seq_scan, last_idx_scan and
    -- n_tup_newpage_upd added
    ('pg_stat_all_tables', 160000, false,
     'SELECT relid, pg_table_size(relid) AS tbl_size,
        seq_scan,  last_seq_scan, seq_tup_read,
        idx_scan,  last_idx_scan, idx_tup_fetch,
        n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, n_tup_newpage_upd,
        n_live_tup, n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum,
        last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
        vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
        heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit,
        toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit
     FROM pg_catalog.pg_stat_all_tables st
     JOIN pg_catalog.pg_statio_all_tables sit USING (relid)'),
    -- pg_stat_all_indexes
    ('pg_stat_all_indexes', 0, false,
     'SELECT si.relid, indexrelid, pg_table_size(indexrelid) AS idx_size,
        idx_scan, NULL AS last_idx_scan,
        idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit
      FROM pg_catalog.pg_stat_all_indexes si
      JOIN pg_catalog.pg_statio_all_indexes sit USING (indexrelid)'),
    -- pg_stat_all_indexes pg16+
    ('pg_stat_all_indexes', 160000, false,
     'SELECT si.relid, indexrelid, pg_table_size(indexrelid) AS idx_size,
        idx_scan, last_idx_scan,
        idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit
      FROM pg_catalog.pg_stat_all_indexes si
      JOIN pg_catalog.pg_statio_all_indexes sit USING (indexrelid)'),
    -- pg_stat_user_functions
    ('pg_stat_user_functions', 0, false,
     'SELECT funcid, calls, total_time, self_time
      FROM pg_catalog.pg_stat_user_functions');

CREATE FUNCTION @extschema@.powa_db_functions(_srvid integer,
                                              _server_version_num integer)
RETURNS TABLE (srvid integer, db_module text, operation text,
               function_name text, dbnames text[], query_source text,
               tmp_table text, enabled bool, priority numeric)
AS $_$
    SELECT srvid, db_module, operation, function_name, dbnames, query_source,
        CASE WHEN f.operation = 'snapshot'
            THEN tmp_table
            ELSE NULL
        END AS tmp_table, enabled, priority
    FROM @extschema@.powa_db_modules m
    JOIN @extschema@.powa_db_module_functions f USING (db_module)
    JOIN @extschema@.powa_db_module_config c USING (db_module)
    LEFT JOIN LATERAL (
        SELECT query_source
        FROM @extschema@.powa_db_module_src_queries q
        WHERE q.db_module = m.db_module
        AND min_version <= _server_version_num
        ORDER BY min_version DESC
        LIMIT 1
    ) q ON f.operation = 'snapshot'
    WHERE srvid = _srvid;
$_$ LANGUAGE sql
SET search_path = pg_catalog;

CREATE VIEW @extschema@.powa_all_functions AS
    SELECT *
    FROM @extschema@.powa_functions
    UNION ALL
    SELECT srvid, 'db_module' AS kind, db_module AS name, operation, external,
        function_name, NULL as query_source, NULL AS query_cleanup, enabled,
        priority
    FROM @extschema@.powa_db_modules pdm
    JOIN @extschema@.powa_db_module_config pdmc USING (db_module)
    JOIN @extschema@.powa_db_module_functions pdmf USING (db_module);

CREATE TABLE @extschema@.powa_catalogs (
    catname text NOT NULL PRIMARY KEY,
    tmp_table text NOT NULL,
    priority numeric NOT NULL default 10
);

-- pg_class is last as we use it to record the last_refresh timestamp
INSERT INTO @extschema@.powa_catalogs
    (catname,        tmp_table,                        priority) VALUES
    ('pg_class',     'powa_catalog_class_src_tmp',     99),
    ('pg_attribute', 'powa_catalog_attribute_src_tmp', DEFAULT),
    ('pg_namespace', 'powa_catalog_namespace_src_tmp', DEFAULT),
    ('pg_type',      'powa_catalog_type_src_tmp',      DEFAULT),
    ('pg_collation', 'powa_catalog_collation_src_tmp', DEFAULT),
    ('pg_proc',      'powa_catalog_proc_src_tmp',      DEFAULT),
    ('pg_language',  'powa_catalog_language_src_tmp',  DEFAULT)
    ;

CREATE TABLE @extschema@.powa_catalog_src_queries (
    catname text NOT NULL,
    min_version integer NOT NULL,
    query_source text NOT NULL,
    PRIMARY KEY (catname, min_version),
    FOREIGN KEY (catname) REFERENCES @extschema@.powa_catalogs(catname)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

-- we exclude all temporary objects as they're unlikely to be helpful and might
-- bloat the underlying tables.
INSERT INTO @extschema@.powa_catalog_src_queries
    (catname, min_version, query_source) VALUES
    -- pg_class
    ('pg_class', 0,
     $$SELECT oid, relname::text AS relname, relnamespace, relpages, reltuples,
        reltoastrelid, relisshared, relpersistence, relkind, relnatts,
        false AS relrowsecurity, false AS relforcerowsecurity, relreplident,
        false AS relispartition,
        reloptions,
        NULL::text AS relpartbound
      FROM pg_catalog.pg_class
      WHERE relpersistence != 't'$$),
    -- pg_class 9.5+, relrowsecurity and relforcerowsecurity added
    ('pg_class', 90500,
     $$SELECT oid, relname::text AS relname, relnamespace, relpages, reltuples,
        reltoastrelid, relisshared, relpersistence, relkind, relnatts,
        relrowsecurity, relforcerowsecurity, relreplident,
        false AS relispartition,
        reloptions,
        NULL::text AS relpartbound
      FROM pg_catalog.pg_class
      WHERE relpersistence != 't'$$),
    -- pg_class pg10+, relispartition and repartbound added
    ('pg_class', 100000,
     $$SELECT oid, relname::text AS relname, relnamespace, relpages, reltuples,
        reltoastrelid, relisshared, relpersistence, relkind, relnatts,
        relrowsecurity, relforcerowsecurity, relreplident,
        relispartition,
        reloptions,
        pg_get_expr(relpartbound, oid) AS relpartbound
      FROM pg_catalog.pg_class
      WHERE relpersistence != 't'$$),
    -- pg_attribute
    ('pg_attribute', 0,
     $$SELECT attrelid, attname::text AS attname, atttypid, attlen, attnum,
        ''::"char" AS attcompression, attnotnull, atthasdef,
        false AS atthasmissing, ''::"char" AS attidentity,
        ''::"char" AS attgenerated, attstattarget, attcollation, attoptions,
        attfdwoptions
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
      WHERE a.attnum > 0
      AND NOT attisdropped
      AND c.relpersistence != 't'$$),
    -- pg_attribute pg10+, attidentity added
    ('pg_attribute', 100000,
     $$SELECT attrelid, attname::text AS attname, atttypid, attlen, attnum,
        ''::"char" AS attcompression, attnotnull, atthasdef,
        false AS atthasmissing, attidentity,
        ''::"char" AS attgenerated, attstattarget, attcollation, attoptions,
        attfdwoptions
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
      WHERE a.attnum > 0
      AND NOT attisdropped
      AND c.relpersistence != 't'$$),
    -- pg_attribute pg11+, atthasmissing added
    ('pg_attribute', 110000,
     $$SELECT attrelid, attname::text AS attname, atttypid, attlen, attnum,
        ''::"char" AS attcompression, attnotnull, atthasdef,
        atthasmissing, attidentity,
        ''::"char" AS attgenerated, attstattarget, attcollation, attoptions,
        attfdwoptions
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
      WHERE a.attnum > 0
      AND NOT attisdropped
      AND c.relpersistence != 't'$$),
    -- pg_attribute pg12+, attgenerated added
    ('pg_attribute', 120000,
     $$SELECT attrelid, attname::text AS attname, atttypid, attlen, attnum,
        ''::"char" AS attcompression, attnotnull, atthasdef,
        atthasmissing, attidentity,
        attgenerated, attstattarget, attcollation, attoptions,
        attfdwoptions
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
      WHERE a.attnum > 0
      AND NOT attisdropped
      AND c.relpersistence != 't'$$),
    -- pg_attribute pg14+, attcompression added
    ('pg_attribute', 140000,
     $$SELECT attrelid, attname::text AS attname, atttypid, attlen, attnum,
        attcompression, attnotnull, atthasdef,
        atthasmissing, attidentity,
        attgenerated, attstattarget, attcollation, attoptions,
        attfdwoptions
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
      WHERE a.attnum > 0
      AND NOT attisdropped
      AND c.relpersistence != 't'$$),
    -- pg_namespace
    ('pg_namespace', 0,
     $$SELECT oid, nspname::text AS nspname
      FROM pg_catalog.pg_namespace$$),
    -- pg_type
    ('pg_type', 0,
     $$SELECT oid, typname::text AS typname, typnamespace, typlen, typbyval,
        typtype, typcategory, typispreferred, typisdefined, typdelim,
        typrelid, typelem, typarray, typalign, typstorage, typnotnull,
        typbasetype, typtypmod, typndims, typcollation, typdefault
      FROM pg_catalog.pg_type$$),
    -- pg_collation
    ('pg_collation', 0,
     $$SELECT oid, collname::text AS collname, collnamespace,
        CASE WHEN collname = 'default' THEN 'd' ELSE 'c' END AS collprovider,
        true AS collisdeterministic,
        pg_encoding_to_char(collencoding) AS collencoding, collcollate,
        collctype,
        NULL::text AS colliculocale, NULL::text AS collicurules,
        NULL::text AS collversion
      FROM pg_catalog.pg_collation$$),
    -- pg_collation pg10+, collprovider and collversion added
    ('pg_collation', 100000,
     $$SELECT oid, collname::text AS collname, collnamespace, collprovider,
        true AS collisdeterministic,
        pg_encoding_to_char(collencoding) AS collencoding, collcollate,
        collctype,
        NULL::text AS colliculocale, NULL::text AS collicurules,
        collversion
      FROM pg_catalog.pg_collation$$),
    -- pg_collation pg12+, collisdeterministic added
    ('pg_collation', 120000,
     $$SELECT oid, collname::text AS collname, collnamespace, collprovider,
        collisdeterministic,
        pg_encoding_to_char(collencoding) AS collencoding, collcollate,
        collctype,
        NULL::text AS colliculocale, NULL::text AS collicurules,
        collversion
      FROM pg_catalog.pg_collation$$),
    -- pg_collation pg15+, colliculocale added
    ('pg_collation', 150000,
     $$SELECT oid, collname::text AS collname, collnamespace, collprovider,
        collisdeterministic,
        pg_encoding_to_char(collencoding) AS collencoding, collcollate,
        collctype,
        colliculocale, NULL::text AS collicurules,
        collversion
      FROM pg_catalog.pg_collation$$),
    -- pg_collation pg16+, collicurules added
    ('pg_collation', 160000,
     $$SELECT oid, collname::text AS collname, collnamespace, collprovider,
        collisdeterministic,
        pg_encoding_to_char(collencoding) AS collencoding, collcollate,
        collctype,
        colliculocale, collicurules,
        collversion
      FROM pg_catalog.pg_collation$$),
    -- pg_proc
    ('pg_proc', 0,
     $$SELECT oid, proname::text AS proname, oid::regprocedure AS regprocedure,
    pronamespace, prolang, procost,
        prorows, provariadic,
        CASE
            WHEN proisagg THEN 'a'::"char"
            WHEN proiswindow THEN 'w'::"char"
            ELSE 'f'::"char"
        END AS prokind, prosecdef, proleakproof, proisstrict, proretset,
        provolatile, 'u'::"char" AS proparallel, pronargs, prorettype,
        proargtypes, prosrc, proconfig
      FROM pg_catalog.pg_proc$$),
    -- pg_proc pg 9.6+, proparallel added
    ('pg_proc', 90600,
     $$SELECT oid, proname::text AS proname, oid::regprocedure AS regprocedure,
        pronamespace, prolang, procost,
        prorows, provariadic,
        CASE
            WHEN proisagg THEN 'a'::"char"
            WHEN proiswindow THEN 'w'::"char"
            ELSE 'f'::"char"
        END AS prokind, prosecdef, proleakproof, proisstrict, proretset,
        provolatile, proparallel, pronargs, prorettype,
        proargtypes,
        prosrc,
        proconfig
      FROM pg_catalog.pg_proc$$),
    -- pg_proc pg11+, prokind added replacing proisagg and proiswindow
    ('pg_proc', 110000,
     $$SELECT oid, proname::text AS proname, oid::regprocedure AS regprocedure,
        pronamespace, prolang, procost,
        prorows, provariadic,
        prokind, prosecdef, proleakproof, proisstrict, proretset,
        provolatile, proparallel, pronargs, prorettype,
        proargtypes,
        prosrc,
        proconfig
      FROM pg_catalog.pg_proc$$),
    -- pg_proc pg14+, prosqlbody added
    ('pg_proc', 140000,
     $$SELECT oid, proname::text AS proname, oid::regprocedure AS regprocedure,
        pronamespace, prolang, procost,
        prorows, provariadic,
        prokind, prosecdef, proleakproof, proisstrict, proretset,
        provolatile, proparallel, pronargs, prorettype,
        proargtypes,
        CASE WHEN prosqlbody IS NOT NULL THEN
            pg_catalog.pg_get_function_sqlbody(oid)
        ELSE
            prosrc
        END AS prosrc,
        proconfig
      FROM pg_catalog.pg_proc$$),
    -- pg_language
    ('pg_language', 0,
     $$SELECT oid, lanname::text AS lanname, lanispl, lanpltrusted
      FROM pg_catalog.pg_language$$)
    ;

CREATE FUNCTION @extschema@.powa_catalog_src_query(_catname text,
                                                   _server_version_num integer)
RETURNS text
AS $_$
    SELECT query_source
    FROM @extschema@.powa_catalog_src_queries
    WHERE catname = _catname
    AND min_version <= _server_version_num
    ORDER BY min_version DESC
    LIMIT 1;
$_$ LANGUAGE sql
SET search_path = pg_catalog;

CREATE TABLE @extschema@.powa_catalog_databases (
    srvid integer NOT NULL,
    oid oid NOT NULL,
    datname text NOT NULL,
    last_refresh timestamp with time zone,
    PRIMARY KEY (srvid, oid),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE @extschema@.powa_catalog_roles (
    srvid integer NOT NULL,
    oid oid NOT NULL,
    rolname text NOT NULL,
    rolsuper boolean NOT NULL,
    rolinherit boolean NOT NULL,
    rolcreaterole boolean NOT NULL,
    rolcreatedb boolean NOT NULL,
    rolcanlogin boolean NOT NULL,
    rolreplication boolean NOT NULL,
    rolbypassrls boolean NOT NULL,
    PRIMARY KEY (srvid, oid),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE FUNCTION @extschema@.powa_catalog_functions(_srvid integer,
    _server_version_num integer, _refresh_interval interval DEFAULT '1 year')
RETURNS TABLE (catname text, query_source text, tmp_table text,
               excluded_dbnames text[])
AS $_$
    SELECT catname,
        src_query AS query_source,
        '@extschema@.'
            || quote_ident('powa_catalog_'
            || replace(catname, 'pg_', '') || '_src_tmp') AS tmp_table,
        coalesce(array_agg(datname)
            FILTER (WHERE (last_refresh + _refresh_interval) >= now()), '{}')
        AS excluded_dbnames
    FROM @extschema@.powa_catalogs c
    LEFT JOIN @extschema@.powa_catalog_databases d ON d.srvid = _srvid
    LEFT JOIN LATERAL (
        SELECT @extschema@.powa_catalog_src_query(catname, _server_version_num)
    ) f(src_query) ON (true)
    GROUP BY catname, src_query;
$_$ LANGUAGE sql
SET search_path = pg_catalog;

CREATE TABLE @extschema@.powa_catalog_class (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    oid oid NOT NULL,
    relname text NOT NULL,
    relnamespace oid NOT NULL,
    relpages integer NOT NULL,
    reltuples real NOT NULL,
    reltoastrelid oid NOT NULL,
    relisshared bool NOT NULL,
    relpersistence "char" NOT NULL,
    relkind "char" NOT NULL,
    relnatts smallint NOT NULL,
    relrowsecurity boolean NOT NULL,
    relforcerowsecurity boolean NOT NULL,
    relreplident "char" NOT NULL,
    relispartition boolean NOT NULL,
    reloptions text[],
    relpartbound text,
    PRIMARY KEY (srvid, dbid, oid),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES @extschema@.powa_catalog_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE @extschema@.powa_catalog_attribute (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    attrelid oid NOT NULL,
    attname text NOT NULL,
    atttypid oid NOT NULL,
    attlen smallint NOT NULL,
    attnum smallint NOT NULL,
    attcompression "char" NOT NULL,
    attnotnull boolean NOT NULL,
    atthasdef boolean NOT NULL,
    atthasmissing boolean NOT NULL,
    attidentity "char" NOT NULL,
    attgenerated "char" NOT NULL,
    attstattarget smallint,
    attcollation oid NOT NULL,
    attoptions text[],
    attfdwoptions text[],
    PRIMARY KEY (srvid, dbid, attrelid, attnum),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES @extschema@.powa_catalog_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE @extschema@.powa_catalog_namespace (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    oid oid NOT NULL,
    nspname text NOT NULL,
    PRIMARY KEY (srvid, dbid, oid),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES @extschema@.powa_catalog_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE @extschema@.powa_catalog_type (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    oid oid NOT NULL,
    typname text NOT NULL,
    typnamespace oid NOT NULL,
    typlen smallint NOT NULL,
    typbyval boolean NOT NULL,
    typtype "char" NOT NULL,
    typcategory "char" NOT NULL,
    typispreferred boolean NOT NULL,
    typisdefined boolean NOT NULL,
    typdelim "char" NOT NULL,
    typrelid oid NOT NULL,
    typelem oid NOT NULL,
    typarray oid NOT NULL,
    typalign "char" NOT NULL,
    typstorage "char" NOT NULL,
    typnotnull boolean NOT NULL,
    typbasetype oid NOT NULL,
    typtypmod integer NOT NULL,
    typndims integer NOT NULL,
    typcollation oid NOT NULL,
    typdefault text,
    PRIMARY KEY (srvid, dbid, oid),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES @extschema@.powa_catalog_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE @extschema@.powa_catalog_collation (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    oid oid NOT NULL,
    collname text NOT NULL,
    collnamespace oid NOT NULL,
    collprovider "char" NOT NULL,
    collisdeterministic boolean NOT NULL,
    collencoding text NOT NULL,
    collcollate text,
    collctype text,
    colliculocale text,
    collicurules text,
    collversion text,
    PRIMARY KEY (srvid, dbid, oid),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES @extschema@.powa_catalog_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

-- we also store oid::regprocedure for an easy way to have an unambiguous
-- identifier
CREATE TABLE @extschema@.powa_catalog_proc (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    oid oid NOT NULL,
    proname text NOT NULL,
    regprocedure text NOT NULL,
    pronamespace oid NOT NULL,
    prolang oid NOT NULL,
    procost real NOT NULL,
    prorows real NOT NULL,
    provariadic oid NOT NULL,
    prokind "char" NOT NULL,
    prosecdef boolean NOT NULL,
    proleakproof boolean NOT NULL,
    proisstrict boolean NOT NULL,
    proretset boolean NOT NULL,
    provolatile "char" NOT NULL,
    proparallel "char" NOT NULL,
    pronargs smallint NOT NULL,
    prorettype oid NOT NULL,
    proargtypes oidvector NOT NULL,
    prosrc text NOT NULL,
    proconfig text[],
    PRIMARY KEY (srvid, dbid, oid),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES @extschema@.powa_catalog_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE @extschema@.powa_catalog_language (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    oid oid NOT NULL,
    lanname text NOT NULL,
    lanispl boolean NOT NULL,
    lanpltrusted boolean NOT NULL,
    PRIMARY KEY (srvid, dbid, oid),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES @extschema@.powa_catalog_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE @extschema@.powa_snapshot_metas (
    srvid integer PRIMARY KEY,
    coalesce_seq bigint NOT NULL default (1),
    snapts timestamp with time zone NOT NULL default '-infinity'::timestamptz,
    aggts timestamp with time zone NOT NULL default '-infinity'::timestamptz,
    purgets timestamp with time zone NOT NULL default '-infinity'::timestamptz,
    errors text[],
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
INSERT INTO @extschema@.powa_snapshot_metas (srvid) VALUES (0);

CREATE TABLE @extschema@.powa_databases (
    srvid   integer NOT NULL,
    oid     oid,
    datname name,
    dropped timestamp with time zone,
    PRIMARY KEY (srvid, oid),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE @extschema@.powa_statements (
    srvid integer NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    query text NOT NULL,
    last_present_ts timestamptz NULL DEFAULT now(),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES @extschema@.powa_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

ALTER TABLE ONLY @extschema@.powa_statements
    ADD CONSTRAINT powa_statements_pkey PRIMARY KEY (srvid, queryid, dbid, userid);

CREATE INDEX powa_statements_dbid_idx ON @extschema@.powa_statements(srvid, dbid);
CREATE INDEX powa_statements_userid_idx ON @extschema@.powa_statements(userid);
CREATE INDEX powa_statements_mru_idx ON @extschema@.powa_statements (last_present_ts);

CREATE FUNCTION @extschema@.powa_stat_user_functions(IN dbid oid, OUT funcid oid,
    OUT calls bigint,
    OUT total_time double precision,
    OUT self_time double precision)
    RETURNS SETOF record STABLE
    LANGUAGE c COST 100
AS '$libdir/powa', 'powa_stat_user_functions';

CREATE FUNCTION @extschema@.powa_stat_all_rel(IN dbid oid,
    OUT relid oid,
    OUT numscan bigint,
    OUT tup_returned bigint,
    OUT tup_fetched bigint,
    OUT n_tup_ins bigint,
    OUT n_tup_upd bigint,
    OUT n_tup_del bigint,
    OUT n_tup_hot_upd bigint,
    OUT n_liv_tup bigint,
    OUT n_dead_tup bigint,
    OUT n_mod_since_analyze bigint,
    OUT blks_read bigint,
    OUT blks_hit bigint,
    OUT last_vacuum timestamp with time zone,
    OUT vacuum_count bigint,
    OUT last_autovacuum timestamp with time zone,
    OUT autovacuum_count bigint,
    OUT last_analyze timestamp with time zone,
    OUT analyze_count bigint,
    OUT last_autoanalyze timestamp with time zone,
    OUT autoanalyze_count bigint)
    RETURNS SETOF record STABLE
    LANGUAGE c COST 100
AS '$libdir/powa', 'powa_stat_all_rel';

-------------------------------
-- data sources generic support
-------------------------------
CREATE FUNCTION @extschema@.powa_generic_datatype_setup(
    _datasource text,
    _cols text[],
    _extra jsonb DEFAULT '{}',
    _need_operators boolean default true
)
RETURNS void AS
$$
DECLARE
    i integer;
    v_prefix text;
    v_sql text;
    v_record_name text;
    v_colname text;
    v_coltype text;
    v_extra text;
    v_kind text;
    c_no_agg text[];
    v_has_no_agg_col bool;
    c_no_minmax text[];
    v_has_no_minmax_col bool;
BEGIN
    IF quote_ident(_datasource) != _datasource THEN
        RAISE EXCEPTION '% require quoting, which is not supported',
                        _datasource;
    END IF;

    -- we don't put any fields for any of those datatypes in the *_history_db
    -- records, as aggregating them per-database or computing a rate wouldn't
    -- make sense
    c_no_agg := ARRAY['timestamp with time zone', 'timestamptz'];
    -- Similarly, don't put any field with datatypes that don't support min/max
    -- in the mins_in_range / maxs_in_range records
    c_no_minmax := ARRAY['xid', 'boolean'];

    -- we loop over the whole process in case we find some columns with
    -- some specific datatypes.  For those we need to create a *_history_db
    -- version datatype without such columns, as we can't do a per-db
    -- aggregation of such data.
    -- Similarly, we need to create a specific datatype for the min/max
    -- aggregated records for datatypes that don't support min/max.
    v_has_no_agg_col := false;
    v_has_no_minmax_col := false;
    FOREACH v_prefix IN ARRAY ARRAY['_history', '_history_db'] LOOP
        -- we only need _db version of the infrastructure if we skipped some
        -- columns
        EXIT WHEN v_prefix = '_history_db' AND NOT v_has_no_agg_col;

        v_record_name := _datasource || v_prefix || '_record';

        -- first, create a main record type
        -- as this is the first iteration over the columns, make sure that none
        -- of them require quoting
        v_sql := format('CREATE TYPE @extschema@.%I AS (
ts timestamp with time zone',
                        v_record_name);
        FOR i IN 1..array_upper(_cols, 1) LOOP
            v_colname := _cols[i][1];
            v_coltype := _cols[i][2];

            -- the *_history_db type version is the same as the *_history
            -- without the fields of datatype we don't aggregate
            CONTINUE WHEN v_prefix = '_history_db'
                     AND v_coltype = ANY (c_no_agg);

            IF quote_ident(v_colname) != v_colname THEN
                RAISE EXCEPTION '% require quoting, which is not supported',
                                v_colname;
            END IF;

            -- datasources should only use a few of native system types
            IF v_coltype NOT IN ('timestamp with time zone', 'oid',  'bigint',
                                 'integer', 'numeric', 'double precision',
                                 'text', 'inet', 'xid', 'pg_lsn', 'interval',
                                 'boolean')
            THEN
                RAISE EXCEPTION 'invalid data type % for col %.%',
                                v_coltype, _datasource, v_colname;
            END IF;

            IF v_coltype = ANY (c_no_minmax) THEN
                v_has_no_minmax_col := true;
            END IF;

            v_sql := v_sql || ',' || chr(10) || v_colname || ' ' || v_coltype;
        END LOOP;
        v_sql := v_sql || ')';
        EXECUTE v_sql;

        -- Create a specific min/max record if needed
        IF v_prefix = '_history' AND v_has_no_minmax_col THEN
            v_sql := format('CREATE TYPE @extschema@.%I AS (
ts timestamp with time zone',
                            v_record_name || '_minmax');
            FOR i IN 1..array_upper(_cols, 1) LOOP
                v_colname := _cols[i][1];
                v_coltype := _cols[i][2];

                CONTINUE WHEN v_coltype = ANY (c_no_minmax);

                v_sql := v_sql || ',' || chr(10) || v_colname || ' ' || v_coltype;
            END LOOP;
            v_sql := v_sql || ')';
            EXECUTE v_sql;
        END IF;

        CONTINUE WHEN NOT _need_operators;

        -- add a *history_rate and a *history_diff type, and remember if we saw
        -- (and skipped) any timestamptz column
        FOREACH v_kind IN ARRAY ARRAY['diff', 'rate'] LOOP
            CONTINUE WHEN v_prefix = '_history_db';

            IF v_kind = 'diff' THEN
                v_extra := 'intvl interval';
            ELSE
                v_extra := 'sec integer';
            END IF;
            v_sql := format('CREATE TYPE @extschema@.%I AS ('
                            || chr(10)
                            || '%s',
                            _datasource || v_prefix || '_' || v_kind, v_extra);
            FOR i IN 1..array_upper(_cols, 1) LOOP
                v_colname := _cols[i][1];
                v_coltype := _cols[i][2];

                IF v_coltype = ANY (c_no_agg)
                THEN
                    v_has_no_agg_col = true;
                    CONTINUE;
                END IF;

                v_colname = coalesce(_extra->v_kind->'colname'->>v_colname,
                                     v_colname);

                IF v_kind = 'rate' THEN
                    v_colname := v_colname ||
                        coalesce(_extra->v_kind->'suffix'->>v_colname,
                                 '_per_sec');
                    IF v_coltype != 'numeric' THEN
                        v_coltype := 'double precision';
                    END IF;
                END IF;

                v_sql := v_sql || ', ' || chr(10)
                    || quote_ident(v_colname) || ' ' || v_coltype;
            END LOOP;
            v_sql := v_sql || ')';
            EXECUTE v_sql;
        END LOOP;

        -- add a *_mi() function
        v_sql := format('CREATE FUNCTION @extschema@.%1$I(
a @extschema@.%2$I,
b @extschema@.%2$I)
RETURNS @extschema@.%3$I AS
$_$
DECLARE
    res @extschema@.%3$I;
BEGIN
    res.intvl = a.ts - b.ts;',
                        _datasource || v_prefix || '_mi',
                        v_record_name,
                        -- the datatype is the same for the _db version
                        _datasource || '_history_diff');
        FOR i in 1..array_upper(_cols, 1) LOOP
            CONTINUE WHEN _cols[i][2] = ANY (c_no_agg);
            v_colname := _cols[i][1];
            v_colname = coalesce(_extra->'mi'->'colname'->>v_colname,
                                 v_colname);

            v_sql := v_sql || chr(10)
                || format('    res.%1$I = a.%1$I - b.%1$I;', v_colname);
        END LOOP;
        v_sql := v_sql || chr(10) || chr(10) || '    return res;'
            || chr(10) || 'END;' || chr(10) || '$_$'
            || chr(10) || 'LANGUAGE plpgsql IMMUTABLE STRICT';
        EXECUTE v_sql;

        -- add a "-" operator
        v_sql := format('CREATE OPERATOR @extschema@.- ('
                        'PROCEDURE = @extschema@.%1$I,'
                        'LEFTARG = @extschema@.%2$I,'
                        'RIGHTARG = @extschema@.%2$I)',
                        _datasource || v_prefix || '_mi',
                        v_record_name);
        EXECUTE v_sql;

        -- add a *_div() function
        v_sql := format('CREATE FUNCTION @extschema@.%1$I(
a @extschema@.%2$I,
b @extschema@.%2$I)
RETURNS @extschema@.%3$I AS
$_$
DECLARE
    res @extschema@.%3$I;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;',
                        _datasource || v_prefix || '_div',
                        v_record_name,
                        -- the datatype is the same for the _db version
                        _datasource || '_history_rate');
        FOR i in 1..array_upper(_cols, 1) LOOP
            CONTINUE WHEN _cols[i][2] = ANY (c_no_agg);

            v_colname := _cols[i][1];
            v_extra := coalesce(_extra->'rate'->'colname'->>v_colname,
                                v_colname);
            v_extra := v_extra ||
                coalesce(_extra->'rate'->'suffix'->>v_colname, '_per_sec');

            v_sql := v_sql || chr(10)
                || format('    res.%1$I = (a.%2$I - b.%2$I)::double precision / sec;',
                          v_extra, v_colname);
    END LOOP;
    v_sql := v_sql || '

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT';
    EXECUTE v_sql;

    -- add a "/" operator
    v_sql := format('CREATE OPERATOR @extschema@./ ('
                    'PROCEDURE = @extschema@.%1$I,'
                    'LEFTARG = @extschema@.%2$I,'
                    'RIGHTARG = @extschema@.%2$I)',
                    _datasource || v_prefix || '_div',
                    v_record_name);
    EXECUTE v_sql;
    END LOOP;
END;
$$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_generic_datatype_setup */

CREATE FUNCTION @extschema@.powa_generic_module_setup(_pg_module text,
                                                      _counter_cols text[],
                                                      _nullable text[] DEFAULT '{}',
                                                      _need_operators boolean default true,
                                                      _key_cols text[] DEFAULT '{}',
                                                      _key_nullable boolean DEFAULT false,
                                                      _min_version integer DEFAULT 0)
RETURNS void AS
$$
DECLARE
    i integer;
    v_nb integer;
    v_module text;
    v_sql text;
    v_colname text;
    v_coltype text;
    v_kind text;
    v_null text;
    v_has_no_minmax_col bool;
    v_suffix text;
    v_accum text;
BEGIN
    IF quote_ident(_pg_module) != _pg_module THEN
        RAISE EXCEPTION '% require quoting, which is not supported',
                         _pg_module;
    END IF;

    IF _pg_module !~ '^pg_' THEN
        RAISE EXCEPTION '% is not a postgres module', _pg_module;
    END IF;

    v_module := regexp_replace(_pg_module, '^pg', 'powa');

    -- declare the module and its configuration
    INSERT INTO @extschema@.powa_modules VALUES (_pg_module, _min_version);
    INSERT INTO @extschema@.powa_module_config VALUES (0, _pg_module, false);
    INSERT INTO @extschema@.powa_module_functions VALUES
        (_pg_module, 'snapshot',  v_module || '_snapshot',  v_module || '_src'),
        (_pg_module, 'aggregate', v_module || '_aggregate', NULL),
        (_pg_module, 'purge',     v_module || '_purge',     NULL),
        (_pg_module, 'reset',     v_module || '_reset',     NULL);

    -- create the underlying record datatype(s) and operators if needed
    EXECUTE @extschema@.powa_generic_datatype_setup(v_module, _counter_cols,
                                                    _need_operators => _need_operators);
    -- create the *_src_tmp unlogged table
    v_sql := format('CREATE UNLOGGED TABLE @extschema@.%I (
    srvid integer NOT NULL,
    ts timestamp with time zone NOT NULL',
                    v_module || '_src_tmp');

    -- iterate over the key columns first
    IF _key_nullable THEN
        v_null := '';
    ELSE
        v_null := ' NOT NULL';
    END IF;
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_colname := _key_cols[i][1];
        v_coltype := _key_cols[i][2];

        -- as this is the first iteration over the columns, make sure that none
        -- of them require quoting
        IF quote_ident(v_colname) != v_colname THEN
            RAISE EXCEPTION '% require quoting, which is not supported',
                            v_colname;
        END IF;

        -- key columns should only use a few of native system types
        IF v_coltype NOT IN ('boolean', 'integer', 'name', 'oid', 'text')
        THEN
            RAISE EXCEPTION 'invalid data type % for key col %.%',
                            v_coltype, v_module, v_colname;
        END IF;

        IF v_colname = ANY (_nullable) THEN
            RAISE EXCEPTION 'invalid nullable info for key col %.%',
                            v_module, v_colname;
        END IF;

        v_sql := v_sql || ',' || chr(10) || format('    %I %s%s',
                                                   v_colname, v_coltype, v_null);
    END LOOP;

    -- then iterate over the counter columns
    v_has_no_minmax_col := false;
    FOR i IN 1..array_upper(_counter_cols, 1) LOOP
        v_colname := _counter_cols[i][1];
        v_coltype := _counter_cols[i][2];

        IF v_coltype IN ('xid', 'boolean') THEN
            v_has_no_minmax_col := true;
        END IF;

        -- as this is the first iteration over the columns, make sure that none
        -- of them require quoting
        IF quote_ident(v_colname) != v_colname THEN
            RAISE EXCEPTION '% require quoting, which is not supported',
                            v_colname;
        END IF;

        -- datasources should only use a few of native system types
        IF v_coltype NOT IN ('timestamp with time zone', 'oid',  'bigint',
                             'integer', 'numeric', 'double precision',
                             'text', 'inet', 'xid', 'pg_lsn', 'interval',
                             'boolean')
        THEN
            RAISE EXCEPTION 'invalid data type % for col %.%',
                            v_coltype, v_module, v_colname;
        END IF;

        IF v_colname = ANY (_nullable) THEN
            _nullable := array_remove(_nullable, v_colname);
            v_null := '';
        ELSE
            v_null := ' NOT NULL';
        END IF;
        v_sql := v_sql || ',' || chr(10) || format('    %I %s%s',
                                                   v_colname, v_coltype,
                                                   v_null);
    END LOOP;

    IF array_upper(_nullable, 1) IS NOT NULL THEN
        RAISE EXCEPTION 'Columns % declared as nullable, but not found in the '
                        'list of columns', _nullable;
    END IF;

    v_sql := v_sql || ')';
    EXECUTE v_sql;

    -- create the *_history table and its index
    v_suffix := v_module || '_history_record';
    IF v_has_no_minmax_col THEN
        v_suffix := v_suffix || '_minmax';
    END IF;
    v_sql := format('CREATE TABLE @extschema@.%1$I (
    srvid integer NOT NULL,', v_module || '_history');

    IF _key_nullable THEN
        v_null := '';
    ELSE
        v_null := ' NOT NULL';
    END IF;
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_sql := v_sql || format('
    %I %s%s,', _key_cols[i][1], _key_cols[i][2], v_null);
    END LOOP;

    v_sql := v_sql || format('
    coalesce_range tstzrange NOT NULL,
    records @extschema@.%2$I[] NOT NULL,
    mins_in_range @extschema@.%3$I NOT NULL,
    maxs_in_range @extschema@.%3$I NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.%1$I ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.%1$I ALTER COLUMN maxs_in_range SET STORAGE MAIN;
CREATE INDEX %4$I ON @extschema@.%1$I USING gist(srvid, coalesce_range);',
                    v_module || '_history', v_module || '_history_record',
                    v_suffix, v_module || '_history_ts');
    EXECUTE v_sql;

    -- and the *_history_current table and index
    v_accum = 'srvid integer NOT NULL,';
    IF _key_nullable THEN
        v_null := '';
    ELSE
        v_null := ' NOT NULL';
    END IF;
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_colname := _key_cols[i][1];
        v_coltype := _key_cols[i][2];

        v_accum := v_accum || format('
    %I %s%s,', v_colname, v_coltype, v_null);
    END LOOP;
    v_sql := format('CREATE TABLE @extschema@.%1$I (
    %3$s
    record @extschema@.%2$I NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.%1$I (srvid);',
    v_module || '_history_current', v_module || '_history_record', v_accum);
    EXECUTE v_sql;

    -- make sure the *_history and  *_history_current tables are dumped
    PERFORM pg_catalog.pg_extension_config_dump('@extschema@.' || v_module || '_history','');
    PERFORM pg_catalog.pg_extension_config_dump('@extschema@.' || v_module || '_history_current','');

    -- create the *_snapshot function
    v_accum := '_srvid';
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_colname := _key_cols[i][1];

        v_accum := v_accum || format(', %I', v_colname);
    END LOOP;
    v_sql := format('CREATE FUNCTION @extschema@.%1$I (_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format(''@extschema@.%%I(%%s)'',
                                 %1$L, _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format(''running %%s'', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert background writer statistics
    WITH rel AS (
        SELECT *
        FROM @extschema@.%2$I(_srvid)
    )
    INSERT INTO @extschema@.%3$I
        SELECT %4$s,
        ROW(ts',
                  v_module || '_snapshot',
                  v_module || '_src',
                  v_module || '_history_current',
                  v_accum);

    FOR i IN 1..array_upper(_counter_cols, 1) LOOP
        v_colname := _counter_cols[i][1];

        IF i > 1 AND (i - 1) % 3 = 0 THEN
            v_sql := v_sql || ',' || chr(10) || '            ';
        ELSE
            v_sql := v_sql || ', ';
        END IF;
        v_sql := v_sql || v_colname;
    END LOOP;

    v_sql := v_sql || format(')::@extschema@.%I AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format(''%%s - rowcount: %%s'',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.%I WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ language plpgsql;',
                    v_module || '_history_record', v_module || '_src_tmp');
    EXECUTE v_sql;

    -- create the *_aggregate function
    v_accum := 'srvid';
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_colname := _key_cols[i][1];
        v_coltype := _key_cols[i][2];

        v_sql := v_sql || format(',
            (record).%I', v_colname);

        v_accum := v_accum || ', ' || v_colname;
    END LOOP;
    v_sql := format('CREATE FUNCTION @extschema@.%1$I(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format(''@extschema@.%%I(%%s)'',
                                 %1$L, _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format(''running %%s'', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate %3$s history table
    INSERT INTO @extschema@.%2$I
        SELECT %4$s,
            tstzrange(min((record).ts), max((record).ts),''[]''),
            array_agg(record)',
                    v_module || '_aggregate', v_module || '_history',
                    v_module, v_accum);

    FOREACH v_kind IN ARRAY ARRAY['min', 'max'] LOOP
        v_sql := v_sql || format(',
            ROW(%s((record).ts)', v_kind);

        FOR i IN 1..array_upper(_counter_cols, 1) LOOP
            v_colname := _counter_cols[i][1];
            v_coltype := _counter_cols[i][2];

            IF v_coltype NOT IN ('xid', 'boolean') THEN
                v_sql := v_sql || format(',
                    %s((record).%I)', v_kind, v_colname);
            END IF;
        END LOOP;

        v_sql := v_sql || format(')::@extschema@.%I',
                                 v_suffix);
    END LOOP;

    v_sql := v_sql || format('
        FROM @extschema@.%1$I
        WHERE srvid = _srvid
        GROUP BY %2$s;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format(''%%s - rowcount: %%s'',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.%1$I WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql',
                    v_module || '_history_current', v_accum);
    EXECUTE v_sql;

    -- create the *_purge function
    v_sql := format('CREATE FUNCTION @extschema@.%1$I(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format(''@extschema@.%%I(%%s)'',
                                 %1$L, _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format(''running %%s'', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.%2$I
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format(''%%s - rowcount: %%s'',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql
        ',
                    v_module || '_purge', v_module || '_history');
    EXECUTE v_sql;

    -- create the *_reset function
    v_sql := format('CREATE FUNCTION @extschema@.%1$I(_srvid integer)
RETURNS boolean AS $function$
BEGIN
    PERFORM @extschema@.powa_log(''Resetting %2$I('' || _srvid || '')'');
    DELETE FROM @extschema@.%2$I WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log(''Resetting %3$I('' || _srvid || '')'');
    DELETE FROM @extschema@.%3$I WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log(''Resetting %4$I('' || _srvid || '')'');
    DELETE FROM @extschema@.%4$I WHERE srvid = _srvid;

    RETURN true;
END;
$function$ LANGUAGE plpgsql',
                    v_module || '_reset', v_module || '_history',
                    v_module || '_history_current', v_module || '_src_tmp');
    EXECUTE v_sql;
END;
$$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_generic_module_setup */

SELECT @extschema@.powa_generic_datatype_setup('powa_statements',
$${
{calls, bigint}, {total_exec_time, double precision}, {rows, bigint},
{shared_blks_hit, bigint}, {shared_blks_read, bigint},
{shared_blks_dirtied, bigint}, {shared_blks_written, bigint},
{local_blks_hit, bigint}, {local_blks_read, bigint},
{local_blks_dirtied, bigint}, {local_blks_written, bigint},
{temp_blks_read, bigint}, {temp_blks_written, bigint},
{shared_blk_read_time, double precision}, {shared_blk_write_time, double precision},
{local_blk_read_time, double precision}, {local_blk_write_time, double precision},
{temp_blk_read_time, double precision}, {temp_blk_write_time, double precision},
{plans, bigint}, {total_plan_time, double precision},
{wal_records, bigint}, {wal_fpi, bigint}, {wal_bytes, numeric},
{jit_functions, bigint}, {jit_generation_time, double precision},
{jit_inlining_count, bigint}, {jit_inlining_time, double precision},
{jit_optimization_count, bigint}, {jit_optimization_time, double precision},
{jit_emission_count, bigint}, {jit_emission_time, double precision},
{jit_deform_count, bigint}, {jit_deform_time, double precision}
}$$,
'{"rate": {
    "colname": { "total_exec_time": "runtime", "total_plan_time": "plantime"}
}}');

SELECT @extschema@.powa_generic_datatype_setup('powa_user_functions',
$${
{calls, bigint}, {total_time, double precision}, {self_time, double precision}
}$$);

-- powa_all_indexes combines info from pg_stat_all_indexes and
-- pg_statio_all_indexes
SELECT @extschema@.powa_generic_datatype_setup('powa_all_indexes',
$${
{idx_size, bigint},
{idx_scan, bigint}, {last_idx_scan, timestamp with time zone},
{idx_tup_read, bigint}, {idx_tup_fetch, bigint},
{idx_blks_read, bigint}, {idx_blks_hit, bigint}
}$$);

-- powa_all_tablees combines info from pg_stat_all_tablees and
-- pg_statio_all_tablees
SELECT @extschema@.powa_generic_datatype_setup('powa_all_tables',
$${
{tbl_size, bigint},
{seq_scan, bigint}, {last_seq_scan, timestamp with time zone},
{seq_tup_read, bigint}, {idx_scan, bigint},
{last_idx_scan, timestamp with time zone},
{n_tup_ins, bigint}, {n_tup_upd, bigint}, {n_tup_del, bigint},
{n_tup_hot_upd, bigint}, {n_tup_newpage_upd, bigint},
{n_liv_tup, bigint}, {n_dead_tup, bigint},
{n_mod_since_analyze, bigint}, {n_ins_since_vacuum, bigint},
{last_vacuum, timestamp with time zone},
{last_autovacuum, timestamp with time zone},
{last_analyze, timestamp with time zone},
{last_autoanalyze, timestamp with time zone},
{vacuum_count, bigint}, {autovacuum_count, bigint},
{analyze_count, bigint}, {autoanalyze_count, bigint},
{heap_blks_read, bigint}, {heap_blks_hit, bigint},
{idx_blks_read, bigint}, {idx_blks_hit, bigint},
{toast_blks_read, bigint}, {toast_blks_hit, bigint},
{tidx_blks_read, bigint}, {tidx_blks_hit, bigint}
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_replication_slots',
$${
{cur_txid, xid}, {current_lsn, pg_lsn},
{active, boolean}, {active_pid, integer},
{slot_xmin, xid}, {catalog_xmin, xid}, {restart_lsn, pg_lsn},
{confirmed_flush_lsn, pg_lsn}, {wal_status, text}, {safe_wal_size, bigint},
{two_phase, boolean}, {conflicting, boolean}
}$$,
$${
cur_txid, active, active_pid,
slot_xmin, catalog_xmin, restart_lsn, confirmed_flush_lsn, wal_status,
safe_wal_size, two_phase, conflicting
}$$,
_key_cols => $${
{slot_name, text}, {plugin, text}, {slot_type, text}, {datoid, oid},
{temporary, boolean}
}$$,
_key_nullable => true,
_min_version => 130000
);

SELECT @extschema@.powa_generic_module_setup('pg_stat_activity',
$${
{cur_txid, xid},
{datid, oid}, {pid, integer}, {leader_pid, integer}, {usesysid, oid},
{application_name, text}, {client_addr, inet},
{backend_start, timestamp with time zone},
{xact_start, timestamp with time zone},
{query_start, timestamp with time zone},
{state_change, timestamp with time zone},
{state, text}, {backend_xid, xid}, {backend_xmin, xid},
{query_id, bigint}, {backend_type, text}
}$$,
$${
cur_txid, datid, leader_pid, usesysid, client_addr, xact_start, query_start,
state_change, state, backend_xid, backend_xmin, query_id, backend_type
}$$,
_need_operators => false);

SELECT @extschema@.powa_generic_module_setup('pg_stat_archiver',
$${
{current_wal, text},
{archived_count, bigint}, {last_archived_wal, text},
{last_archived_time, timestamp with time zone},
{failed_count, bigint},
{last_failed_wal, text}, {last_failed_time, timestamp with time zone}
}$$,
$${
current_wal, last_archived_wal, last_archived_time, last_failed_wal,
last_failed_time
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_stat_bgwriter',
$${
{buffers_clean, bigint}, {maxwritten_clean, bigint},
{buffers_backend, bigint}, {buffers_backend_fsync, bigint},
{buffers_alloc, bigint}
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_stat_checkpointer',
$${
{num_timed, bigint}, {num_requested, bigint},
{write_time, double precision}, {sync_time, double precision},
{buffers_written, bigint}
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_stat_database',
$${
{numbackends, integer},
{xact_commit, bigint}, {xact_rollback, bigint},
{blks_read, bigint}, {blks_hit, bigint},
{tup_returned, bigint}, {tup_fetched, bigint}, {tup_inserted, bigint},
{tup_updated, bigint}, {tup_deleted, bigint},
{conflicts, bigint}, {temp_files, bigint}, {temp_bytes, bigint},
{deadlocks, bigint},
{checksum_failures, bigint}, {checksum_last_failure, timestamp with time zone},
{blk_read_time, double precision}, {blk_write_time, double precision},
{session_time, double precision}, {active_time, double precision},
{idle_in_transaction_time, double precision}, {sessions, bigint},
{sessions_abandoned, bigint}, {sessions_fatal, bigint},
{sessions_killed, bigint}, {stats_reset, timestamp with time zone}
}$$,
$${
checksum_failures, checksum_last_failure, session_time, active_time,
idle_in_transaction_time, sessions, sessions_abandoned, sessions_fatal,
sessions_killed, stats_reset
}$$,
_key_cols => $${
{datid, oid}
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_stat_database_conflicts',
$${
{confl_tablespace, bigint}, {confl_lock, bigint}, {confl_snapshot, bigint},
{confl_bufferpin, bigint}, {confl_deadlock, bigint},
{confl_active_logicalslot, bigint}
}$$,
_key_cols => $${
{datid, oid}
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_stat_io',
$${
{reads, bigint}, {read_time, double precision},
{writes, bigint}, {write_time, double precision},
{writebacks, bigint}, {writeback_time, double precision},
{extends, bigint}, {extend_time, double precision},
{op_bytes, bigint}, {hits, bigint}, {evictions, bigint}, {reuses, bigint},
{fsyncs, bigint}, {fsync_time, double precision},
{stats_reset, timestamp with time zone}
}$$,
$${
reads, read_time, writebacks, writeback_time, extends, extend_time, hits,
evictions, reuses, fsyncs, fsync_time
}$$,
_key_cols => $${
{backend_type, text}, {object, text}, {context, text}
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_stat_replication',
$${
{current_lsn, pg_lsn},
{pid, integer}, {usename, text}, {application_name, text}, {client_addr, inet},
{backend_start, timestamp with time zone}, {backend_xmin, xid}, {state, text},
{sent_lsn, pg_lsn},
{write_lsn, pg_lsn}, {flush_lsn, pg_lsn}, {replay_lsn, pg_lsn},
{write_lag, interval}, {flush_lag, interval}, {replay_lag, interval},
{sync_priority, integer}, {sync_state, text},
{reply_time, timestamp with time zone}
}$$,
$${
pid, usename, application_name, client_addr, backend_start, backend_xmin,
state, sent_lsn, write_lsn, flush_lsn, replay_lsn, write_lag, flush_lag,
replay_lag, sync_priority, sync_state, reply_time
}$$,
_min_version => 130000
);

SELECT @extschema@.powa_generic_module_setup('pg_stat_slru',
$${
{blks_zeroed, bigint}, {blks_hit, bigint}, {blks_read, bigint},
{blks_written, bigint}, {blks_exists, bigint},
{flushes, bigint}, {truncates, bigint},
{stats_reset, timestamp with time zone}
}$$,
_key_cols => $${
{name, text}
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_stat_subscription',
$${
{worker_type, text}, {pid, integer}, {leader_pid, integer}, {relid, oid},
{received_lsn, pg_lsn}, {last_msg_send_time, timestamp with time zone},
{last_msg_receipt_time, timestamp with time zone},
{latest_end_lsn, pg_lsn}, {latest_end_time, timestamp with time zone}
}$$,
$${
worker_type, pid, leader_pid, relid, received_lsn, last_msg_send_time,
last_msg_receipt_time, latest_end_lsn, latest_end_time
}$$,
_key_cols => $${
{subid, oid}, {subname, name}
}$$,
_min_version => 130000
);

-- we don't save subname, it can be found in pg_stat_subscription
SELECT @extschema@.powa_generic_module_setup('pg_stat_subscription_stats',
$${
{apply_error_count, bigint}, {sync_error_count, bigint},
{stats_reset, timestamp with time zone}
}$$,
$${
stats_reset
}$$,
_key_cols => $${
{subid, oid}
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_stat_wal',
$${
{wal_records, bigint}, {wal_fpi, bigint}, {wal_bytes, numeric},
{wal_buffers_full, bigint}, {wal_write, bigint}, {wal_sync, bigint},
{wal_write_time, double precision}, {wal_sync_time, double precision},
{stats_reset, timestamp with time zone}
}$$);

SELECT @extschema@.powa_generic_module_setup('pg_stat_wal_receiver',
$${
{pid, integer}, {status, text},
{receive_start_lsn, pg_lsn}, {receive_start_tli, integer},
{last_received_lsn, pg_lsn},
{written_lsn, pg_lsn}, {flushed_lsn, pg_lsn},
{received_tli, integer},
{last_msg_send_time, timestamp with time zone},
{last_msg_receipt_time, timestamp with time zone},
{latest_end_lsn, pg_lsn}, {latest_end_time, timestamp with time zone},
{conninfo, text}
}$$,
_key_cols => $${
{slot_name, text}, {sender_host, text}, {sender_port, integer}
}$$,
_key_nullable => true,
_min_version => 130000
);

SELECT @extschema@.powa_generic_datatype_setup('powa_kcache',
$${
{plan_reads, bigint}, {plan_writes, bigint},
{plan_user_time, double precision}, {plan_system_time, double precision},
{plan_minflts, bigint}, {plan_majflts, bigint},
{plan_nswaps, bigint},
{plan_msgsnds, bigint}, {plan_msgrcvs, bigint},
{plan_nsignals, bigint},
{plan_nvcsws, bigint}, {plan_nivcsws, bigint},
{exec_reads, bigint}, {exec_writes, bigint},
{exec_user_time, double precision}, {exec_system_time, double precision},
{exec_minflts, bigint}, {exec_majflts, bigint},
{exec_nswaps, bigint},
{exec_msgsnds, bigint}, {exec_msgrcvs, bigint},
{exec_nsignals, bigint},
{exec_nvcsws, bigint}, {exec_nivcsws, bigint}
}$$);

SELECT @extschema@.powa_generic_datatype_setup('powa_qualstats',
$${
{occurences, bigint}, {execution_count, bigint}, {nbfiltered, bigint},
{mean_err_estimate_ratio, double precision},
{mean_err_estimate_num, double precision}
}$$,
'{"rate": {"suffix": {"mean_err_estimate_ratio": "",
                      "mean_err_estimate_num": ""}}}');

SELECT @extschema@.powa_generic_datatype_setup('powa_wait_sampling',
$${
{count, bigint}
}$$);

DROP FUNCTION @extschema@.powa_generic_datatype_setup(text, text[], jsonb, boolean);
DROP FUNCTION @extschema@.powa_generic_module_setup(text, text[], text[], boolean, text[], boolean, integer);

/* pg_catalog import support */
CREATE UNLOGGED TABLE @extschema@.powa_catalog_class_src_tmp (
    LIKE @extschema@.powa_catalog_class
);

CREATE UNLOGGED TABLE @extschema@.powa_catalog_attribute_src_tmp (
    LIKE @extschema@.powa_catalog_attribute
);

CREATE UNLOGGED TABLE @extschema@.powa_catalog_namespace_src_tmp (
    LIKE @extschema@.powa_catalog_namespace
);

CREATE UNLOGGED TABLE @extschema@.powa_catalog_type_src_tmp (
    LIKE @extschema@.powa_catalog_type
);

CREATE UNLOGGED TABLE @extschema@.powa_catalog_collation_src_tmp (
    LIKE @extschema@.powa_catalog_collation
);

CREATE UNLOGGED TABLE @extschema@.powa_catalog_proc_src_tmp (
    LIKE @extschema@.powa_catalog_proc
);

CREATE UNLOGGED TABLE @extschema@.powa_catalog_language_src_tmp (
    LIKE @extschema@.powa_catalog_language
);
/* end of pg_catalog import support */


CREATE UNLOGGED TABLE @extschema@.powa_databases_src_tmp (
    srvid integer NOT NULL,
    oid oid NOT NULL,
    datname name NOT NULL
);

CREATE UNLOGGED TABLE @extschema@.powa_statements_src_tmp (
    srvid integer NOT NULL,
    ts  timestamp with time zone NOT NULL,
    userid oid NOT NULL,
    dbid oid NOT NULL,
    toplevel boolean NOT NULL,
    queryid bigint NOT NULL,
    query text NOT NULL,
    calls bigint NOT NULL,
    total_exec_time double precision NOT NULL,
    rows bigint NOT NULL,
    shared_blks_hit bigint NOT NULL,
    shared_blks_read bigint NOT NULL,
    shared_blks_dirtied bigint NOT NULL,
    shared_blks_written bigint NOT NULL,
    local_blks_hit bigint NOT NULL,
    local_blks_read bigint NOT NULL,
    local_blks_dirtied bigint NOT NULL,
    local_blks_written bigint NOT NULL,
    temp_blks_read bigint NOT NULL,
    temp_blks_written bigint NOT NULL,
    shared_blk_read_time double precision NOT NULL,
    shared_blk_write_time double precision NOT NULL,
    local_blk_read_time double precision NOT NULL,
    local_blk_write_time double precision NOT NULL,
    temp_blk_read_time double precision NOT NULL,
    temp_blk_write_time double precision NOT NULL,
    plans bigint NOT NULL,
    total_plan_time double precision NOT NULL,
    wal_records bigint NOT NULL,
    wal_fpi bigint NOT NULL,
    wal_bytes numeric NOT NULL,
    jit_functions bigint NOT NULL,
    jit_generation_time double precision NOT NULL,
    jit_inlining_count bigint NOT NULL,
    jit_inlining_time double precision NOT NULL,
    jit_optimization_count bigint NOT NULL,
    jit_optimization_time double precision NOT NULL,
    jit_emission_count bigint NOT NULL,
    jit_emission_time double precision NOT NULL,
    jit_deform_count bigint NOT NULL,
    jit_deform_time double precision NOT NULL
);

CREATE UNLOGGED TABLE @extschema@.powa_user_functions_src_tmp (
    srvid integer NOT NULL,
    ts  timestamp with time zone NOT NULL,
    dbid oid NOT NULL,
    funcid oid NOT NULL,
    calls bigint NOT NULL,
    total_time double precision NOT NULL,
    self_time double precision NOT NULL
);

CREATE UNLOGGED TABLE @extschema@.powa_all_indexes_src_tmp (
    srvid integer NOT NULL,
    ts  timestamp with time zone NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    indexrelid oid NOT NULL,
    idx_size bigint NOT NULL,
    idx_scan bigint,
    last_idx_scan timestamp with time zone,
    idx_tup_read bigint,
    idx_tup_fetch bigint,
    idx_blks_read bigint,
    idx_blks_hit bigint
);

CREATE UNLOGGED TABLE @extschema@.powa_all_tables_src_tmp (
    srvid integer NOT NULL,
    ts  timestamp with time zone NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    tbl_size bigint NOT NULL,
    seq_scan bigint,
    last_seq_scan timestamp with time zone,
    seq_tup_read bigint,
    idx_scan bigint,
    last_idx_scan timestamp with time zone,
    idx_tup_fetch bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    n_tup_newpage_upd bigint,
    n_liv_tup bigint,
    n_dead_tup bigint,
    n_mod_since_analyze bigint,
    n_ins_since_vacuum bigint,
    last_vacuum timestamp with time zone,
    last_autovacuum timestamp with time zone,
    last_analyze timestamp with time zone,
    last_autoanalyze timestamp with time zone,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint,
    heap_blks_read bigint,
    heap_blks_hit bigint,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    toast_blks_read bigint,
    toast_blks_hit bigint,
    tidx_blks_read bigint,
    tidx_blks_hit bigint
);

CREATE UNLOGGED TABLE @extschema@.powa_catalog_database_src_tmp (
    srvid integer NOT NULL,
    oid oid NOT NULL,
    datname text NOT NULL
);

CREATE UNLOGGED TABLE @extschema@.powa_catalog_role_src_tmp (
    srvid integer NOT NULL,
    oid oid NOT NULL,
    rolname text NOT NULL,
    rolsuper boolean NOT NULL,
    rolinherit boolean NOT NULL,
    rolcreaterole boolean NOT NULL,
    rolcreatedb boolean NOT NULL,
    rolcanlogin boolean NOT NULL,
    rolreplication boolean NOT NULL,
    rolbypassrls boolean NOT NULL
);

CREATE TABLE @extschema@.powa_statements_history (
    srvid integer NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    toplevel boolean NOT NULL,
    userid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_statements_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_statements_history_record NOT NULL,
    maxs_in_range @extschema@.powa_statements_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_statements_history ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_statements_history ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_statements_history_query_ts ON @extschema@.powa_statements_history USING gist (srvid, queryid, coalesce_range);

CREATE TABLE @extschema@.powa_statements_history_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_statements_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_statements_history_record NOT NULL,
    maxs_in_range @extschema@.powa_statements_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_statements_history_db ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_statements_history_db ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_statements_history_db_ts ON @extschema@.powa_statements_history_db USING gist (srvid, dbid, coalesce_range);

CREATE TABLE @extschema@.powa_statements_history_current (
    srvid integer NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    toplevel boolean NOT NULL,
    userid oid NOT NULL,
    record @extschema@.powa_statements_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_statements_history_current(srvid);

CREATE TABLE @extschema@.powa_statements_history_current_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    record @extschema@.powa_statements_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_statements_history_current_db(srvid);

CREATE TABLE @extschema@.powa_user_functions_history (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    funcid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_user_functions_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_user_functions_history_record NOT NULL,
    maxs_in_range @extschema@.powa_user_functions_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_user_functions_history ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_user_functions_history ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_user_functions_history_funcid_ts ON @extschema@.powa_user_functions_history USING gist (srvid, funcid, coalesce_range);

CREATE TABLE @extschema@.powa_user_functions_history_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_user_functions_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_user_functions_history_record NOT NULL,
    maxs_in_range @extschema@.powa_user_functions_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_user_functions_history_db ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_user_functions_history_db ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_user_functions_history_db_dbid_ts ON @extschema@.powa_user_functions_history USING gist (srvid, dbid, coalesce_range);

CREATE TABLE @extschema@.powa_user_functions_history_current (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    funcid oid NOT NULL,
    record @extschema@.powa_user_functions_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_user_functions_history_current(srvid);

CREATE TABLE @extschema@.powa_user_functions_history_current_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    record @extschema@.powa_user_functions_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_user_functions_history_current_db(srvid);

CREATE TABLE @extschema@.powa_all_indexes_history (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    indexrelid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_all_indexes_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_all_indexes_history_record NOT NULL,
    maxs_in_range @extschema@.powa_all_indexes_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_all_indexes_history ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_all_indexes_history ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_all_indexes_history_relid_ts ON @extschema@.powa_all_indexes_history USING gist (srvid, relid, coalesce_range);

CREATE TABLE @extschema@.powa_all_indexes_history_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_all_indexes_history_db_record[] NOT NULL,
    mins_in_range @extschema@.powa_all_indexes_history_db_record NOT NULL,
    maxs_in_range @extschema@.powa_all_indexes_history_db_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_all_indexes_history_db ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_all_indexes_history_db ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_all_indexes_history_db_dbid_ts ON @extschema@.powa_all_indexes_history_db USING gist (srvid, dbid, coalesce_range);

CREATE TABLE @extschema@.powa_all_indexes_history_current (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    indexrelid oid NOT NULL,
    record @extschema@.powa_all_indexes_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_all_indexes_history_current(srvid);

CREATE TABLE @extschema@.powa_all_indexes_history_current_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    record @extschema@.powa_all_indexes_history_db_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_all_indexes_history_current_db(srvid);

CREATE TABLE @extschema@.powa_all_tables_history (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_all_tables_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_all_tables_history_record NOT NULL,
    maxs_in_range @extschema@.powa_all_tables_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_all_tables_history ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_all_tables_history ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_all_tables_history_relid_ts ON @extschema@.powa_all_tables_history USING gist (srvid, relid, coalesce_range);

CREATE TABLE @extschema@.powa_all_tables_history_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_all_tables_history_db_record[] NOT NULL,
    mins_in_range @extschema@.powa_all_tables_history_db_record NOT NULL,
    maxs_in_range @extschema@.powa_all_tables_history_db_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_all_tables_history_db ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_all_tables_history_db ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_all_tables_history_db_dbid_ts ON @extschema@.powa_all_tables_history_db USING gist (srvid, dbid, coalesce_range);

CREATE TABLE @extschema@.powa_all_tables_history_current (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    record @extschema@.powa_all_tables_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_all_tables_history_current(srvid);

CREATE TABLE @extschema@.powa_all_tables_history_current_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    record @extschema@.powa_all_tables_history_db_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_all_tables_history_current_db(srvid);

-- Register the given module if needed.
CREATE FUNCTION @extschema@.powa_activate_module(_srvid int, _module text) RETURNS boolean
AS $_$
DECLARE
    v_res bool;
BEGIN
    IF (_srvid IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_module: no server id provided';
    END IF;

    IF (_module IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_module: no module provided';
    END IF;

    -- Check that the module is known.
    SELECT COUNT(*) = 1 INTO v_res
    FROM @extschema@.powa_modules
    WHERE module = _module;

    IF (NOT v_res) THEN
        RAISE EXCEPTION 'Module "%" is not known', _module;
    END IF;

    -- The record may already be present, but the enabled flag could be off.
    -- If so simply enable it.  Otherwise, add the needed record.
    SELECT COUNT(*) > 0 INTO v_res
    FROM @extschema@.powa_module_config
    WHERE module = _module
    AND srvid = _srvid;

    IF (v_res) THEN
        UPDATE @extschema@.powa_module_config
        SET enabled = true
        WHERE enabled = false
        AND srvid = _srvid
        AND module = _module;
    ELSE
        INSERT INTO @extschema@.powa_module_config
            (srvid, module, added_manually)
        VALUES
            (_srvid, _module, (_srvid != 0));
    END IF;

    RETURN true;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_activate_module */

-- Deactivate a module: leave the record in powa_module_config but
-- remove the enabled flag.
CREATE FUNCTION @extschema@.powa_deactivate_module(_srvid int, _module text) RETURNS boolean
AS $_$
DECLARE
    v_res bool;
BEGIN
    IF (_srvid IS NULL) THEN
        RAISE EXCEPTION 'powa_deactivate_module: no server id provided';
    END IF;

    IF (_module IS NULL) THEN
        RAISE EXCEPTION 'powa_deactivate_module: no module provided';
    END IF;

    -- Check that the module is known.
    SELECT COUNT(*) = 1 INTO v_res
    FROM @extschema@.powa_modules
    WHERE module = _module;

    IF (NOT v_res) THEN
        RAISE EXCEPTION 'Module "%" is not known', _module;
    END IF;

    UPDATE @extschema@.powa_module_config
    SET enabled = false
    WHERE enabled = true
    AND srvid = _srvid
    AND module = _module;

    RETURN true;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_deactivate_module */

-- Register the given db module if needed.
CREATE FUNCTION @extschema@.powa_activate_db_module(_srvid int, _db_module text,
                                                    databases text[] DEFAULT NULL)
RETURNS boolean
AS $_$
DECLARE
    v_res bool;
    v_enabled bool;
    v_dbnames text[];
BEGIN
    IF (_srvid IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_db_module: no server id provided';
    END IF;

    IF (_db_module IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_db_module: no module provided';
    END IF;

    -- Check that the module is known.
    SELECT COUNT(*) = 1 INTO v_res
    FROM @extschema@.powa_db_modules
    WHERE db_module = _db_module;

    IF (NOT v_res) THEN
        RAISE EXCEPTION 'Database module "%" is not known', _db_module;
    END IF;

    -- We need to check if there's an existing row or not, and if yes what is
    -- the current dbnames value.  As dbnames is NULLable, also retrieve the
    -- enabled field: we can use it to know if the row already exist and also
    -- to avoid a useless UPDATE.
    SELECT enabled, dbnames INTO v_enabled, v_dbnames
    FROM @extschema@.powa_db_module_config
    WHERE db_module = _db_module
    AND srvid = _srvid;

    IF (v_enabled IS NOT NULL) THEN
        -- There's an existing row.  Update the dbnames and enable the db
        -- module if needed.  Note that we don't try hard to detect whether the
        -- dbnames are semantically equivalent.
        IF (v_enabled AND v_dbnames IS NOT DISTINCT FROM databases) THEN
            -- existing info already matches, bail out
            RETURN true;
        END IF;
        UPDATE @extschema@.powa_db_module_config
        SET enabled = true, dbnames = databases
        WHERE srvid = _srvid
        AND db_module = _db_module;
    ELSE
        -- Just insert the wanted informations
        INSERT INTO @extschema@.powa_db_module_config
            (srvid, db_module, dbnames)
        VALUES
            (_srvid, _db_module, databases);
    END IF;

    RETURN true;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_activate_db_module */

-- Deactivate a db module: leave the record in powa_db_module_config but
-- remove the enabled flag.
CREATE FUNCTION @extschema@.powa_deactivate_db_module(_srvid int,
                                                      _db_module text,
                                                      databases text[] DEFAULT NULL)
RETURNS boolean
AS $_$
DECLARE
    v_res bool;
    v_enabled bool;
    v_dbnames text[];
    v_new_dbnames text[];
BEGIN
    IF (_srvid IS NULL) THEN
        RAISE EXCEPTION 'powa_deactivate_db_module: no server id provided';
    END IF;

    IF (_db_module IS NULL) THEN
        RAISE EXCEPTION 'powa_deactivate_db_module: no module provided';
    END IF;

    -- Check that the module is known.
    SELECT COUNT(*) = 1 INTO v_res
    FROM @extschema@.powa_db_modules
    WHERE db_module = _db_module;

    IF (NOT v_res) THEN
        RAISE EXCEPTION 'db module "%" is not known', _db_module;
    END IF;

    -- We need to check if there's an existing row or not, and if yes what is
    -- the current dbnames value.  As dbnames is NULLable, also retrieve the
    -- enabled field: we can use it to know if the row already exist and also
    -- to avoid a useless UPDATE.
    SELECT enabled, dbnames INTO v_enabled, v_dbnames
    FROM @extschema@.powa_db_module_config
    WHERE db_module = _db_module
    AND srvid = _srvid;

    -- Deactivating a non configured db module is a not allowed
    IF (v_enabled IS NULL) THEN
        RAISE EXCEPTION 'db module "%" is not configured', _db_module;
    END IF;

    -- Deactivating a deactivated db module is a noop, as in that case there
    -- shouldn't dbnames
    IF (v_enabled IS FALSE) THEN
        ASSERT v_dbnames IS NULL, 'A deactivated db module shouldn''t contain db names';
        RETURN true;
    END IF;

    -- We don't support deactivating only some databases if this is an "all-db"
    -- record
    IF (databases IS NOT NULL AND v_dbnames IS NULL) THEN
        RAISE EXCEPTION 'cannot deactivate a db module for a specific database if no specific database is configured';
    END IF;

    -- If users asked to deactivate only for some databases, error out if any
    -- of the wanted datbase isn't already activated
    IF (databases IS NOT NULL AND NOT databases <@ v_dbnames) THEN
        RAISE EXCEPTION 'cannot deactivate a db module for a specific database if not already activated on that database';
    END IF;

    -- If there's no record dbname, simply deactivate the module
    IF (databases IS NULL) THEN
        -- existing info already matches, bail out
        IF (NOT v_enabled AND v_dbnames IS NULL) THEN
            RETURN true;
        END IF;
        UPDATE @extschema@.powa_db_module_config
        SET enabled = false, dbnames = NULL
        WHERE srvid = _srvid
        AND db_module = _db_module;
    ELSIF (v_dbnames IS NULL) THEN
        -- stored dbnames isn't NULL but user didn't provide any.  Reset all
        UPDATE @extschema@.powa_db_module_config
        SET enabled = false, dbnames = NULL
        WHERE srvid = _srvid
        AND db_module = _db_module;
    ELSE
        -- There are stored dbnames and users provided some.  Keep the module
        -- activated and simply remove the specified dbnames
        ASSERT v_enabled IS TRUE, 'Module should be enabled';
        SELECT array_agg(dbname ORDER BY dbname)
        FROM (
            SELECT unnest(v_dbnames)
            EXCEPT
            SELECT unnest(databases)
        ) s(dbname) INTO v_new_dbnames;

        IF (coalesce(cardinality(v_new_dbnames), 0) = 0) THEN
            -- If everything was removed, clear all dbnames and disable the db
            -- module
            UPDATE @extschema@.powa_db_module_config
            SET enabled = false, dbnames = NULL
            WHERE srvid = _srvid
            AND db_module = _db_module;
        ELSE
            ASSERT v_enabled IS TRUE, 'Module should be enabled';
            -- otherwise just save the new data
            UPDATE @extschema@.powa_db_module_config
            SET dbnames = v_new_dbnames
            WHERE srvid = _srvid
            AND db_module = _db_module;
        END IF;
    END IF;

    RETURN true;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_deactivate_db_module */

-- Register the given extension if needed, and set the enabled flag to on.
CREATE FUNCTION @extschema@.powa_activate_extension(_srvid integer, _extname text) RETURNS boolean
AS $_$
DECLARE
    v_res boolean;
BEGIN
    IF (_srvid IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_extension: no server id provided';
    END IF;

    IF (_extname IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_extension: no extension provided';
    END IF;

    -- Check that the extension is known.
    SELECT COUNT(*) = 1 INTO v_res
    FROM @extschema@.powa_extensions
    WHERE extname = _extname;

    IF (NOT v_res) THEN
        RAISE WARNING 'powa_activate_extension "%" is not known', _extname;
        RETURN false;
    END IF;

    -- For local server, check that the extension has been created before
    -- blindly activating it.  It allows us to simply try to activate all known
    -- extensions for the local server by default.
    IF (_srvid = 0) THEN
        SELECT COUNT(*) = 1 INTO v_res
        FROM pg_catalog.pg_extension
        WHERE extname = _extname;

        IF (NOT v_res) THEN
            RAISE WARNING 'Extension % is not installed locally, ignoring',
                          _extname;
            RETURN false;
        END IF;
    END IF;

    -- Activating the "powa" extension is an alias for activating all the
    -- underlying (default) modules.  We don't activate db modules for the
    -- local server though, as those are only process by powa-collector, which
    -- ignores the local server.
    IF (_extname = 'powa') THEN
        SELECT bool_and(v)
        FROM (
            SELECT @extschema@.powa_activate_module(_srvid, module)
            FROM @extschema@.powa_modules
            UNION ALL
            SELECT @extschema@.powa_activate_db_module(_srvid, db_module)
            FROM @extschema@.powa_db_modules
            WHERE _srvid != 0
            AND NOT added_manually
        ) s(v) INTO v_res;

        RETURN v_res;
    END IF;

    -- The record may already be present, but the enabled flag could be off.
    -- If so simply enable it.  Otherwise, add the needed record.
    SELECT COUNT(*) > 0 INTO v_res
    FROM @extschema@.powa_extension_config
    WHERE extname = _extname
    AND srvid = _srvid;

    IF (v_res) THEN
        UPDATE @extschema@.powa_extension_config
        SET enabled = true
        WHERE enabled = false
        AND srvid = _srvid
        AND extname = _extname;
    ELSE
        INSERT INTO @extschema@.powa_extension_config
            (srvid, extname, added_manually)
        VALUES
            (_srvid, _extname, (_srvid != 0));
    END IF;

    RETURN true;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_activate_extension */

-- Deactivate an extension: leave the record in powa_extension_config but
-- remove the enabled flag.
CREATE FUNCTION @extschema@.powa_deactivate_extension(_srvid integer, _extname text) RETURNS boolean
AS $_$
DECLARE
    v_res bool;
BEGIN
    IF (_srvid IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_extension: no server id provided';
    END IF;

    IF (_extname IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_extension: no extension provided';
    END IF;

    -- Deactivating extension "powa" is an alias for deactivating all the
    -- underlying modules.
    IF (_extname = 'powa') THEN
        SELECT bool_and(@extschema@.powa_deactivate_module(_srvid, module))
            INTO v_res
        FROM @extschema@.powa_modules;

        RETURN v_res;
    ELSE
        UPDATE @extschema@.powa_extension_config
        SET enabled = false
        WHERE extname = _extname
        AND srvid = _srvid;
    END IF;

    return true;
END;
$_$ LANGUAGE plpgsql; /* end of powa_deactivate_extension */

CREATE FUNCTION @extschema@.powa_register_server(hostname text,
    port integer DEFAULT 5432,
    alias text DEFAULT NULL,
    username text DEFAULT 'powa',
    password text DEFAULT NULL,
    dbname text DEFAULT 'powa',
    frequency integer DEFAULT 300,
    powa_coalesce integer default 100,
    retention interval DEFAULT '1 day'::interval,
    allow_ui_connection boolean DEFAULT true,
    extensions text[] DEFAULT NULL)
RETURNS boolean AS $_$
DECLARE
    v_ok boolean;
    v_srvid integer;
    v_extname text;
BEGIN
    -- sanity checks
    SELECT coalesce(port, 5432), coalesce(username, 'powa'),
        coalesce(dbname, 'powa'), coalesce(frequency, 300),
        coalesce(powa_coalesce, 100), coalesce(retention, '1 day')::interval,
        coalesce(allow_ui_connection, true)
    INTO port, username, dbname, frequency, powa_coalesce, retention, allow_ui_connection;

    INSERT INTO @extschema@.powa_servers
        (alias, hostname, port, username, password, dbname, frequency, powa_coalesce, retention, allow_ui_connection)
    VALUES
        (alias, hostname, port, username, password, dbname, frequency, powa_coalesce, retention, allow_ui_connection)
    RETURNING id INTO v_srvid;

    INSERT INTO @extschema@.powa_snapshot_metas(srvid) VALUES (v_srvid);

    -- always register pgss, as it's mandatory
    SELECT @extschema@.powa_activate_extension(v_srvid, 'pg_stat_statements') INTO v_ok;
    IF (NOT v_ok) THEN
        RAISE EXCEPTION 'Could not activate pg_stat_statements';
    END IF;
    -- and also the powa, which is an alias for activating all its modules
    SELECT @extschema@.powa_activate_extension(v_srvid, 'powa') INTO v_ok;
    IF (NOT v_ok) THEN
        RAISE EXCEPTION 'Could not activate powa modules';
    END IF;

    -- If no extra extensions were asked, we're done
    IF extensions IS NULL THEN
        RETURN true;
    END IF;

    FOREACH v_extname IN ARRAY extensions
    LOOP
        -- don't process pg_stat_statements or powa extensions as those are
        -- already forced
        CONTINUE WHEN v_extname IN ('pg_stat_statements', 'powa');

        SELECT @extschema@.powa_activate_extension(v_srvid, v_extname) INTO v_ok;

        IF (v_ok IS DISTINCT FROM true) THEN
            RAISE WARNING 'Could not activate extension % on server %:%',
                v_extname, hostname, port;
        END IF;
    END LOOP;
    RETURN true;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_register_server */

CREATE FUNCTION @extschema@.powa_configure_server(_srvid integer, data json) RETURNS boolean
AS $_$
DECLARE
    v_rowcount bigint;
    k text;
    v text;
    v_query text = '';
BEGIN
    IF (_srvid = 0) THEN
        RAISE EXCEPTION 'Local server cannot be configured';
    END IF;

    IF (data IS NULL) THEN
        RAISE EXCEPTION 'No data provided';
    END IF;

    FOR k, v IN SELECT * FROM json_each_text(data) LOOP
        IF (k = 'id') THEN
            RAISE EXCEPTION 'Updating server id is not allowed';
        END IF;

        IF (k NOT IN ('hostname', 'alias', 'port', 'username', 'password',
            'dbname', 'frequency', 'retention', 'allow_ui_connection')
        ) THEN
            RAISE EXCEPTION 'Unknown field: %', k;
        END IF;

        IF (v_query != '') THEN
            v_query := v_query || ', ';
        END IF;
        v_query := v_query || format('%I = %L', k, v);
    END LOOP;

    v_query := 'UPDATE @extschema@.powa_servers SET '
        || v_query
        || format(' WHERE id = %s', _srvid);

    EXECUTE v_query;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    RETURN v_rowcount = 1;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* powa_config_server */

CREATE FUNCTION @extschema@.powa_deactivate_server(_srvid integer) RETURNS boolean
AS $_$
DECLARE
    v_rowcount bigint;
BEGIN
    IF (_srvid = 0) THEN
        RAISE EXCEPTION 'Local server cannot be updated';
    END IF;

    UPDATE @extschema@.powa_servers SET frequency = -1 WHERE id = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    RETURN v_rowcount = 1;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* powa_deactivate_server */

CREATE FUNCTION @extschema@.powa_delete_and_purge_server(_srvid integer) RETURNS boolean
AS $_$
DECLARE
    v_rowcount bigint;
BEGIN
    IF (_srvid = 0) THEN
        RAISE EXCEPTION 'Local server cannot be deleted';
    END IF;

    DELETE FROM @extschema@.powa_servers WHERE id = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    -- pg_track_settings is an autonomous extension, so it doesn't have a FK to
    -- powa_servers.  It therefore needs to be processed manually
    SELECT COUNT(*)
        FROM pg_extension
        WHERE extname = 'pg_track_settings'
        INTO v_rowcount;
    IF (v_rowcount = 1) THEN
        DELETE FROM pg_track_settings_list WHERE srvid = _srvid;
        DELETE FROM pg_track_settings_history WHERE srvid = _srvid;
        DELETE FROM pg_track_db_role_settings_list WHERE srvid = _srvid;
        DELETE FROM pg_track_db_role_settings_history WHERE srvid = _srvid;
        DELETE FROM pg_reboot WHERE srvid = _srvid;
    END IF;

    RETURN v_rowcount = 1;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* powa_deactivate_server */

DO $anon$
BEGIN
    IF current_setting('server_version_num')::int < 90600 THEN
        CREATE FUNCTION @extschema@.powa_get_guc (guc text, def text DEFAULT NULL) RETURNS text
        LANGUAGE plpgsql
        AS $_$
        DECLARE
            v_val text;
        BEGIN
            BEGIN
                SELECT current_setting(guc) INTO v_val;
            EXCEPTION WHEN OTHERS THEN
                v_val = def;
            END;

            RETURN v_val;
        END;
        $_$
        SET search_path = pg_catalog;
    ELSE
        CREATE FUNCTION @extschema@.powa_get_guc (guc text, def text DEFAULT NULL) RETURNS text
        LANGUAGE plpgsql
        AS $_$
        BEGIN
            RETURN COALESCE(current_setting(guc, true), def);
        END;
        $_$
        SET search_path = pg_catalog;
    END IF;
END;
$anon$;

CREATE FUNCTION @extschema@.powa_log (msg text) RETURNS void
LANGUAGE plpgsql
AS $_$
BEGIN
    IF @extschema@.powa_get_guc('powa.debug', 'false')::bool THEN
        RAISE WARNING '%', msg;
    ELSE
        RAISE DEBUG '%', msg;
    END IF;
END;
$_$
SET search_path = pg_catalog;

CREATE FUNCTION @extschema@.powa_get_server_retention(_srvid integer)
RETURNS interval AS $_$
DECLARE
    v_ret interval = NULL;
BEGIN
    IF (_srvid = 0) THEN
        v_ret := current_setting('powa.retention')::interval;
    ELSE
        SELECT retention INTO v_ret
        FROM @extschema@.powa_servers
        WHERE id = _srvid;
    END IF;

    IF (v_ret IS NULL) THEN
        RAISE EXCEPTION 'Not retention found for server %', _srvid;
    END IF;

    RETURN v_ret;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_get_server_retention */

/* pg_stat_kcache integration - part 1 */

CREATE UNLOGGED TABLE @extschema@.powa_kcache_src_tmp (
    srvid            integer NOT NULL,
    ts               timestamp with time zone NOT NULL,
    queryid          bigint NOT NULL,
    top              bool NOT NULL,
    userid           oid NOT NULL,
    dbid             oid NOT NULL,
    plan_reads       bigint,
    plan_writes      bigint,
    plan_user_time   double precision,
    plan_system_time double precision,
    plan_minflts     bigint,
    plan_majflts     bigint,
    plan_nswaps      bigint,
    plan_msgsnds     bigint,
    plan_msgrcvs     bigint,
    plan_nsignals    bigint,
    plan_nvcsws      bigint,
    plan_nivcsws     bigint,
    exec_reads       bigint,
    exec_writes      bigint,
    exec_user_time   double precision NOT NULL,
    exec_system_time double precision NOT NULL,
    exec_minflts     bigint,
    exec_majflts     bigint,
    exec_nswaps      bigint,
    exec_msgsnds     bigint,
    exec_msgrcvs     bigint,
    exec_nsignals    bigint,
    exec_nvcsws      bigint,
    exec_nivcsws     bigint
);

CREATE TABLE @extschema@.powa_kcache_metrics (
    srvid integer NOT NULL,
    coalesce_range tstzrange NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    metrics @extschema@.powa_kcache_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_kcache_history_record NOT NULL,
    maxs_in_range @extschema@.powa_kcache_history_record NOT NULL,
    top boolean NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, queryid, dbid, userid, top),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_kcache_metrics ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_kcache_metrics ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX ON @extschema@.powa_kcache_metrics (srvid, queryid);

CREATE TABLE @extschema@.powa_kcache_metrics_db (
    srvid integer NOT NULL,
    coalesce_range tstzrange NOT NULL,
    dbid oid NOT NULL,
    metrics @extschema@.powa_kcache_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_kcache_history_record NOT NULL,
    maxs_in_range @extschema@.powa_kcache_history_record NOT NULL,
    top boolean NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, dbid, top),
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_kcache_metrics_db ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_kcache_metrics_db ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE TABLE @extschema@.powa_kcache_metrics_current ( srvid integer NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    metrics @extschema@.powa_kcache_history_record NULL NULL,
    top boolean NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_kcache_metrics_current(srvid);

CREATE TABLE @extschema@.powa_kcache_metrics_current_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    metrics @extschema@.powa_kcache_history_record NULL NULL,
    top boolean NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_kcache_metrics_current_db(srvid);

/* end of pg_stat_kcache integration - part 1 */

/* pg_qualstats integration - part 1 */

CREATE TYPE @extschema@.qual_type AS (
    relid oid,
    attnum integer,
    opno oid,
    eval_type "char"
);

CREATE TYPE @extschema@.qual_values AS (
    constants text[],
    occurences bigint,
    execution_count bigint,
    nbfiltered bigint,
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision
);

CREATE UNLOGGED TABLE @extschema@.powa_qualstats_src_tmp (
    srvid integer NOT NULL,
    ts timestamp with time zone NOT NULL,
    uniquequalnodeid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    qualnodeid bigint NOT NULL,
    occurences bigint NOT NULL,
    execution_count bigint NOT NULL,
    nbfiltered bigint NOT NULL,
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision,
    queryid bigint NOT NULL,
    constvalues varchar[] NOT NULL,
    quals @extschema@.qual_type[] NOT NULL
);

CREATE TABLE @extschema@.powa_qualstats_quals (
    srvid integer NOT NULL,
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    quals @extschema@.qual_type[],
    PRIMARY KEY (srvid, qualid, queryid, dbid, userid),
    FOREIGN KEY (srvid, queryid, dbid, userid) REFERENCES @extschema@.powa_statements(srvid, queryid, dbid, userid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_qualstats_quals(srvid, queryid);

CREATE TABLE @extschema@.powa_qualstats_quals_history (
    srvid integer NOT NULL,
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    coalesce_range tstzrange,
    records @extschema@.powa_qualstats_history_record[],
    mins_in_range @extschema@.powa_qualstats_history_record,
    maxs_in_range @extschema@.powa_qualstats_history_record,
    FOREIGN KEY (srvid, qualid, queryid, dbid, userid) REFERENCES @extschema@.powa_qualstats_quals (srvid, qualid, queryid, dbid, userid) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_qualstats_quals_history ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_qualstats_quals_history ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_qualstats_quals_history_query_ts ON @extschema@.powa_qualstats_quals_history USING gist (srvid, queryid, coalesce_range);

CREATE TABLE @extschema@.powa_qualstats_quals_history_current (
    srvid integer NOT NULL,
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    ts timestamptz,
    occurences bigint,
    execution_count   bigint,
    nbfiltered bigint,
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision,
    FOREIGN KEY (srvid, qualid, queryid, dbid, userid) REFERENCES @extschema@.powa_qualstats_quals(srvid, qualid, queryid, dbid, userid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_qualstats_quals_history_current(srvid);

CREATE TABLE @extschema@.powa_qualstats_constvalues_history (
    srvid integer NOT NULL,
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    coalesce_range tstzrange,
    most_used @extschema@.qual_values[],
    most_filtering @extschema@.qual_values[],
    least_filtering @extschema@.qual_values[],
    most_executed @extschema@.qual_values[],
    most_errestim_ratio @extschema@.qual_values[],
    most_errestim_num @extschema@.qual_values[],
    FOREIGN KEY (srvid, qualid, queryid, dbid, userid) REFERENCES @extschema@.powa_qualstats_quals (srvid, qualid, queryid, dbid, userid) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_qualstats_constvalues_history USING gist (srvid, queryid, qualid, coalesce_range);
CREATE INDEX ON @extschema@.powa_qualstats_constvalues_history (srvid, qualid, queryid);

CREATE TABLE @extschema@.powa_qualstats_constvalues_history_current (
    srvid integer NOT NULL,
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    ts timestamptz,
    constvalues text[],
    occurences bigint,
    execution_count bigint,
    nbfiltered bigint,
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision,
    FOREIGN KEY (srvid, qualid, queryid, dbid, userid) REFERENCES @extschema@.powa_qualstats_quals (srvid, qualid, queryid, dbid, userid) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_qualstats_constvalues_history_current(srvid);


/* end of pg_qualstats_integration - part 1 */

/* pg_wait_sampling integration - part 1 */

CREATE UNLOGGED TABLE @extschema@.powa_wait_sampling_src_tmp (
    srvid integer NOT NULL,
    ts timestamp with time zone NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    queryid bigint NOT NULL,
    count numeric NOT NULL
);

CREATE TABLE @extschema@.powa_wait_sampling_history (
    srvid integer NOT NULL REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    coalesce_range tstzrange NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    records @extschema@.powa_wait_sampling_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_wait_sampling_history_record NOT NULL,
    maxs_in_range @extschema@.powa_wait_sampling_history_record NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, queryid, dbid, event_type, event)
);
ALTER TABLE @extschema@.powa_wait_sampling_history ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_wait_sampling_history ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_wait_sampling_history_query_ts ON @extschema@.powa_wait_sampling_history USING gist (srvid, queryid, coalesce_range);

CREATE TABLE @extschema@.powa_wait_sampling_history_db (
    srvid integer NOT NULL REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    coalesce_range tstzrange NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    records @extschema@.powa_wait_sampling_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_wait_sampling_history_record NOT NULL,
    maxs_in_range @extschema@.powa_wait_sampling_history_record NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, dbid, event_type, event)
);
ALTER TABLE @extschema@.powa_wait_sampling_history_db ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.powa_wait_sampling_history_db ALTER COLUMN maxs_in_range SET STORAGE MAIN;

CREATE INDEX powa_wait_sampling_history_db_ts ON @extschema@.powa_wait_sampling_history_db USING gist (srvid, dbid, coalesce_range);

CREATE TABLE @extschema@.powa_wait_sampling_history_current (
    srvid integer NOT NULL REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    record @extschema@.powa_wait_sampling_history_record NOT NULL
);
CREATE INDEX ON @extschema@.powa_wait_sampling_history_current(srvid);

CREATE TABLE @extschema@.powa_wait_sampling_history_current_db (
    srvid integer NOT NULL REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    record @extschema@.powa_wait_sampling_history_record NOT NULL
);
CREATE INDEX ON @extschema@.powa_wait_sampling_history_current_db(srvid);

/* end of pg_wait_sampling integration - part 1 */

-- Mark all of powa's tables as "to be dumped"
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_servers','WHERE id > 0');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_snapshot_metas','WHERE srvid > 0');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_databases','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_statements','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_statements_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_statements_history_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_statements_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_statements_history_current_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_user_functions_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_user_functions_history_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_user_functions_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_user_functions_history_current_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_indexes_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_indexes_history_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_indexes_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_indexes_history_current_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_tables_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_tables_history_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_tables_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_tables_history_current_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_extensions','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_extension_functions','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_extension_config','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_module_functions','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_module_config','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_catalog_databases','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_catalog_roles','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_catalog_class','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_catalog_attribute','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_catalog_namespace','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_catalog_type','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_catalog_collation','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_catalog_proc','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_catalog_language','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_db_modules','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_db_module_functions','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_db_module_config', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_db_module_src_queries','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_kcache_metrics','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_kcache_metrics_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_kcache_metrics_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_kcache_metrics_current_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_qualstats_quals','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_qualstats_quals_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_qualstats_quals_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_qualstats_constvalues_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_qualstats_constvalues_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_wait_sampling_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_wait_sampling_history_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_wait_sampling_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_wait_sampling_history_current_db','');

-- automatically configure powa for local snapshot if supported extension are
-- created locally
CREATE OR REPLACE FUNCTION @extschema@.powa_check_created_extensions()
RETURNS event_trigger
LANGUAGE plpgsql
AS $_$
DECLARE
    v_extname text;
    v_res bool;
BEGIN
    SELECT extname INTO v_extname
    FROM pg_event_trigger_ddl_commands() d
    JOIN pg_extension e ON d.classid = 'pg_extension'::regclass
        AND d.objid = e.oid
    JOIN @extschema@.powa_extensions p USING (extname)
    WHERE d.object_type = 'extension';

    -- Bail out if this isn't a known extension
    IF (v_extname IS NULL) THEN
        RETURN;
    END IF;

    SELECT @extschema@.powa_activate_extension(0, v_extname) INTO v_res;

    IF (NOT v_res) THEN
        RAISE WARNING 'Could not automatically activate extension "%"', v_extname;
    END IF;
END;
$_$
SET search_path = pg_catalog; /* end of powa_check_created_extensions */

CREATE EVENT TRIGGER powa_check_created_extensions
    ON ddl_command_end
    WHEN tag IN ('CREATE EXTENSION')
    EXECUTE PROCEDURE @extschema@.powa_check_created_extensions() ;

-- automatically remove extensions from local snapshot if supported extension
-- is removed locally
CREATE OR REPLACE FUNCTION @extschema@.powa_check_dropped_extensions()
RETURNS event_trigger
LANGUAGE plpgsql
AS $_$
DECLARE
    r         record;
    v_state   text;
    v_msg     text;
    v_detail  text;
    v_hint    text;
    v_context text;
BEGIN
    -- We unregister extensions regardless the "enabled" field
    WITH src AS (
        SELECT object_name
        FROM pg_event_trigger_dropped_objects() d
        WHERE d.object_type = 'extension'
    )
    SELECT CASE external
        WHEN true THEN quote_ident(nsp.nspname)
        ELSE '@extschema@'
        END AS schema, function_name AS funcname INTO r
    FROM @extschema@.powa_extensions AS pe
    JOIN src ON pe.module = src.object_name
    LEFT JOIN pg_extension AS ext ON ext.extname = pe.extname
    LEFT JOIN pg_namespace AS nsp ON nsp.oid = ext.extnamespace
    WHERE operation = 'unregister'
    ORDER BY module;

    IF ( r.funcname IS NOT NULL ) THEN
        BEGIN
            PERFORM @extschema@.powa_log(format('running %s.%I',
                    r.schema, r.funcname));
            EXECUTE format('SELECT %s.%I(0)', r.schema, r.funcname);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;
            RAISE WARNING '@extschema@.powa_check_dropped_extensions(): function %.% failed:
                state  : %
                message: %
                detail : %
                hint   : %
                context: %', r.schema, quote_ident(r.funcname), v_state, v_msg,
                             v_detail, v_hint, v_context;
        END;
    END IF;
END;
$_$
SET search_path = pg_catalog; /* end of powa_check_dropped_extensions */

CREATE EVENT TRIGGER powa_check_dropped_extensions
    ON sql_drop
    WHEN tag IN ('DROP EXTENSION')
    EXECUTE PROCEDURE @extschema@.powa_check_dropped_extensions() ;

CREATE OR REPLACE FUNCTION @extschema@.powa_prevent_concurrent_snapshot(_srvid integer = 0)
RETURNS void
AS $PROC$
DECLARE
    v_state   text;
    v_msg     text;
    v_detail  text;
    v_hint    text;
    v_context text;
BEGIN
    BEGIN
        PERFORM 1
        FROM @extschema@.powa_snapshot_metas
        WHERE srvid = _srvid
        FOR UPDATE NOWAIT;
    EXCEPTION
    WHEN lock_not_available THEN
        RAISE EXCEPTION 'Could not lock the powa_snapshot_metas record, '
        'a concurrent snapshot is probably running';
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT;
        RAISE EXCEPTION 'Failed to lock the powa_snapshot_metas record:
            state  : %
            message: %
            detail : %
            hint   : %
            context: %', v_state, v_msg, v_detail, v_hint, v_context;
    END;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_prevent_concurrent_snapshot() */

CREATE OR REPLACE FUNCTION @extschema@.powa_take_snapshot(_srvid integer = 0) RETURNS integer
AS $PROC$
DECLARE
  purgets timestamp with time zone;
  purge_seq  bigint;
  r          record;
  v_state    text;
  v_msg      text;
  v_detail   text;
  v_hint     text;
  v_context  text;
  v_title    text = 'PoWA - ';
  v_rowcount bigint;
  v_nb_err int = 0;
  v_errs     text[] = '{}';
  v_pattern  text = '@extschema@.powa_take_snapshot(%s): function %s.%I failed:
              state  : %s
              message: %s
              detail : %s
              hint   : %s
              context: %s';
  v_pattern_simple text = '@extschema@.powa_take_snapshot(%s): function %s.%I failed: %s';

  v_pattern_cat  text = '@extschema@.powa_take_snapshot(%s): function @extschema@.powa_catalog_generic_snapshot for catalog %s failed:
              state  : %s
              message: %s
              detail : %s
              hint   : %s
              context: %s';
  v_pattern_cat_simple text = '@extschema@.powa_take_snapshot(%s): function @extschema@.powa_catalog_generic_snapshot for catalog %s failed: %s';
  v_coalesce bigint;
  v_catname text;
BEGIN
    PERFORM set_config('application_name',
        v_title || ' snapshot database list',
        false);
    PERFORM @extschema@.powa_log('start of powa_take_snapshot(' || _srvid || ')');

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    UPDATE @extschema@.powa_snapshot_metas
    SET coalesce_seq = coalesce_seq + 1,
        errors = NULL,
        snapts = now()
    WHERE srvid = _srvid
    RETURNING coalesce_seq INTO purge_seq;

    PERFORM @extschema@.powa_log(format('coalesce_seq(%s): %s', _srvid, purge_seq));

    IF (_srvid = 0) THEN
        SELECT current_setting('powa.coalesce') INTO v_coalesce;
    ELSE
        SELECT powa_coalesce
        FROM @extschema@.powa_servers
        WHERE id = _srvid
        INTO v_coalesce;
    END IF;

    -- For all enabled snapshot functions in the powa_functions table, execute
    FOR r IN SELECT CASE external
                WHEN true THEN quote_ident(nsp.nspname)
                ELSE '@extschema@'
             END AS schema, function_name AS funcname
             FROM @extschema@.powa_all_functions AS pf
             LEFT JOIN pg_extension AS ext ON pf.kind = 'extension'
                AND ext.extname = pf.name
             LEFT JOIN pg_namespace AS nsp ON nsp.oid = ext.extnamespace
             WHERE operation='snapshot'
             AND enabled
             AND srvid = _srvid
             ORDER BY priority, name
    LOOP
      -- Call all of them, for the current srvid
      BEGIN
        PERFORM @extschema@.powa_log(format('calling snapshot function: %s.%I',
                                     r.schema, r.funcname));
        PERFORM set_config('application_name',
            v_title || quote_ident(r.funcname) || '(' || _srvid || ')', false);

        EXECUTE format('SELECT %s.%I(%s)', r.schema, r.funcname, _srvid);
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS
              v_state   = RETURNED_SQLSTATE,
              v_msg     = MESSAGE_TEXT,
              v_detail  = PG_EXCEPTION_DETAIL,
              v_hint    = PG_EXCEPTION_HINT,
              v_context = PG_EXCEPTION_CONTEXT;

          RAISE warning '%', format(v_pattern, _srvid, r.schema, r.funcname,
            v_state, v_msg, v_detail, v_hint, v_context);

          v_errs := array_append(v_errs, format(v_pattern_simple, _srvid,
                r.schema, r.funcname, v_msg));

          v_nb_err = v_nb_err + 1;
      END;
    END LOOP;

    -- Coalesce datas if needed
    IF ( (purge_seq % v_coalesce ) = 0 )
    THEN
      PERFORM @extschema@.powa_log(
        format('coalesce needed, srvid: %s - seq: %s - coalesce seq: %s',
        _srvid, purge_seq, v_coalesce ));

      FOR r IN SELECT CASE external
                  WHEN true THEN quote_ident(nsp.nspname)
                  ELSE '@extschema@'
               END AS schema, function_name AS funcname
               FROM @extschema@.powa_all_functions AS pf
               LEFT JOIN pg_extension AS ext ON pf.kind = 'extension'
                  AND ext.extname = pf.name
               LEFT JOIN pg_namespace AS nsp ON nsp.oid = ext.extnamespace
               WHERE operation='aggregate'
               AND enabled
               AND srvid = _srvid
               ORDER BY priority, name
      LOOP
        -- Call all of them, for the current srvid
        BEGIN
          PERFORM @extschema@.powa_log(format('calling aggregate function: %s.%I(%s)',
                r.schema, r.funcname, _srvid));

          PERFORM set_config('application_name',
              v_title || quote_ident(r.funcname) || '(' || _srvid || ')',
              false);

          EXECUTE format('SELECT %s.%I(%s)', r.schema, r.funcname, _srvid);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;

            RAISE warning '%', format(v_pattern, _srvid, r.schema, r.funcname,
                v_state, v_msg, v_detail, v_hint, v_context);

            v_errs := array_append(v_errs, format(v_pattern_simple, _srvid,
                    r.schema, r.funcname, v_msg));

            v_nb_err = v_nb_err + 1;
        END;
      END LOOP;

      PERFORM set_config('application_name',
          v_title || 'UPDATE powa_snapshot_metas.aggets',
          false);
      UPDATE @extschema@.powa_snapshot_metas
      SET aggts = now()
      WHERE srvid = _srvid;
    END IF;

    -- We also purge, at the pass after the coalesce
    IF ( (purge_seq % v_coalesce) = 1 )
    THEN
      PERFORM @extschema@.powa_log(
        format('purge needed, srvid: %s - seq: %s coalesce seq: %s',
        _srvid, purge_seq, v_coalesce));

      FOR r IN SELECT CASE external
                    WHEN true THEN quote_ident(nsp.nspname)
                    ELSE '@extschema@'
               END AS schema, function_name AS funcname
               FROM @extschema@.powa_all_functions AS pf
               LEFT JOIN pg_extension AS ext ON pf.kind = 'extension'
                  AND ext.extname = pf.name
               LEFT JOIN pg_namespace AS nsp ON nsp.oid = ext.extnamespace
               WHERE operation='purge'
               AND enabled
               AND srvid = _srvid
               ORDER BY priority, name
      LOOP
        -- Call all of them, for the current srvid
        BEGIN
          PERFORM @extschema@.powa_log(format('calling purge function: %s.%I(%s)',
                r.schema, r.funcname, _srvid));
          PERFORM set_config('application_name',
              v_title || quote_ident(r.funcname) || '(' || _srvid || ')',
              false);

          EXECUTE format('SELECT %s.%I(%s)', r.schema, r.funcname, _srvid);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;

            RAISE warning '%', format(v_pattern, _srvid, r.schema, r.funcname,
                v_state, v_msg, v_detail, v_hint, v_context);

            v_errs := array_append(v_errs, format(v_pattern_simple, _srvid,
                  r.schema, r.funcname, v_msg));

            v_nb_err = v_nb_err + 1;
        END;
      END LOOP;

      PERFORM set_config('application_name',
          v_title || 'UPDATE powa_snapshot_metas.purgets',
          false);
      UPDATE @extschema@.powa_snapshot_metas
      SET purgets = now()
      WHERE srvid = _srvid;
    END IF;

    -- and finally we call the snapshot function for the per-db catalog import,
    -- if this is a remote server
    IF (_srvid != 0) THEN
      FOR v_catname IN SELECT catname FROM @extschema@.powa_catalogs ORDER BY priority
      LOOP
        PERFORM @extschema@.powa_log(format('calling catalog function: %s.%I(%s, %s)',
              '@extschema@', 'powa_catalog_generic_snapshot', _srvid, v_catname));
        PERFORM set_config('application_name',
            v_title || quote_ident('powa_catalog_generic_snapshot')
                    || '(' || _srvid || ', ' || v_catname || ')', false);

        BEGIN
          PERFORM @extschema@.powa_catalog_generic_snapshot(_srvid, v_catname);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;

            RAISE warning '%', format(v_pattern_cat, _srvid, v_catname,
                v_state, v_msg, v_detail, v_hint, v_context);

            v_errs := array_append(v_errs, format(v_pattern_cat_simple, _srvid,
                  v_catname, v_msg));

            v_nb_err = v_nb_err + 1;
        END;
      END LOOP;
    END IF;

    IF (v_nb_err > 0) THEN
      UPDATE @extschema@.powa_snapshot_metas
      SET errors = v_errs
      WHERE srvid = _srvid;
    END IF;

    PERFORM @extschema@.powa_log('end of powa_take_snapshot(' || _srvid || ')');
    PERFORM set_config('application_name',
        v_title || 'snapshot finished',
        false);

    return v_nb_err;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_take_snapshot(int) */

CREATE OR REPLACE FUNCTION @extschema@.powa_databases_src(IN _srvid integer,
    OUT oid oid,
    OUT datname name)
RETURNS SETOF record
STABLE
AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY SELECT d.oid, d.datname
        FROM pg_catalog.pg_database d;
    ELSE
        RETURN QUERY SELECT d.oid, d.datname
        FROM @extschema@.powa_databases_src_tmp d
        WHERE srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_databases_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_databases_snapshot(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_databases_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Keep track of existing databases
    PERFORM @extschema@.powa_log('Maintaining database list...');

    WITH missing AS (
        SELECT _srvid AS srvid, d.oid, d.datname
        FROM @extschema@.powa_databases_src(_srvid) d
        LEFT JOIN @extschema@.powa_databases p ON d.oid = p.oid AND p.srvid = _srvid
        WHERE p.oid IS NULL
    )
    INSERT INTO @extschema@.powa_databases
    SELECT * FROM missing;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('missing db: %s', v_rowcount));

    -- Keep track of renamed databases
    WITH renamed AS (
        SELECT d.oid, d.datname
        FROM @extschema@.powa_databases_src(_srvid) AS d
        JOIN @extschema@.powa_databases AS p ON d.oid = p.oid AND p.srvid = _srvid
        WHERE d.datname != p.datname
    )
    UPDATE @extschema@.powa_databases AS p
    SET datname = r.datname
    FROM renamed AS r
    WHERE p.oid = r.oid
      AND p.srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('renamed db: %s', v_rowcount));

    -- Keep track of when databases are dropped
    WITH dropped AS (
        SELECT p.oid
        FROM @extschema@.powa_databases p
        LEFT JOIN @extschema@.powa_databases_src(_srvid) d ON p.oid = d.oid
        WHERE d.oid IS NULL
          AND p.dropped IS NULL
          AND p.srvid = _srvid)
    UPDATE @extschema@.powa_databases p
    SET dropped = now()
    FROM dropped d
    WHERE p.oid = d.oid
      AND p.srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('dropped db: %s', v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_databases_src_tmp WHERE srvid = _srvid;
    END IF;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_databases_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_statements_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT userid oid,
    OUT dbid oid,
    OUT toplevel boolean,
    OUT queryid bigint,
    OUT query text,
    OUT calls bigint,
    OUT total_exec_time double precision,
    OUT rows bigint,
    OUT shared_blks_hit bigint,
    OUT shared_blks_read bigint,
    OUT shared_blks_dirtied bigint,
    OUT shared_blks_written bigint,
    OUT local_blks_hit bigint,
    OUT local_blks_read bigint,
    OUT local_blks_dirtied bigint,
    OUT local_blks_written bigint,
    OUT temp_blks_read bigint,
    OUT temp_blks_written bigint,
    OUT shared_blk_read_time double precision,
    OUT shared_blk_write_time double precision,
    OUT local_blk_read_time double precision,
    OUT local_blk_write_time double precision,
    OUT temp_blk_read_time double precision,
    OUT temp_blk_write_time double precision,
    OUT plans bigint,
    OUT total_plan_time float8,
    OUT wal_records bigint,
    OUT wal_fpi bigint,
    OUT wal_bytes numeric,
    OUT jit_functions bigint,
    OUT jit_generation_time double precision,
    OUT jit_inlining_count bigint,
    OUT jit_inlining_time double precision,
    OUT jit_optimization_count bigint,
    OUT jit_optimization_time double precision,
    OUT jit_emission_count bigint,
    OUT jit_emission_time double precision,
    OUT jit_deform_count bigint,
    OUT jit_deform_time double precision
)
RETURNS SETOF record
STABLE
AS $PROC$
DECLARE
    v_pgss integer[];
    v_nsp text;
BEGIN
    IF (_srvid = 0) THEN
        SELECT regexp_split_to_array(extversion, E'\\.'), nspname
            INTO STRICT v_pgss, v_nsp
        FROM pg_catalog.pg_extension e
        JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'pg_stat_statements';

        -- pgss 1.11+, blk_(read|write)_time split in (shared|local_temp) and
        -- jit_deform_* added
        IF (v_pgss[1] = 1 AND v_pgss[2] >= 11) THEN
            RETURN QUERY EXECUTE format($$SELECT now(),
                pgss.userid, pgss.dbid, pgss.toplevel, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_exec_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written,
                pgss.shared_blk_read_time, pgss.shared_blk_write_time,
                pgss.local_blk_read_time, pgss.local_blk_write_time,
                pgss.temp_blk_read_time, pgss.temp_blk_write_time,
                pgss.plans, pgss.total_plan_time,
                pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes,
                pgss.jit_functions, pgss.jit_generation_time,
                pgss.jit_inlining_count, pgss.jit_inlining_time,
                pgss.jit_optimization_count, pgss.jit_optimization_time,
                pgss.jit_emission_count, pgss.jit_emission_time,
                pgss.jit_deform_count, pgss.jit_deform_time
            FROM %I.pg_stat_statements pgss
            JOIN pg_catalog.pg_database d ON d.oid = pgss.dbid
            JOIN pg_catalog.pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        @extschema@.powa_get_guc('powa.ignored_users', ''),
                        ',')))
            $$, v_nsp);
        -- pgss 1.10+, toplevel and some jit fields added
        ELSIF (v_pgss[1] = 1 AND v_pgss[2] >= 10) THEN
            RETURN QUERY EXECUTE format($$SELECT now(),
                pgss.userid, pgss.dbid, pgss.toplevel, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_exec_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written,
                pgss.blk_read_time AS shared_blk_read_time,
                pgss.blk_write_time AS shared_blk_write_time,
                0::double precision AS local_blk_read_time,
                0::double precision AS local_blk_write_time,
                0::double precision AS temp_blk_read_time,
                0::double precision AS temp_blk_write_time,
                pgss.plans, pgss.total_plan_time,
                pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes,
                pgss.jit_functions, pgss.jit_generation_time,
                pgss.jit_inlining_count, pgss.jit_inlining_time,
                pgss.jit_optimization_count, pgss.jit_optimization_time,
                pgss.jit_emission_count, pgss.jit_emission_time,
                0::bigint AS jit_deform_count, 0::double precision AS jit_deform_time
            FROM %I.pg_stat_statements pgss
            JOIN pg_catalog.pg_database d ON d.oid = pgss.dbid
            JOIN pg_catalog.pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        @extschema@.powa_get_guc('powa.ignored_users', ''),
                        ',')))
            $$, v_nsp);
        -- pgss 1.8+, planning counters added
        ELSIF (v_pgss[1] = 1 AND v_pgss[2] >= 8) THEN
            RETURN QUERY EXECUTE format($$SELECT now(),
                pgss.userid, pgss.dbid, true::boolean, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_exec_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written,
                pgss.blk_read_time AS shared_blk_read_time,
                pgss.blk_write_time AS shared_blk_write_time,
                0::double precision AS local_blk_read_time,
                0::double precision AS local_blk_write_time,
                0::double precision AS temp_blk_read_time,
                0::double precision AS temp_blk_write_time,
                pgss.plans, pgss.total_plan_time,
                pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes,
                0::bigint AS jit_functions, 0::double precision AS jit_generation_time,
                0::bigint AS jit_inlining_count, 0::double precision AS jit_inlining_time,
                0::bigint AS jit_optimization_count, 0::double precision AS jit_optimization_time,
                0::bigint AS jit_emission_count, 0::double precision AS jit_emission_time,
                0::bigint AS jit_deform_count, 0::double precision AS jit_deform_time
            FROM %I.pg_stat_statements pgss
            JOIN pg_catalog.pg_database d ON d.oid = pgss.dbid
            JOIN pg_catalog.pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        @extschema@.powa_get_guc('powa.ignored_users', ''),
                        ',')))
            $$, v_nsp);
        ELSE
            RETURN QUERY EXECUTE format($$SELECT now(),
                pgss.userid, pgss.dbid, true::boolean, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written,
                pgss.blk_read_time AS shared_blk_read_time,
                pgss.blk_write_time AS shared_blk_write_time,
                0::double precision AS local_blk_read_time,
                0::double precision AS local_blk_write_time,
                0::double precision AS temp_blk_read_time,
                0::double precision AS temp_blk_write_time,
                0::bigint AS plans, 0::double precision AS total_plan_time,
                0::bigint AS wal_records, 0::bigint AS wal_fpi, 0::numeric AS wal_bytes,
                0::bigint AS jit_functions, 0::double precision AS jit_generation_time,
                0::bigint AS jit_inlining_count, 0::double precision AS jit_inlining_time,
                0::bigint AS jit_optimization_count, 0::double precision AS jit_optimization_time,
                0::bigint AS jit_emission_count, 0::double precision AS jit_emission_time,
                0::bigint AS jit_deform_count, 0::double precision AS jit_deform_time
            FROM %I.pg_stat_statements pgss
            JOIN pg_catalog.pg_database d ON d.oid = pgss.dbid
            JOIN pg_catalog.pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        @extschema@.powa_get_guc('powa.ignored_users', ''),
                        ',')))
            $$, v_nsp);
        END IF;
    ELSE
        RETURN QUERY SELECT pgss.ts,
            pgss.userid, pgss.dbid, pgss.toplevel, pgss.queryid, pgss.query,
            pgss.calls, pgss.total_exec_time,
            pgss.rows, pgss.shared_blks_hit,
            pgss.shared_blks_read, pgss.shared_blks_dirtied,
            pgss.shared_blks_written, pgss.local_blks_hit,
            pgss.local_blks_read, pgss.local_blks_dirtied,
            pgss.local_blks_written, pgss.temp_blks_read,
            pgss.temp_blks_written,
            pgss.shared_blk_read_time, pgss.shared_blk_write_time,
            pgss.local_blk_read_time, pgss.local_blk_write_time,
            pgss.temp_blk_read_time, pgss.temp_blk_write_time,
            pgss.plans, pgss.total_plan_time,
            pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes,
            pgss.jit_functions, pgss.jit_generation_time,
            pgss.jit_inlining_count, pgss.jit_inlining_time,
            pgss.jit_optimization_count, pgss.jit_optimization_time,
            pgss.jit_emission_count, pgss.jit_emission_time,
            pgss.jit_deform_count, pgss.jit_deform_time
        FROM @extschema@.powa_statements_src_tmp pgss WHERE srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_statements_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_statements_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_statements_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    -- In this function, we capture statements, and also aggregate counters by database
    -- so that the first screens of powa stay reactive even though there may be thousands
    -- of different statements
    -- We only capture databases that are still there
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    WITH capture AS(
        SELECT *
        FROM @extschema@.powa_statements_src(_srvid)
    ),
    mru as (UPDATE @extschema@.powa_statements set last_present_ts = now()
            FROM capture
            WHERE @extschema@.powa_statements.queryid = capture.queryid
              AND @extschema@.powa_statements.dbid = capture.dbid
              AND @extschema@.powa_statements.userid = capture.userid
              AND @extschema@.powa_statements.srvid = _srvid
    ),
    missing_statements AS(
        INSERT INTO @extschema@.powa_statements (srvid, queryid, dbid, userid, query)
            SELECT _srvid, queryid, dbid, userid, min(query)
            FROM capture c
            WHERE NOT EXISTS (SELECT 1
                              FROM @extschema@.powa_statements ps
                              WHERE ps.queryid = c.queryid
                              AND ps.dbid = c.dbid
                              AND ps.userid = c.userid
                              AND ps.srvid = _srvid
            )
            GROUP BY queryid, dbid, userid
    ),

    by_query AS (
        INSERT INTO @extschema@.powa_statements_history_current (srvid, queryid,
                dbid, toplevel, userid, record)
            SELECT _srvid, queryid, dbid, toplevel, userid,
            ROW(
                ts, calls, total_exec_time, rows,
                shared_blks_hit, shared_blks_read, shared_blks_dirtied,
                shared_blks_written, local_blks_hit, local_blks_read,
                local_blks_dirtied, local_blks_written, temp_blks_read,
                temp_blks_written,
                shared_blk_read_time, shared_blk_write_time,
                local_blk_read_time, local_blk_write_time,
                temp_blk_read_time, temp_blk_write_time,
                plans, total_plan_time,
                wal_records, wal_fpi, wal_bytes,
                jit_functions, jit_generation_time,
                jit_inlining_count, jit_inlining_time,
                jit_optimization_count, jit_optimization_time,
                jit_emission_count, jit_emission_time,
                jit_deform_count, jit_deform_time
            )::@extschema@.powa_statements_history_record AS record
            FROM capture
    ),

    by_database AS (
        INSERT INTO @extschema@.powa_statements_history_current_db (srvid, dbid, record)
            SELECT _srvid, dbid,
            ROW(
                ts, sum(calls),
                sum(total_exec_time), sum(rows), sum(shared_blks_hit),
                sum(shared_blks_read), sum(shared_blks_dirtied),
                sum(shared_blks_written), sum(local_blks_hit),
                sum(local_blks_read), sum(local_blks_dirtied),
                sum(local_blks_written), sum(temp_blks_read),
                sum(temp_blks_written),
                sum(shared_blk_read_time), sum(shared_blk_write_time),
                sum(local_blk_read_time), sum(local_blk_write_time),
                sum(temp_blk_read_time), sum(temp_blk_write_time),
                sum(plans), sum(total_plan_time),
                sum(wal_records), sum(wal_fpi), sum(wal_bytes),
                sum(jit_functions), sum(jit_generation_time),
                sum(jit_inlining_count), sum(jit_inlining_time),
                sum(jit_optimization_count), sum(jit_optimization_time),
                sum(jit_emission_count), sum(jit_emission_time),
                sum(jit_deform_count), sum(jit_deform_time)
            )::@extschema@.powa_statements_history_record AS record
            FROM capture
            GROUP BY dbid, ts
    )

    SELECT count(*) INTO v_rowcount
    FROM capture;

    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_statements_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true; -- For now we don't care. jhat could we do on error except crash anyway?
END;
$PROC$ language plpgsql; /* end of powa_statements_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT dbid oid,
    OUT funcid oid,
    OUT calls bigint,
    OUT total_time double precision,
    OUT self_time double precision
) RETURNS SETOF record
STABLE
AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY SELECT now(), d.oid, r.funcid, r.calls, r.total_time,
            r.self_time
        FROM pg_catalog.pg_database d, @extschema@.powa_stat_user_functions(oid) r;
    ELSE
        RETURN QUERY SELECT r.ts, r.dbid, r.funcid, r.calls, r.total_time,
            r.self_time
        FROM @extschema@.powa_user_functions_src_tmp r
        WHERE r.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_user_functions_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_user_functions_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert cluster-wide user function statistics
    WITH func AS (
        SELECT *
        FROM @extschema@.powa_user_functions_src(_srvid)
    ),

    by_function AS (
        INSERT INTO @extschema@.powa_user_functions_history_current
            (srvid, dbid, funcid, record)
            SELECT _srvid, dbid, funcid,
            ROW(ts, calls, total_time, self_time
            )::@extschema@.powa_user_functions_history_record AS record
            FROM func
    ),

    by_database AS (
        INSERT INTO @extschema@.powa_user_functions_history_current_db
            (srvid, dbid, record)
            SELECT _srvid AS srvid, dbid,
            ROW(ts, sum(calls), sum(total_time), sum(self_time)
            )::@extschema@.powa_user_functions_history_record AS record
            FROM func
            GROUP BY srvid, dbid, ts
    )

    SELECT COUNT(*) INTO v_rowcount
    FROM func;

    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_user_functions_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_user_functions_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_indexes_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_all_indexes_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    ASSERT _srvid != 0, 'db module functions can only be called for remote servers';

    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert cluster-wide index statistics
    WITH rel AS (
        SELECT *
        FROM @extschema@.powa_all_indexes_src_tmp
    ),

    by_relation AS (
        INSERT INTO @extschema@.powa_all_indexes_history_current
            (srvid, dbid, relid, indexrelid, record)
            SELECT _srvid, dbid, relid, indexrelid,
            ROW(ts,
                idx_size,
                idx_scan, last_idx_scan, idx_tup_read, idx_tup_fetch,
                idx_blks_read, idx_blks_hit
            )::@extschema@.powa_all_indexes_history_record AS record
            FROM rel
    ),

    by_database AS (
        INSERT INTO @extschema@.powa_all_indexes_history_current_db
        (srvid, dbid, record)
            SELECT _srvid AS srvid, dbid,
            ROW(ts,
                sum(idx_size),
                sum(idx_scan), sum(idx_tup_read), sum(idx_tup_fetch),
                sum(idx_blks_read), sum(idx_blks_hit)
            )::@extschema@.powa_all_indexes_history_db_record
            FROM rel
            GROUP BY srvid, dbid, ts
    )

    SELECT COUNT(*) into v_rowcount
    FROM rel;

    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_indexes_src_tmp WHERE srvid = _srvid;

    result := true;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_all_indexes_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_tables_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_all_tables_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    ASSERT _srvid != 0, 'db module functions can only be called for remote servers';

    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert cluster-wide relation statistics
    WITH rel AS (
        SELECT *
        FROM @extschema@.powa_all_tables_src_tmp
    ),

    by_relation AS (
        INSERT INTO @extschema@.powa_all_tables_history_current
            (srvid, dbid, relid, record)
            SELECT _srvid, dbid, relid,
            ROW(ts,
                tbl_size,
                seq_scan, last_seq_scan, seq_tup_read, idx_scan,
                last_idx_scan, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
                n_tup_newpage_upd, n_liv_tup, n_dead_tup, n_mod_since_analyze,
                n_ins_since_vacuum, last_vacuum, last_autovacuum, last_analyze,
                last_autoanalyze, vacuum_count, autovacuum_count,
                analyze_count, autoanalyze_count, heap_blks_read,
                heap_blks_hit, idx_blks_read, idx_blks_hit, toast_blks_read,
                toast_blks_hit, tidx_blks_read,
                tidx_blks_hit
            )::@extschema@.powa_all_tables_history_record AS record
            FROM rel
    ),

    by_database AS (
        INSERT INTO @extschema@.powa_all_tables_history_current_db
            (srvid, dbid, record)
            SELECT _srvid AS srvid, dbid,
            ROW(ts,
                sum(tbl_size),
                sum(seq_scan), sum(seq_tup_read), sum(idx_scan),
                sum(n_tup_ins), sum(n_tup_upd), sum(n_tup_del),
                sum(n_tup_hot_upd), sum(n_tup_newpage_upd), sum(n_liv_tup),
                sum(n_dead_tup), sum(n_mod_since_analyze),
                sum(n_ins_since_vacuum), sum(vacuum_count),
                sum(autovacuum_count), sum(analyze_count),
                sum(autoanalyze_count), sum(heap_blks_read),
                sum(heap_blks_hit), sum(idx_blks_read), sum(idx_blks_hit),
                sum(toast_blks_read), sum(toast_blks_hit), sum(tidx_blks_read),
                sum(tidx_blks_hit)
            )::@extschema@.powa_all_tables_history_db_record
            FROM rel
            GROUP BY srvid, dbid, ts
    )

    SELECT COUNT(*) into v_rowcount
    FROM rel;

    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_tables_src_tmp WHERE srvid = _srvid;

    result := true;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_all_tables_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_replication_slots_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT slot_name text,
    OUT plugin text,
    OUT slot_type text,
    OUT datoid oid,
    OUT temporary boolean,
    OUT cur_txid xid,
    OUT current_lsn pg_lsn,
    OUT active bool,
    OUT active_pid int,
    OUT slot_xmin xid,
    OUT catalog_xmin xid,
    OUT restart_lsn pg_lsn,
    OUT confirmed_flush_lsn pg_lsn,
    OUT wal_status text,
    OUT safe_wal_size bigint,
    OUT two_phase boolean,
    OUT conflicting boolean
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_txid xid;
    v_current_lsn pg_lsn;
BEGIN
    IF (_srvid = 0) THEN
        IF pg_catalog.pg_is_in_recovery() THEN
            v_txid = NULL;
        ELSE
            v_txid = pg_catalog.txid_current();
        END IF;

        IF current_setting('server_version_num')::int < 100000 THEN
            IF pg_is_in_recovery() THEN
                v_current_lsn := pg_last_xlog_receive_location();
            ELSE
                v_current_lsn := pg_current_xlog_location();
            END IF;
        ELSE
            IF pg_is_in_recovery() THEN
                v_current_lsn := pg_last_wal_receive_lsn();
            ELSE
                v_current_lsn := pg_current_wal_lsn();
            END IF;
        END IF;

        -- We want to always return a row, even if no replication slots is
        -- found, so the UI can properly graph that no slot exists.

        -- conflicting added in pg16
        IF current_setting('server_version_num')::int >= 160000 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, s.temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn, s.wal_status,
                s.safe_wal_size, s.two_phase, s.conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- two_phase added in pg14
        ELSIF current_setting('server_version_num')::int >= 140000 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, s.temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn, s.wal_status,
                s.safe_wal_size, s.two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- wal_status and safe_wal_size added in pg13
        ELSIF current_setting('server_version_num')::int >= 130000 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, s.temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn, s.wal_status,
                s.safe_wal_size, false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- temporary added in pg10
        ELSIF current_setting('server_version_num')::int >= 100000 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, s.temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn,
                NULL::text as wal_status,
                NULL::bigint as safe_wal_size,
                false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- confirmed_flush_lsn added in pg9.6
        ELSIF current_setting('server_version_num')::int >= 90600 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, false AS temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn,
                NULL::text as wal_status,
                NULL::bigint as safe_wal_size,
                false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- active_pid added in pg9.5
        ELSIF current_setting('server_version_num')::int >= 90500 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, false AS temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, NULL::pg_lsn AS confirmed_flush_lsn,
                NULL::text as wal_status,
                NULL::bigint as safe_wal_size,
                false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        ELSE
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, false AS temporary,
                v_txid, v_current_lsn,
                s.active,
                NULL::int AS active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, NULL::pg_lsn AS confirmed_flush_lsn,
                NULL::text as wal_status,
                NULL::bigint as safe_wal_size,
                false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.slot_name, s.plugin,
            s.slot_type, s.datoid, s.temporary,
            s.cur_txid, s.current_lsn,
            s.active,
            s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
            s.restart_lsn, s.confirmed_flush_lsn, s.wal_status,
            s.safe_wal_size, s.two_phase, s.conflicting
        FROM @extschema@.powa_replication_slots_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_replication_slots_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_activity_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT cur_txid xid,
    OUT datid oid,
    OUT pid integer,
    OUT leader_pid integer,
    OUT usesysid oid,
    OUT application_name text,
    OUT client_addr inet,
    OUT backend_start timestamp with time zone,
    OUT xact_start timestamp with time zone,
    OUT query_start timestamp with time zone,
    OUT state_change timestamp with time zone,
    OUT state text,
    OUT backend_xid xid,
    OUT backend_xmin xid,
    OUT query_id bigint,
    OUT backend_type text
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    txid xid;
BEGIN
    IF (_srvid = 0) THEN
        IF pg_catalog.pg_is_in_recovery() THEN
            txid = NULL;
        ELSE
            txid = pg_catalog.txid_current();
        END IF;

        -- query_id added in pg14
        IF current_setting('server_version_num')::int >= 140000 THEN
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, s.leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, s.query_id, s.backend_type
            FROM pg_catalog.pg_stat_activity AS s;
        -- leader_pid added in pg13+
        ELSIF current_setting('server_version_num')::int >= 130000 THEN
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, s.leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, NULL::bigint AS query_id, s.backend_type
            FROM pg_catalog.pg_stat_activity AS s;
        -- backend_type added in pg10+
        ELSIF current_setting('server_version_num')::int >= 100000 THEN
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, NULL::integer AS leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, NULL::bigint AS query_id, s.backend_type
            FROM pg_catalog.pg_stat_activity AS s;
        ELSE
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, NULL::integer AS leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, NULL::bigint AS query_id,
                NULL::text AS backend_type
            FROM pg_catalog.pg_stat_activity AS s;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.cur_txid,
            s.datid, s.pid, s.leader_pid, s.usesysid,
            s.application_name, s.client_addr, s.backend_start,
            s.xact_start,
            s.query_start, s.state_change, s.state, s.backend_xid,
            s.backend_xmin, s.query_id, s.backend_type
        FROM @extschema@.powa_stat_activity_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_activity_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_archiver_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT current_wal text,
    OUT archived_count bigint,
    OUT last_archived_wal text,
    OUT last_archived_time timestamp with time zone,
    OUT failed_count bigint,
    OUT last_failed_wal text,
    OUT last_failed_time timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_current_wal text;
BEGIN
    IF (_srvid = 0) THEN
        -- get the current WAL name if possible
        IF pg_is_in_recovery() THEN
            -- there's no reliable way to get either the current WAL offset
            -- (not exposed for pure WAL-shipping replication), nor the
            -- underlying WAL file name for a given WAL offset on a standby
            v_current_wal := NULL;
        ELSE
            IF current_setting('server_version_num')::int < 100000 THEN
                v_current_wal :=  pg_walfile_name(pg_last_wal_receive_lsn());
            ELSE
                v_current_wal :=  pg_walfile_name(pg_current_wal_lsn());
            END IF;
        END IF;

        RETURN QUERY SELECT now(),
            v_current_wal,
            s.archived_count, s.last_archived_wal, s.last_archived_time,
            s.failed_count, s.last_failed_wal, s.last_failed_time
        FROM pg_catalog.pg_stat_archiver AS s;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.current_wal,
            s.archived_count, s.last_archived_wal, s.last_archived_time,
            s.failed_count, s.last_failed_wal, s.last_failed_time
        FROM @extschema@.powa_stat_archiver_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_archiver_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_bgwriter_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT buffers_clean bigint,
    OUT maxwritten_clean bigint,
    OUT buffers_backend bigint,
    OUT buffers_backend_fsync bigint,
    OUT buffers_alloc bigint
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        -- pg17+, buffers_backend* removed.  We maintain them, extracted from
        -- pg_stat_io to make UI job easier.
        IF current_setting('server_version_num')::int >= 170000 THEN
            RETURN QUERY SELECT now(),
                s.buffers_clean,
                s.maxwritten_clean, i.buffers_backend, i.buffers_backend_fsync,
                s.buffers_alloc
            FROM pg_catalog.pg_stat_bgwriter AS s
            CROSS JOIN (
                SELECT sum(writes + extends)::bigint AS buffers_backend,
                    sum(fsyncs)::bigint AS buffers_backend_fsync
                FROM pg_catalog.pg_stat_io
                WHERE backend_type = 'client backend'
            ) AS i;
        ELSE
            RETURN QUERY SELECT now(),
                s.buffers_clean,
                s.maxwritten_clean, s.buffers_backend, s.buffers_backend_fsync,
                s.buffers_alloc
            FROM pg_catalog.pg_stat_bgwriter AS s;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.buffers_clean,
            s.maxwritten_clean,
            s.buffers_backend, s.buffers_backend_fsync,
            s.buffers_alloc
        FROM @extschema@.powa_stat_bgwriter_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_bgwriter_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_checkpointer_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT num_timed bigint,
    OUT num_requested bigint,
    OUT write_time double precision,
    OUT sync_time double precision,
    OUT buffers_written bigint
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        -- pg17+, the pg_stat_checkpointer view is introduced
        IF current_setting('server_version_num')::int >= 170000 THEN
            RETURN QUERY SELECT now(),
                s.num_timed, s.num_requested,
                s.write_time, s.sync_time, s.buffers_written
            FROM pg_catalog.pg_stat_checkpointer AS s;
        -- for older versions, simulate that view getting info from
        -- pg_stat_bgwriter
        ELSE
            RETURN QUERY SELECT now(),
                s.checkpoints_timed AS num_timed,
                s.checkpoints_req AS num_requested,
                s.checkpoint_write_time AS write_time,
                s.checkpoint_sync_time AS sync_time,
                s.buffers_checkpoint AS buffers_written
            FROM pg_catalog.pg_stat_bgwriter AS s;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.num_timed, s.num_requested,
            s.write_time, s.sync_time, s.buffers_written
        FROM @extschema@.powa_stat_checkpointer_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_checkpointer_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_database_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT datid oid,
    OUT numbackends integer,
    OUT xact_commit bigint,
    OUT xact_rollback bigint,
    OUT blks_read bigint,
    OUT blks_hit bigint,
    OUT tup_returned bigint,
    OUT tup_fetched bigint,
    OUT tup_inserted bigint,
    OUT tup_updated bigint,
    OUT tup_deleted bigint,
    OUT conflicts bigint,
    OUT temp_files bigint,
    OUT temp_bytes bigint,
    OUT deadlocks bigint,
    OUT checksum_failures bigint,
    OUT checksum_last_failure timestamp with time zone,
    OUT blk_read_time double precision,
    OUT blk_write_time double precision,
    OUT session_time double precision,
    OUT active_time double precision,
    OUT idle_in_transaction_time double precision,
    OUT sessions bigint,
    OUT sessions_abandoned bigint,
    OUT sessions_fatal bigint,
    OUT sessions_killed bigint,
    OUT stats_reset timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        -- pg14+, *_time and sessions_* added
        IF current_setting('server_version_num')::int >= 140000 THEN
            RETURN QUERY SELECT now(),
            s.datid, s.numbackends, s.xact_commit, s.xact_rollback,
            s.blks_read, s.blks_hit, s.tup_returned, s.tup_fetched,
            s.tup_inserted, s.tup_updated, s.tup_deleted, s.conflicts,
            s.temp_files, s.temp_bytes, s.deadlocks, s.checksum_failures,
            s.checksum_last_failure, s.blk_read_time, s.blk_write_time,
            s.session_time, s.active_time,
            s.idle_in_transaction_time,
            s.sessions, s.sessions_abandoned,
            s.sessions_fatal, s.sessions_killed,
            s.stats_reset
            FROM pg_catalog.pg_stat_database AS s;
        -- pg12+, checksum_failures and checksum_last_failure added
        ELSIF current_setting('server_version_num')::int >= 120000 THEN
            RETURN QUERY SELECT now(),
            s.datid, s.numbackends, s.xact_commit, s.xact_rollback,
            s.blks_read, s.blks_hit, s.tup_returned, s.tup_fetched,
            s.tup_inserted, s.tup_updated, s.tup_deleted, s.conflicts,
            s.temp_files, s.temp_bytes, s.deadlocks, s.checksum_failures,
            s.checksum_last_failure, s.blk_read_time, s.blk_write_time,
            NULL::double precision AS session_time,
            NULL::double precision AS active_time,
            NULL::double precision AS idle_in_transaction_time,
            NULL::bigint AS sessions, NULL::bigint AS sessions_abandoned,
            NULL::bigint AS sessions_fatal, NULL::bigint AS sessions_killed,
            s.stats_reset
            FROM pg_catalog.pg_stat_database AS s;
        ELSE
            RETURN QUERY SELECT now(),
            s.datid, s.numbackends, s.xact_commit, s.xact_rollback,
            s.blks_read, s.blks_hit, s.tup_returned, s.tup_fetched,
            s.tup_inserted, s.tup_updated, s.tup_deleted, s.conflicts,
            s.temp_files, s.temp_bytes, s.deadlocks,
            0::bigint AS checksum_failures,
            NULL::timestamptz AS checksum_last_failure,
            s.blk_read_time, s.blk_write_time,
            NULL::double precision AS session_time,
            NULL::double precision AS active_time,
            NULL::double precision AS idle_in_transaction_time,
            NULL::bigint AS sessions, NULL::bigint AS sessions_abandoned,
            NULL::bigint AS sessions_fatal, NULL::bigint AS sessions_killed,
            s.stats_reset
            FROM pg_catalog.pg_stat_database AS s;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.datid, s.numbackends, s.xact_commit, s.xact_rollback,
            s.blks_read, s.blks_hit, s.tup_returned, s.tup_fetched,
            s.tup_inserted, s.tup_updated, s.tup_deleted, s.conflicts,
            s.temp_files, s.temp_bytes, s.deadlocks, s.checksum_failures,
            s.checksum_last_failure, s.blk_read_time, s.blk_write_time,
            s.session_time, s.active_time,
            s.idle_in_transaction_time,
            s.sessions, s.sessions_abandoned,
            s.sessions_fatal, s.sessions_killed,
            s.stats_reset
        FROM @extschema@.powa_stat_database_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_database_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_database_conflicts_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT datid oid,
    OUT confl_tablespace bigint,
    OUT confl_lock bigint,
    OUT confl_snapshot bigint,
    OUT confl_bufferpin bigint,
    OUT confl_deadlock bigint,
    OUT confl_active_logicalslot bigint
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        -- pg16+, confl_active_logicalslot added
        IF current_setting('server_version_num')::int >= 160000 THEN
            RETURN QUERY SELECT now(),
            s.datid,
            s.confl_tablespace, s.confl_lock, s.confl_snapshot,
            s.confl_bufferpin, s.confl_deadlock,
            s.confl_active_logicalslot
            FROM pg_catalog.pg_stat_database_conflicts AS s;
        ELSE
            RETURN QUERY SELECT now(),
            s.datid,
            s.confl_tablespace, s.confl_lock, s.confl_snapshot,
            s.confl_bufferpin, s.confl_deadlock,
            0::bigint AS confl_active_logicalslot
            FROM pg_catalog.pg_stat_database_conflicts AS s;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.datid,
            s.confl_tablespace, s.confl_lock, s.confl_snapshot,
            s.confl_bufferpin, s.confl_deadlock,
            s.confl_active_logicalslot
        FROM @extschema@.powa_stat_database_conflicts_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_database_conflicts_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_io_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT backend_type text,
    OUT object text,
    OUT context text,
    OUT reads bigint,
    OUT read_time double precision,
    OUT writes bigint,
    OUT write_time double precision,
    OUT writebacks bigint,
    OUT writeback_time double precision,
    OUT extends bigint,
    OUT extend_time double precision,
    OUT op_bytes bigint,
    OUT hits bigint,
    OUT evictions bigint,
    OUT reuses bigint,
    OUT fsyncs bigint,
    OUT fsync_time double precision,
    OUT stats_reset timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        -- pg16+, the view is introduced
        IF current_setting('server_version_num')::int >= 160000 THEN
            RETURN QUERY SELECT now(),
            s.backend_type, s.object, s.context,
            s.reads, s.read_time,
            s.writes, s.write_time,
            s.writebacks, s.writeback_time,
            s.extends, s.extend_time,
            s.op_bytes, s.hits,
            s.evictions, s.reuses,
            s.fsyncs, s.fsync_time,
            s.stats_reset
            FROM pg_catalog.pg_stat_io AS s;
        ELSE -- return an empty dataset for pg15- servers
            RETURN QUERY SELECT now(),
            NULL::text AS backend_type, NULL::text AS object,
            NULL::text AS context,
            0::bigint AS reads, 0::double precision AS read_time,
            0::bigint AS writes, 0::double precision AS write_time,
            0::bigint AS writebacks, 0::double precision AS writeback_time,
            0::bigint AS extends, 0::double precision AS extend_time,
            NULL::bigint AS op_bytes, 0::bigint AS hits,
            0::bigint AS evictions, 0::bigint AS reuses,
            0::bigint AS fsyncs, 0::double precision AS fsync_time,
            NULL::timestamp with time zone AS stats_reset
            WHERE false;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.backend_type, s.object, s.context,
            s.reads, s.read_time,
            s.writes, s.write_time,
            s.writebacks, s.writeback_time,
            s.extends, s.extend_time,
            s.op_bytes, s.hits,
            s.evictions, s.reuses,
            s.fsyncs, s.fsync_time,
            s.stats_reset
        FROM @extschema@.powa_stat_io_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_io_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_replication_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT current_lsn pg_lsn,
    OUT pid integer,
    OUT usename text,
    OUT application_name text,
    OUT client_addr inet,
    OUT backend_start timestamp with time zone,
    OUT backend_xmin xid,
    OUT state text,
    OUT sent_lsn pg_lsn,
    OUT write_lsn pg_lsn,
    OUT flush_lsn pg_lsn,
    OUT replay_lsn pg_lsn,
    OUT write_lag interval,
    OUT flush_lag interval,
    OUT replay_lag interval,
    OUT sync_priority integer,
    OUT sync_state text,
    OUT reply_time timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_current_lsn pg_lsn;
BEGIN
    IF (_srvid = 0) THEN
        IF current_setting('server_version_num')::int < 100000 THEN
            IF pg_is_in_recovery() THEN
                v_current_lsn := pg_last_xlog_receive_location();
            ELSE
                v_current_lsn := pg_current_xlog_location();
            END IF;
        ELSE
            IF pg_is_in_recovery() THEN
                v_current_lsn := pg_last_wal_receive_lsn();
            ELSE
                v_current_lsn := pg_current_wal_lsn();
            END IF;
        END IF;

        -- We use a LEFT JOIN on the pg_stat_replication view to make sure that
        -- we always return at least one (all-NULL) row, so client apps can
        -- detect when all the replication connections are down.
        RETURN QUERY SELECT now,
        v_current_lsn,
        s.pid, s.usename::text AS usename, s.application_name, s.client_addr,
        s.backend_start, s.backend_xmin, s.state, s.sent_lsn, s.write_lsn,
        s.flush_lsn, s.replay_lsn, s.write_lag, s.flush_lag, s.replay_lag,
        s.sync_priority, s.sync_state, s.reply_time
        FROM (SELECT now() AS now) n
        LEFT JOIN pg_catalog.pg_stat_replication AS s ON true;
    ELSE
        RETURN QUERY SELECT s.ts,
        s.current_lsn,
        s.pid, s.usename, s.application_name, s.client_addr,
        s.backend_start, s.backend_xmin, s.state, s.sent_lsn, s.write_lsn,
        s.flush_lsn, s.replay_lsn, s.write_lag, s.flush_lag, s.replay_lag,
        s.sync_priority, s.sync_state, s.reply_time
        FROM @extschema@.powa_stat_replication_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_replication_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_slru_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT name text,
    OUT blks_zeroed bigint,
    OUT blks_hit bigint,
    OUT blks_read bigint,
    OUT blks_written bigint,
    OUT blks_exists bigint,
    OUT flushes bigint,
    OUT truncates bigint,
    OUT stats_reset timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        -- pg13+, the view is introduced
        IF current_setting('server_version_num')::int >= 130000 THEN
            RETURN QUERY SELECT now(),
            s.name,
            s.blks_zeroed, s.blks_hit,
            s.blks_read, s.blks_written, s.blks_exists,
            s.flushes, s.truncates,
            s.stats_reset
            FROM pg_catalog.pg_stat_slru AS s;
        ELSE -- return an empty dataset for pg15- servers
            RETURN QUERY SELECT now(),
            NULL::text AS name,
            0::bigint AS blks_zeroed, 0::bigint AS blks_hit,
            0::bigint AS blks_read, 0::bigint AS blks_written,
            0::bigint AS blks_exists,
            0::bigint AS flushes, 0::bigint as truncates,
            NULL::timestamp with time zone AS stats_reset
            WHERE false;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.name,
            s.blks_zeroed, s.blks_hit,
            s.blks_read, s.blks_written, s.blks_exists,
            s.flushes, s.truncates,
            s.stats_reset
        FROM @extschema@.powa_stat_slru_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_slru_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT subid oid,
    OUT subname name,
    OUT worker_type text,
    OUT pid integer,
    OUT leader_pid integer,
    OUT relid oid,
    OUT received_lsn pg_lsn,
    OUT last_msg_send_time timestamp with time zone,
    OUT last_msg_receipt_time timestamp with time zone,
    OUT latest_end_lsn pg_lsn,
    OUT latest_end_time timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_pg_version_num int;
BEGIN
    IF (_srvid = 0) THEN
        v_pg_version_num := current_setting('server_version_num')::int;

        -- pg17+, worker_type is added
        IF v_pg_version_num >= 170000 THEN
            RETURN QUERY SELECT now(),
            s.subid, s.subname,
            s.worker_type,
            s.pid, s.leader_pid,
            s.relid, s.received_lsn,
            s.last_msg_send_time, s.last_msg_receipt_time,
            s.latest_end_lsn, s.latest_end_time
            FROM pg_catalog.pg_stat_subscription AS s;
        -- pg16+, leader_pid is added
        ELSIF v_pg_version_num >= 160000 THEN
            RETURN QUERY SELECT now(),
            s.subid, s.subname,
            'apply'::text AS worker_type,
            s.pid, s.leader_pid,
            s.relid, s.received_lsn,
            s.last_msg_send_time, s.last_msg_receipt_time,
            s.latest_end_lsn, s.latest_end_time
            FROM pg_catalog.pg_stat_subscription AS s;
        -- pg10+, the view is introduced
        ELSIF v_pg_version_num >= 100000 THEN
            RETURN QUERY SELECT now(),
            s.subid, s.subname,
            'apply'::text AS worker_type,
            s.pid, NULL::integer AS leader_pid,
            s.relid, s.received_lsn,
            s.last_msg_send_time, s.last_msg_receipt_time,
            s.latest_end_lsn, s.latest_end_time
            FROM pg_catalog.pg_stat_subscription AS s;
        ELSE -- return an empty dataset for pg9.6- servers
            RETURN QUERY SELECT now(),
            0::oid AS subid, '' AS subname,
            ''::text AS worker_type,
            0::oid AS pid, NULL::integer AS leader_pid,
            0::oid AS relid, NULL::pg_lsn AS received_lsn,
            NULL::timestamp with time zone AS last_msg_send_time,
            NULL::timestamp with time zone AS last_msg_receipt_time,
            NULL::pg_lsn AS latest_end_lsn,
            NULL::timestamp with time zone AS latest_end_time
            WHERE false;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.subid, s.subname,
            s.worker_type,
            s.pid, s.leader_pid,
            s.relid, s.received_lsn,
            s.last_msg_send_time, s.last_msg_receipt_time,
            s.latest_end_lsn, s.latest_end_time
        FROM @extschema@.powa_stat_subscription_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_subscription_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_stats_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT subid oid,
    OUT apply_error_count bigint,
    OUT sync_error_count bigint,
    OUT stats_reset timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_pg_version_num int;
BEGIN
    IF (_srvid = 0) THEN
        v_pg_version_num := current_setting('server_version_num')::int;

        -- pg15+, the view is introduced
        IF v_pg_version_num >= 150000 THEN
            RETURN QUERY SELECT now(),
            s.subid,
            s.apply_error_count, s.sync_error_count,
            s.stats_reset
            FROM pg_catalog.pg_stat_subscription_stats AS s;
        ELSE -- return an empty dataset for pg9.6- servers
            RETURN QUERY SELECT now(),
            0::oid AS subid,
            0::bigint AS apply_error_count, 0::bigint AS sync_error_count,
            NULL::timestamp with time zone AS stats_reset
            WHERE false;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.subid,
            s.apply_error_count, s.sync_error_count,
            s.stats_reset
        FROM @extschema@.powa_stat_subscription_stats_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_subscription_stats_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_wal_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT wal_records bigint,
    OUT wal_fpi bigint,
    OUT wal_bytes numeric,
    OUT wal_buffers_full bigint,
    OUT wal_write bigint,
    OUT wal_sync bigint,
    OUT wal_write_time double precision,
    OUT wal_sync_time double precision,
    OUT stats_reset timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        -- pg14+, the view is introduced
        IF current_setting('server_version_num')::int >= 140000 THEN
            RETURN QUERY SELECT now(),
            s.wal_records, s.wal_fpi, s.wal_bytes,
            s.wal_buffers_full,
            s.wal_write, s.wal_sync,
            s.wal_write_time, s.wal_sync_time,
            s.stats_reset
            FROM pg_catalog.pg_stat_wal AS s;
        ELSE -- return an empty dataset for pg15- servers
            RETURN QUERY SELECT now(),
            0::bigint AS wal_records, 0::bigint AS wal_fpi,
            0::numeric AS wal_bytes,
            0::bigint AS wal_buffers_full,
            0::bigint AS wal_write, 0::bigint AS wal_sync,
            0::double precision AS wal_write_time,
            0::double precision AS wal_sync_time,
            NULL::timestamp with time zone AS stats_reset
            WHERE false;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.wal_records, s.wal_fpi, s.wal_bytes,
            s.wal_buffers_full,
            s.wal_write, s.wal_sync,
            s.wal_write_time, s.wal_sync_time,
            s.stats_reset
        FROM @extschema@.powa_stat_wal_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_wal_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_wal_receiver_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT slot_name text,
    OUT sender_host text,
    OUT sender_port integer,
    OUT pid integer,
    OUT status text,
    OUT receive_start_lsn pg_lsn,
    OUT receive_start_tli integer,
    OUT last_received_lsn pg_lsn,
    OUT written_lsn pg_lsn,
    OUT flushed_lsn pg_lsn,
    OUT received_tli integer,
    OUT last_msg_send_time timestamp with time zone,
    OUT last_msg_receipt_time timestamp with time zone,
    OUT latest_end_lsn pg_lsn,
    OUT latest_end_time timestamp with time zone,
    OUT conninfo text
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_pg_version_num int;
    v_current_lsn pg_lsn;
BEGIN
    IF (_srvid = 0) THEN
        v_pg_version_num := current_setting('server_version_num')::int;

         -- return an empty dataset for pg9.5- servers or non-standby
        IF (NOT pg_is_in_recovery()
            OR v_pg_version_num < 90600
        ) THEN
            RETURN QUERY SELECT now(),
            ''::text AS slot_name,
            ''::text AS sender_host, 0::integer AS sender_port,
            0::integer pid, ''::text AS status,
            NULL::pg_lsn AS receive_start_lsn, 0::integer AS receive_start_tli,
            NULL::pg_lsn AS last_received_lsn,
            NULL::pg_lsn AS written_lsn, NULL::pg_lsn AS flushed_lsn,
            0::integer AS received_tli,
            NULL::timestamp with time zone AS last_msg_send_time,
            NULL::timestamp with time zone AS last_msg_receipt_time,
            NULL::pg_lsn AS latest_end_lsn,
            NULL::timestamp with time zone AS latest_end_time,
            ''::text AS conninfo
            WHERE false;
        END IF;

        IF v_pg_version_num < 100000 THEN
            v_current_lsn := pg_last_xlog_receive_location();
        ELSE
            v_current_lsn := pg_last_wal_receive_lsn();
        END IF;

        -- pg13+, received_lsn split in written_lsn and flushed_lsn
        IF v_pg_version_num >= 130000 THEN
            RETURN QUERY SELECT now(),
            s.slot_name,
            s.sender_host, s.sender_port,
            s.pid, s.status,
            s.receive_start_lsn, s.receive_start_tli,
            v_current_lsn,
            s.written_lsn, s.flushed_lsn,
            s.received_tli,
            s.last_msg_send_time,
            s.last_msg_receipt_time,
            s.latest_end_lsn,
            s.latest_end_time,
            s.conninfo
            FROM pg_catalog.pg_stat_wal_receiver AS s;
        -- pg11+, sender_host and sender_port added
        ELSIF v_pg_version_num >= 110000 THEN
            RETURN QUERY SELECT now(),
            s.slot_name,
            s.sender_host, s.sender_port,
            s.pid, s.status,
            s.receive_start_lsn, s.receive_start_tli,
            v_current_lsn,
            NULL::pg_lsn AS written_lsn, s.received_lsn AS flushed_lsn,
            s.received_tli,
            s.last_msg_send_time,
            s.last_msg_receipt_time,
            s.latest_end_lsn,
            s.latest_end_time,
            s.conninfo
            FROM pg_catalog.pg_stat_wal_receiver AS s;
        -- pg9.6+, the view is introduced
        ELSIF v_pg_version_num >= 90600 THEN
            RETURN QUERY SELECT now(),
            s.slot_name,
            NULL::text AS sender_host, NULL::integer AS sender_port,
            s.pid, s.status,
            s.receive_start_lsn, s.receive_start_tli,
            v_current_lsn,
            NULL::pg_lsn AS written_lsn, s.received_lsn AS flushed_lsn,
            s.received_tli,
            s.last_msg_send_time,
            s.last_msg_receipt_time,
            s.latest_end_lsn,
            s.latest_end_time,
            s.conninfo
            FROM pg_catalog.pg_stat_wal_receiver AS s;
        ELSE
            -- already handled above
            RAISE EXCEPTION 'bug in powa_stat_wal_receiver_src_tmp';
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.slot_name,
            s.sender_host, s.sender_port,
            s.pid, s.status,
            s.receive_start_lsn, s.receive_start_tli,
            s.last_received_lsn,
            s.written_lsn, s.flushed_lsn,
            s.received_tli,
            s.last_msg_send_time,
            s.last_msg_receipt_time,
            s.latest_end_lsn,
            s.latest_end_time,
            s.conninfo
        FROM @extschema@.powa_stat_wal_receiver_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_wal_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_catalog_database_src(IN _srvid integer,
    OUT oid oid,
    OUT datname text
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY SELECT
            d.oid, d.datname::text
        FROM pg_catalog.pg_database AS d;
    ELSE
        RETURN QUERY SELECT
            d.oid, d.datname
        FROM @extschema@.powa_catalog_database_src_tmp AS d
        WHERE d.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_catalog_database_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_catalog_role_src(IN _srvid integer,
    OUT oid oid,
    OUT rolname text, OUT rolsuper boolean, OUT rolinherit boolean,
    OUT rolcreaterole boolean, OUT rolcreatedb boolean, OUT rolcanlogin
    boolean, OUT rolreplication boolean, OUT rolbypassrls boolean
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        IF current_setting('server_version_num')::int < 90500 THEN
            RETURN QUERY SELECT
                r.oid, r.rolname::text AS rolname, r.rolsuper, r.rolinherit,
                r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
                r.rolreplication, false AS rolbypassrls
            FROM pg_catalog.pg_roles AS r;
        ELSE
            RETURN QUERY SELECT
                r.oid, r.rolname::text AS rolname, r.rolsuper, r.rolinherit,
                r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
                r.rolreplication, r.rolbypassrls
            FROM pg_catalog.pg_roles AS r;
        END IF;
    ELSE
        RETURN QUERY SELECT
            r.oid, r.rolname, r.rolsuper, r.rolinherit,
            r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
            r.rolreplication, r.rolbypassrls
        FROM @extschema@.powa_catalog_role_src_tmp AS r
        WHERE r.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_catalog_role_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_catalog_database_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_catalog_database_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    WITH src AS (
        SELECT * FROM @extschema@.powa_catalog_database_src(_srvid)
    ),
    remove_deleted AS (
        DELETE FROM @extschema@.powa_catalog_databases AS d
        WHERE d.srvid = _srvid
        AND d.oid NOT IN (
            SELECT oid
            FROM src
        )
    ),
    add_new AS (
        INSERT INTO @extschema@.powa_catalog_databases (srvid, oid, datname)
        SELECT _srvid, src.oid, src.datname
        FROM src
        WHERE src.oid NOT IN (
            SELECT oid
            FROM @extschema@.powa_catalog_databases AS d
            WHERE d.srvid = _srvid
        )
    )
    UPDATE @extschema@.powa_catalog_databases
    SET datname = src.datname
    FROM src
    WHERE powa_catalog_databases.srvid = _srvid
    AND powa_catalog_databases.oid = src.oid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_catalog_database_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_catalog_database_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_catalog_role_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_catalog_role_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    WITH src AS (
        SELECT * FROM @extschema@.powa_catalog_role_src(_srvid)
    ),
    remove_deleted AS (
        DELETE FROM @extschema@.powa_catalog_roles AS r
        WHERE r.srvid = _srvid
        AND r.oid NOT IN (
            SELECT oid
            FROM src
        )
    ),
    add_new AS (
        INSERT INTO @extschema@.powa_catalog_roles (srvid, oid, rolname,
            rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin,
            rolreplication, rolbypassrls)
        SELECT _srvid, src.oid, src.rolname, src.rolsuper, src.rolinherit,
            src.rolcreaterole, src.rolcreatedb, src.rolcanlogin,
            src.rolreplication, src.rolbypassrls
        FROM src
        WHERE src.oid NOT IN (
            SELECT oid
            FROM @extschema@.powa_catalog_roles AS r
            WHERE r.srvid = _srvid
        )
    )
    UPDATE @extschema@.powa_catalog_roles
    SET rolname = src.rolname, rolsuper = src.rolsuper,
        rolinherit = src.rolinherit, rolcreaterole = src.rolcreaterole,
        rolcreatedb = src.rolcreatedb, rolcanlogin = src.rolcanlogin,
        rolreplication = src.rolreplication, rolbypassrls = src.rolbypassrls
    FROM src
    WHERE powa_catalog_roles.srvid = _srvid
    AND powa_catalog_roles.oid = src.oid
    AND (powa_catalog_roles.rolname != src.rolname
         OR powa_catalog_roles.rolsuper != src.rolsuper
         OR powa_catalog_roles.rolinherit != src.rolinherit
         OR powa_catalog_roles.rolcreaterole != src.rolcreaterole
         OR powa_catalog_roles.rolcreatedb != src.rolcreatedb
         OR powa_catalog_roles.rolcanlogin != src.rolcanlogin
         OR powa_catalog_roles.rolreplication != src.rolreplication
         OR powa_catalog_roles.rolbypassrls != src.rolbypassrls);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_catalog_role_src_tmp WHERE srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_catalog_role_snapshot() */

CREATE OR REPLACE FUNCTION @extschema@.powa_catalog_generic_snapshot(_srvid integer,
    _catname text)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s, %s)',
                                 'powa_catalog_generic_snapshot',
                                 _srvid, _catname);
    v_rowcount    bigint;
    v_prefix      text;
    v_src_tmp     text;
    v_query       text;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- get the table prefix and src_tmp table name
    SELECT 'powa_catalog_' || replace(_catname, 'pg_', '')
        INTO STRICT v_prefix;

    SELECT quote_ident(v_prefix || '_src_tmp') INTO STRICT v_src_tmp;

    -- bail out if there's no source data
    EXECUTE format('SELECT 1 FROM @extschema@.%s WHERE srvid = %s LIMIT 1',
        v_src_tmp, _srvid) INTO v_rowcount;

    IF v_rowcount IS NULL THEN
        RETURN;
    END IF;

    -- Remove all records for the given server.
    -- Note that only remove record for found database oid so we can handle
    -- partial per-db snapshot.  This has to be done in a different step as
    -- wCTE don't see the results of previous wCTE.
    -- If a database is removed from a remote server, all the underyling
    -- records will already be removed when cascading the delete in the
    -- powa_catalog_databases table.
    EXECUTE format('DELETE FROM @extschema@.%s
        WHERE srvid = %s
        AND dbid IN (SELECT DISTINCT dbid
            FROM @extschema@.%s
            WHERE srvid = %s
    )', v_prefix, _srvid, v_src_tmp, _srvid);

    -- Insert the new records.
    -- We also finally save the refresh time.  We only want to do it once per
    -- remote server and not once per catalog, so arbitrarily do that for the
    -- pg_class catalog only, which is done last.
    v_query := format('WITH src AS (
             DELETE FROM @extschema@.%1$s
             WHERE srvid = %3$s
             RETURNING *
        ),
        metadata AS (
            UPDATE @extschema@.powa_catalog_databases
            SET last_refresh = now()
            WHERE srvid = %3$s
            AND %4$L = ''pg_class''
            AND oid IN (SELECT DISTINCT dbid
                FROM src)
        )
        INSERT INTO @extschema@.%2$s
        SELECT *
        FROM src', v_src_tmp, v_prefix, _srvid, _catname);

    -- execute it
    EXECUTE v_query;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_catalog_generic_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_catalog_reset(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_catalog_reset',
                                 _srvid);
    v_catname     text;
    v_prefix      text;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    FOR v_catname IN SELECT catname FROM @extschema@.powa_catalogs
    LOOP
        SELECT 'powa_catalog_' || replace(v_catname, 'pg_', '')
            INTO STRICT v_prefix;

        PERFORM format('DELETE FROM @extschema@.%I WHERE srvid = %s',
            v_prefix, _srvid);
        PERFORM format('DELETE FROM @extschema@.%I WHERE srvid = %s',
            v_prefix || '_src_tmp', _srvid);
    END LOOP;
END;
$PROC$ language plpgsql; /* end of powa_catalog_reset */


CREATE OR REPLACE FUNCTION @extschema@.powa_databases_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_databases_purge', _srvid);
    v_rowcount    bigint;
    v_dropped_dbid oid[];
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Cleanup old dropped databases, over retention
    -- This will cascade automatically to powa_statements and other
    WITH dropped_databases AS
      ( DELETE FROM @extschema@.powa_databases
        WHERE dropped < (now() - v_retention * 1.2)
        AND srvid = _srvid
        RETURNING oid
        )
    SELECT array_agg(oid) INTO v_dropped_dbid FROM dropped_databases;

    PERFORM @extschema@.powa_log(format('%s (powa_databases) - rowcount: %s)',
           v_funcname,array_length(v_dropped_dbid,1)));
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_databases_purge */


CREATE OR REPLACE FUNCTION @extschema@.powa_statements_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_statements_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete data. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_statements_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_statements_hitory) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_statements_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_statements
    WHERE last_present_ts < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_statements) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_user_functions_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_user_functions_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_user_functions_history) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_user_functions_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_user_functions_history_db) rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_user_functions_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_indexes_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_all_indexes_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_all_indexes_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_indexes_history) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_indexes_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_indexes_history_db) rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_indexes_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_tables_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_all_tables_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_all_tables_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_tables_history) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_tables_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_tables_history_db) rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_all_tables_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_statements_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_statements_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate statements table
    INSERT INTO @extschema@.powa_statements_history (srvid, queryid, dbid, toplevel,
            userid, coalesce_range, records, mins_in_range, maxs_in_range)
        SELECT srvid, queryid, dbid, toplevel, userid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).calls),min((record).total_exec_time),
                min((record).rows),
                min((record).shared_blks_hit),min((record).shared_blks_read),
                min((record).shared_blks_dirtied),min((record).shared_blks_written),
                min((record).local_blks_hit),min((record).local_blks_read),
                min((record).local_blks_dirtied),min((record).local_blks_written),
                min((record).temp_blks_read),min((record).temp_blks_written),
                min((record).shared_blk_read_time),min((record).shared_blk_write_time),
                min((record).local_blk_read_time),min((record).local_blk_write_time),
                min((record).temp_blk_read_time),min((record).temp_blk_write_time),
                min((record).plans),min((record).total_plan_time),
                min((record).wal_records),min((record).wal_fpi),
                min((record).wal_bytes),
                min((record).jit_functions), min((record).jit_generation_time),
                min((record).jit_inlining_count), min((record).jit_inlining_time),
                min((record).jit_optimization_count), min((record).jit_optimization_time),
                min((record).jit_emission_count), min((record).jit_emission_time),
                min((record).jit_deform_count), min((record).jit_deform_time)
            )::@extschema@.powa_statements_history_record,
            ROW(max((record).ts),
                max((record).calls),max((record).total_exec_time),
                max((record).rows),
                max((record).shared_blks_hit),max((record).shared_blks_read),
                max((record).shared_blks_dirtied),max((record).shared_blks_written),
                max((record).local_blks_hit),max((record).local_blks_read),
                max((record).local_blks_dirtied),max((record).local_blks_written),
                max((record).temp_blks_read),max((record).temp_blks_written),
                max((record).shared_blk_read_time),max((record).shared_blk_write_time),
                max((record).local_blk_read_time),max((record).local_blk_write_time),
                max((record).temp_blk_read_time),max((record).temp_blk_write_time),
                max((record).plans),max((record).total_plan_time),
                max((record).wal_records),max((record).wal_fpi),
                max((record).wal_bytes),
                max((record).jit_functions), max((record).jit_generation_time),
                max((record).jit_inlining_count), max((record).jit_inlining_time),
                max((record).jit_optimization_count), max((record).jit_optimization_time),
                max((record).jit_emission_count), max((record).jit_emission_time),
                max((record).jit_deform_count), max((record).jit_deform_time)
            )::@extschema@.powa_statements_history_record
        FROM @extschema@.powa_statements_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, queryid, dbid, toplevel, userid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_statements_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_statements_history_current WHERE srvid = _srvid;

    -- aggregate db table
    INSERT INTO @extschema@.powa_statements_history_db (srvid, dbid, coalesce_range,
            records, mins_in_range, maxs_in_range)
        SELECT srvid, dbid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).calls),min((record).total_exec_time),
                min((record).rows),
                min((record).shared_blks_hit),min((record).shared_blks_read),
                min((record).shared_blks_dirtied),min((record).shared_blks_written),
                min((record).local_blks_hit),min((record).local_blks_read),
                min((record).local_blks_dirtied),min((record).local_blks_written),
                min((record).temp_blks_read),min((record).temp_blks_written),
                min((record).shared_blk_read_time),min((record).shared_blk_write_time),
                min((record).local_blk_read_time),min((record).local_blk_write_time),
                min((record).temp_blk_read_time),min((record).temp_blk_write_time),
                min((record).plans),min((record).total_plan_time),
                min((record).wal_records),min((record).wal_fpi),
                min((record).wal_bytes),
                min((record).jit_functions), min((record).jit_generation_time),
                min((record).jit_inlining_count), min((record).jit_inlining_time),
                min((record).jit_optimization_count), min((record).jit_optimization_time),
                min((record).jit_emission_count), min((record).jit_emission_time),
                min((record).jit_deform_count), min((record).jit_deform_time)
            )::@extschema@.powa_statements_history_record,
            ROW(max((record).ts),
                max((record).calls),max((record).total_exec_time),
                max((record).rows),
                max((record).shared_blks_hit),max((record).shared_blks_read),
                max((record).shared_blks_dirtied),max((record).shared_blks_written),
                max((record).local_blks_hit),max((record).local_blks_read),
                max((record).local_blks_dirtied),max((record).local_blks_written),
                max((record).temp_blks_read),max((record).temp_blks_written),
                max((record).shared_blk_read_time),max((record).shared_blk_write_time),
                max((record).local_blk_read_time),max((record).local_blk_write_time),
                max((record).temp_blk_read_time),max((record).temp_blk_write_time),
                max((record).plans),max((record).total_plan_time),
                max((record).wal_records),max((record).wal_fpi),
                max((record).wal_bytes),
                max((record).jit_functions), max((record).jit_generation_time),
                max((record).jit_inlining_count), max((record).jit_inlining_time),
                max((record).jit_optimization_count), max((record).jit_optimization_time),
                max((record).jit_emission_count), max((record).jit_emission_time),
                max((record).jit_deform_count), max((record).jit_deform_time)
            )::@extschema@.powa_statements_history_record
        FROM @extschema@.powa_statements_history_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_statements_history_current_db WHERE srvid = _srvid;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_statements_aggregate */

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_user_functions_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log('running powa_user_functions_aggregate(' || _srvid ||')');

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate user_functions table
    INSERT INTO @extschema@.powa_user_functions_history
        (srvid, dbid, funcid, coalesce_range, records,
                mins_in_range, maxs_in_range)
        SELECT srvid, dbid, funcid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts), min((record).calls),min((record).total_time),
                min((record).self_time))::@extschema@.powa_user_functions_history_record,
            ROW(max((record).ts), max((record).calls),max((record).total_time),
                max((record).self_time))::@extschema@.powa_user_functions_history_record
        FROM @extschema@.powa_user_functions_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, funcid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_user_functions_history_current) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_user_functions_history_current WHERE srvid = _srvid;

    -- aggregate user_functions_db table
    INSERT INTO @extschema@.powa_user_functions_history_db
        (srvid, dbid, coalesce_range, records, mins_in_range, maxs_in_range)
        SELECT srvid, dbid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts), min((record).calls),min((record).total_time),
                min((record).self_time))::@extschema@.powa_user_functions_history_record,
            ROW(max((record).ts), max((record).calls),max((record).total_time),
                max((record).self_time))::@extschema@.powa_user_functions_history_record
        FROM @extschema@.powa_user_functions_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, dbid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_user_functions_history_current_db) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_user_functions_history_current_db WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_user_functions_aggregate */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_indexes_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_all_indexes_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate all_indexes table
    INSERT INTO @extschema@.powa_all_indexes_history
        (srvid, dbid, relid, indexrelid, coalesce_range, records,
                mins_in_range, maxs_in_range)
        SELECT srvid, dbid, relid, indexrelid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).idx_size),
                min((record).idx_scan), min((record).last_idx_scan),
                min((record).idx_tup_read), min((record).idx_tup_fetch),
                min((record).idx_blks_read), min((record).idx_blks_hit)
            )::@extschema@.powa_all_indexes_history_record,
            ROW(max((record).ts),
                max((record).idx_size),
                max((record).idx_scan), max((record).last_idx_scan),
                max((record).idx_tup_read), max((record).idx_tup_fetch),
                max((record).idx_blks_read), max((record).idx_blks_hit)
            )::@extschema@.powa_all_indexes_history_record
        FROM @extschema@.powa_all_indexes_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, relid, indexrelid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_indexes_history_current) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_indexes_history_current WHERE srvid = _srvid;

    -- aggregate all_indexes_db table
    INSERT INTO @extschema@.powa_all_indexes_history_db
        (srvid, dbid, coalesce_range, records, mins_in_range, maxs_in_range)
        SELECT srvid, dbid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).idx_size),
                min((record).idx_scan),
                min((record).idx_tup_read), min((record).idx_tup_fetch),
                min((record).idx_blks_read), min((record).idx_blks_hit)
            )::@extschema@.powa_all_indexes_history_db_record,
            ROW(max((record).ts),
                max((record).idx_size),
                max((record).idx_scan),
                max((record).idx_tup_read), max((record).idx_tup_fetch),
                max((record).idx_blks_read), max((record).idx_blks_hit)
            )::@extschema@.powa_all_indexes_history_db_record
        FROM @extschema@.powa_all_indexes_history_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_all_indexes_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_indexes_history_current_db WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_all_indexes_aggregate */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_tables_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_all_tables_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate all_tables table
    INSERT INTO @extschema@.powa_all_tables_history
        (srvid, dbid, relid, coalesce_range, records,
                mins_in_range, maxs_in_range)
        SELECT srvid, dbid, relid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).tbl_size),
                min((record).seq_scan), min((record).last_seq_scan),
                min((record).seq_tup_read), min((record).idx_scan),
                min((record).last_idx_scan), min((record).n_tup_ins),
                min((record).n_tup_upd), min((record).n_tup_del),
                min((record).n_tup_hot_upd), min((record).n_tup_newpage_upd),
                min((record).n_liv_tup), min((record).n_dead_tup),
                min((record).n_mod_since_analyze),
                min((record).n_ins_since_vacuum), min((record).last_vacuum),
                min((record).last_autovacuum), min((record).last_analyze),
                min((record).last_autoanalyze), min((record).vacuum_count),
                min((record).autovacuum_count), min((record).analyze_count),
                min((record).autoanalyze_count), min((record).heap_blks_read),
                min((record).heap_blks_hit), min((record).idx_blks_read),
                min((record).idx_blks_hit), min((record).toast_blks_read),
                min((record).toast_blks_hit), min((record).tidx_blks_read),
                min((record).tidx_blks_hit)
            )::@extschema@.powa_all_tables_history_record,
            ROW(max((record).ts),
                max((record).tbl_size),
                max((record).seq_scan), max((record).last_seq_scan),
                max((record).seq_tup_read), max((record).idx_scan),
                max((record).last_idx_scan), max((record).n_tup_ins),
                max((record).n_tup_upd), max((record).n_tup_del),
                max((record).n_tup_hot_upd), max((record).n_tup_newpage_upd),
                max((record).n_liv_tup), max((record).n_dead_tup),
                max((record).n_mod_since_analyze),
                max((record).n_ins_since_vacuum), max((record).last_vacuum),
                max((record).last_autovacuum), max((record).last_analyze),
                max((record).last_autoanalyze), max((record).vacuum_count),
                max((record).autovacuum_count), max((record).analyze_count),
                max((record).autoanalyze_count), max((record).heap_blks_read),
                max((record).heap_blks_hit), max((record).idx_blks_read),
                max((record).idx_blks_hit), max((record).toast_blks_read),
                max((record).toast_blks_hit), max((record).tidx_blks_read),
                max((record).tidx_blks_hit)
            )::@extschema@.powa_all_tables_history_record
        FROM @extschema@.powa_all_tables_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, relid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_tables_history_current) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_tables_history_current WHERE srvid = _srvid;

    -- aggregate all_tables_db table
    INSERT INTO @extschema@.powa_all_tables_history_db
        (srvid, dbid, coalesce_range, records, mins_in_range, maxs_in_range)
        SELECT srvid, dbid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).tbl_size),
                min((record).seq_scan), min((record).seq_tup_read),
                min((record).idx_scan), min((record).n_tup_ins),
                min((record).n_tup_upd), min((record).n_tup_del),
                min((record).n_tup_hot_upd), min((record).n_tup_newpage_upd),
                min((record).n_liv_tup), min((record).n_dead_tup),
                min((record).n_mod_since_analyze),
                min((record).n_ins_since_vacuum), min((record).vacuum_count),
                min((record).autovacuum_count), min((record).analyze_count),
                min((record).autoanalyze_count), min((record).heap_blks_read),
                min((record).heap_blks_hit), min((record).idx_blks_read),
                min((record).idx_blks_hit), min((record).toast_blks_read),
                min((record).toast_blks_hit), min((record).tidx_blks_read),
                min((record).tidx_blks_hit)
            )::@extschema@.powa_all_tables_history_db_record,
            ROW(max((record).ts),
                max((record).tbl_size),
                max((record).seq_scan), max((record).seq_tup_read),
                max((record).idx_scan), max((record).n_tup_ins),
                max((record).n_tup_upd), max((record).n_tup_del),
                max((record).n_tup_hot_upd), max((record).n_tup_newpage_upd),
                max((record).n_liv_tup), max((record).n_dead_tup),
                max((record).n_mod_since_analyze),
                max((record).n_ins_since_vacuum), max((record).vacuum_count),
                max((record).autovacuum_count), max((record).analyze_count),
                max((record).autoanalyze_count), max((record).heap_blks_read),
                max((record).heap_blks_hit), max((record).idx_blks_read),
                max((record).idx_blks_hit), max((record).toast_blks_read),
                max((record).toast_blks_hit), max((record).tidx_blks_read),
                max((record).tidx_blks_hit)
            )::@extschema@.powa_all_tables_history_db_record
        FROM @extschema@.powa_all_tables_history_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_all_tables_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_tables_history_current_db WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_all_tables_aggregate */

CREATE OR REPLACE FUNCTION @extschema@.powa_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
  r         record;
  v_state   text;
  v_msg     text;
  v_detail  text;
  v_hint    text;
  v_context text;
BEGIN
    -- Find reset function for every supported datasource, including pgss
    FOR r IN SELECT CASE external
                WHEN true THEN quote_ident(nsp.nspname)
                ELSE '@extschema@'
            END AS schema, function_name AS funcname
            FROM @extschema@.powa_all_functions AS pf
            LEFT JOIN pg_extension AS ext ON pf.kind = 'extension'
               AND ext.extname = pf.name
            LEFT JOIN pg_namespace AS nsp ON nsp.oid = ext.extnamespace
            WHERE operation='reset'
            AND srvid = _srvid
            ORDER BY priority, name LOOP
      -- Call all of them, for the current srvid
      BEGIN
          EXECUTE format('SELECT %s.%I(%s)', r.schema, r.funcname, _srvid);
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS
              v_state   = RETURNED_SQLSTATE,
              v_msg     = MESSAGE_TEXT,
              v_detail  = PG_EXCEPTION_DETAIL,
              v_hint    = PG_EXCEPTION_HINT,
              v_context = PG_EXCEPTION_CONTEXT;
          RAISE warning 'powa_reset(): function "%.%(%)" failed:
              state  : %
              message: %
              detail : %
              hint   : %
              context: %',
              r.schema, quote_ident(r.funcname), _srvid, v_state, v_msg,
              v_detail, v_hint, v_context;

      END;
    END LOOP;

    -- And reset all catalogs
    BEGIN
      PERFORM @extschema@.powa_catalog_reset(_srvid);
    EXCEPTION
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT;
        RAISE warning 'powa_reset(): function "@extschema@.powa_catalog_reset(%)" failed:
            state  : %
            message: %
            detail : %
            hint   : %
            context: %',
            _srvid, v_state, v_msg, v_detail, v_hint, v_context;
    END;

    RETURN true;
END;
$function$
SET search_path = pg_catalog; /* end of powa_reset */

CREATE OR REPLACE FUNCTION @extschema@.powa_statements_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM @extschema@.powa_log('Resetting powa_statements_history(' || _srvid || ')');
    DELETE FROM @extschema@.powa_statements_history WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_statements_history_current(' || _srvid || ')');
    DELETE FROM @extschema@.powa_statements_history_current WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_statements_history_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_statements_history_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_statements_history_current_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_statements_history_current_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_statements_src_tmp(' || _srvid || ')');
    DELETE FROM @extschema@.powa_statements_src_tmp WHERE srvid = _srvid;

    -- if 3rd part datasource has FK on it, throw everything away
    DELETE FROM @extschema@.powa_statements WHERE srvid = _srvid;
    PERFORM @extschema@.powa_log('Resetting powa_statements(' || _srvid || ')');

    RETURN true;
END;
$function$
SET search_path = pg_catalog; /* end of powa_statements_reset */

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM @extschema@.powa_log('Resetting powa_user_functions_history(' || _srvid || ')');
    DELETE FROM @extschema@.powa_user_functions_history WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_user_functions_history_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_user_functions_history_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_user_functions_history_current(' || _srvid || ')');
    DELETE FROM @extschema@.powa_user_functions_history_current WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_user_functions_history_current_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_user_functions_history_current_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_user_functions_src_tmp(' || _srvid || ')');
    DELETE FROM @extschema@.powa_user_functions_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_user_functions_reset */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_indexes_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM @extschema@.powa_log('Resetting powa_all_indexes_history(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_indexes_history WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_all_indexes_history_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_indexes_history_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_all_indexes_history_current(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_indexes_history_current WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_all_indexes_history_current_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_indexes_history_current_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_all_indexes_src_tmp(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_indexes_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$
SET search_path = pg_catalog; /* end of powa_all_indexes_reset */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_tables_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM @extschema@.powa_log('Resetting powa_all_tables_history(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_tables_history WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_all_tables_history_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_tables_history_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_all_tables_history_current(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_tables_history_current WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_all_tables_history_current_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_tables_history_current_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_all_tables_src_tmp(' || _srvid || ')');
    DELETE FROM @extschema@.powa_all_tables_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$
SET search_path = pg_catalog; /* end of powa_all_tables_reset */

CREATE OR REPLACE FUNCTION @extschema@.powa_catalog_database_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM @extschema@.powa_log('Resetting powa_catalog_databases(' || _srvid || ')');
    DELETE FROM @extschema@.powa_catalog_databases WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_catalog_database_src_tmp(' || _srvid || ')');
    DELETE FROM @extschema@.powa_catalog_database_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$
SET search_path = pg_catalog; /* end of powa_catalog_database_reset */

CREATE OR REPLACE FUNCTION @extschema@.powa_catalog_role_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM @extschema@.powa_log('Resetting powa_catalog_roles(' || _srvid || ')');
    DELETE FROM @extschema@.powa_catalog_roles WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_catalog_role_src_tmp(' || _srvid || ')');
    DELETE FROM @extschema@.powa_catalog_role_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_catalog_role_reset */

/* pg_stat_kcache integration - part 2 */

CREATE OR REPLACE FUNCTION @extschema@.powa_kcache_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT queryid bigint, OUT top bool, OUT userid oid, OUT dbid oid,
    OUT plan_reads bigint, OUT plan_writes bigint,
    OUT plan_user_time double precision, OUT plan_system_time double precision,
    OUT plan_minflts bigint, OUT plan_majflts bigint,
    OUT plan_nswaps bigint,
    OUT plan_msgsnds bigint, OUT plan_msgrcvs bigint,
    OUT plan_nsignals bigint,
    OUT plan_nvcsws bigint, OUT plan_nivcsws bigint,
    OUT exec_reads bigint, OUT exec_writes bigint,
    OUT exec_user_time double precision, OUT exec_system_time double precision,
    OUT exec_minflts bigint, OUT exec_majflts bigint,
    OUT exec_nswaps bigint,
    OUT exec_msgsnds bigint, OUT exec_msgrcvs bigint,
    OUT exec_nsignals bigint,
    OUT exec_nvcsws bigint, OUT exec_nivcsws bigint
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
  is_v2_2 bool;
  v_nsp text;
BEGIN
    IF (_srvid = 0) THEN
        SELECT (
            (regexp_split_to_array(extversion, E'\\.')::int[])[1] >= 2 AND
            (regexp_split_to_array(extversion, E'\\.')::int[])[2] >= 2
        ), nspname INTO is_v2_2, v_nsp
          FROM pg_catalog.pg_extension e
          JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
          WHERE extname = 'pg_stat_kcache';

        IF (is_v2_2 IS NOT DISTINCT FROM 'true'::bool) THEN
            RETURN QUERY EXECUTE format($$SELECT now(),
                k.queryid, k.top, k.userid, k.dbid,
                k.plan_reads, k.plan_writes,
                k.plan_user_time, k.plan_system_time,
                k.plan_minflts, k.plan_majflts, k.plan_nswaps,
                k.plan_msgsnds, k.plan_msgrcvs, k.plan_nsignals,
                k.plan_nvcsws, k.plan_nivcsws,
                k.exec_reads, k.exec_writes,
                k.exec_user_time, k.exec_system_time,
                k.exec_minflts, k.exec_majflts, k.exec_nswaps,
                k.exec_msgsnds, k.exec_msgrcvs, k.exec_nsignals,
                k.exec_nvcsws, k.exec_nivcsws
            FROM %I.pg_stat_kcache() k
            JOIN pg_catalog.pg_roles r ON r.oid = k.userid
            WHERE NOT (r.rolname = ANY (string_to_array(
                        @extschema@.powa_get_guc('powa.ignored_users', ''),
                        ',')))
            AND k.dbid NOT IN (
                SELECT oid FROM @extschema@.powa_databases
                WHERE dropped IS NOT NULL)
            $$, v_nsp);
        ELSE
            RETURN QUERY EXECUTE format($$SELECT now(),
                k.queryid, 'true'::bool as top, k.userid, k.dbid,
                NULL::bigint AS plan_reads, NULL::bigint AS plan_writes,
                NULL::double precision AS plan_user_time,
                NULL::double precision AS plan_system_time,
                NULL::bigint AS plan_minflts, NULL::bigint AS plan_majflts,
                NULL::bigint AS plan_nswaps,
                NULL::bigint AS plan_msgsnds, NULL::bigint AS plan_msgrcvs,
                NULL::bigint AS plan_nsignals,
                NULL::bigint AS plan_nvcsws, NULL::bigint AS plan_nivcsws,
                k.reads AS exec_reads, k.writes AS exec_writes,
                k.user_time AS exec_user_time, k.system_time AS exec_system_time,
                k.minflts AS exec_minflts, k.majflts AS exec_majflts,
                k.nswaps AS exec_nswaps,
                k.msgsnds AS exec_msgsnds, k.msgrcvs AS exec_msgrcvs,
                k.nsignals AS exec_nsignals,
                k.nvcsws AS exec_nvcsws, k.nivcsws AS exec_nivcsws
            FROM %I.pg_stat_kcache() k
            JOIN pg_catalog.pg_roles r ON r.oid = k.userid
            WHERE NOT (r.rolname = ANY (string_to_array(
                        @extschema@.powa_get_guc('powa.ignored_users', ''),
                        ',')))
            AND k.dbid NOT IN (
                SELECT oid FROM @extschema@.powa_databases
                WHERE dropped IS NOT NULL)
            $$, v_nsp);
        END IF;
    ELSE
        RETURN QUERY SELECT k.ts,
            k.queryid, k.top, k.userid, k.dbid,
            k.plan_reads, k.plan_writes,
            k.plan_user_time, k.plan_system_time,
            k.plan_minflts, k.plan_majflts, k.plan_nswaps,
            k.plan_msgsnds, k.plan_msgrcvs, k.plan_nsignals,
            k.plan_nvcsws, k.plan_nivcsws,
            k.exec_reads, k.exec_writes,
            k.exec_user_time, k.exec_system_time,
            k.exec_minflts, k.exec_majflts, k.exec_nswaps,
            k.exec_msgsnds, k.exec_msgrcvs, k.exec_nsignals,
            k.exec_nvcsws, k.exec_nivcsws
        FROM @extschema@.powa_kcache_src_tmp k
        WHERE k.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_kcache_src */

/*
 * powa_kcache snapshot collection.
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_kcache_snapshot(_srvid integer) RETURNS void as $PROC$
DECLARE
  result bool;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_kcache_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    WITH capture AS (
        SELECT *
        FROM @extschema@.powa_kcache_src(_srvid)
    ),

    by_query AS (
        INSERT INTO @extschema@.powa_kcache_metrics_current (srvid, queryid, top, dbid, userid, metrics)
            SELECT _srvid, queryid, top, dbid, userid,
              (ts,
               plan_reads, plan_writes, plan_user_time, plan_system_time,
               plan_minflts, plan_majflts, plan_nswaps,
               plan_msgsnds, plan_msgrcvs, plan_nsignals,
               plan_nvcsws, plan_nivcsws,
               exec_reads, exec_writes, exec_user_time, exec_system_time,
               exec_minflts, exec_majflts, exec_nswaps,
               exec_msgsnds, exec_msgrcvs, exec_nsignals,
               exec_nvcsws, exec_nivcsws
        )::@extschema@.powa_kcache_history_record
            FROM capture
    ),

    by_database AS (
        INSERT INTO @extschema@.powa_kcache_metrics_current_db (srvid, top, dbid, metrics)
            SELECT _srvid AS srvid, top, dbid,
              (ts,
               sum(plan_reads), sum(plan_writes),
               sum(plan_user_time), sum(plan_system_time),
               sum(plan_minflts), sum(plan_majflts), sum(plan_nswaps),
               sum(plan_msgsnds), sum(plan_msgrcvs), sum(plan_nsignals),
               sum(plan_nvcsws), sum(plan_nivcsws),
               sum(exec_reads), sum(exec_writes),
               sum(exec_user_time), sum(exec_system_time),
               sum(exec_minflts), sum(exec_majflts), sum(exec_nswaps),
               sum(exec_msgsnds), sum(exec_msgrcvs), sum(exec_nsignals),
               sum(exec_nvcsws), sum(exec_nivcsws)
              )::@extschema@.powa_kcache_history_record
            FROM capture
            GROUP BY ts, srvid, top, dbid
    )

    SELECT COUNT(*) into v_rowcount
    FROM capture;

    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_kcache_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_kcache_snapshot */

/*
 * powa_kcache aggregation
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_kcache_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    result     bool;
    v_funcname text := format('@extschema@.%I(%s)',
                              'powa_kcache_aggregate', _srvid);
    v_rowcount bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate metrics table
    INSERT INTO @extschema@.powa_kcache_metrics (coalesce_range, srvid, queryid,
                                            top, dbid, userid, metrics,
                                            mins_in_range, maxs_in_range)
        SELECT tstzrange(min((metrics).ts), max((metrics).ts),'[]'),
        srvid, queryid, top, dbid, userid, array_agg(metrics),
        ROW(min((metrics).ts),
            min((metrics).plan_reads), min((metrics).plan_writes),
            min((metrics).plan_user_time), min((metrics).plan_system_time),
            min((metrics).plan_minflts), min((metrics).plan_majflts),
            min((metrics).plan_nswaps),
            min((metrics).plan_msgsnds), min((metrics).plan_msgrcvs),
            min((metrics).plan_nsignals),
            min((metrics).plan_nvcsws), min((metrics).plan_nivcsws),
            min((metrics).exec_reads), min((metrics).exec_writes),
            min((metrics).exec_user_time), min((metrics).exec_system_time),
            min((metrics).exec_minflts), min((metrics).exec_majflts),
            min((metrics).exec_nswaps),
            min((metrics).exec_msgsnds), min((metrics).exec_msgrcvs),
            min((metrics).exec_nsignals),
            min((metrics).exec_nvcsws), min((metrics).exec_nivcsws)
        )::@extschema@.powa_kcache_history_record,
        ROW(max((metrics).ts),
            max((metrics).plan_reads), max((metrics).plan_writes),
            max((metrics).plan_user_time), max((metrics).plan_system_time),
            max((metrics).plan_minflts), max((metrics).plan_majflts),
            max((metrics).plan_nswaps),
            max((metrics).plan_msgsnds), max((metrics).plan_msgrcvs),
            max((metrics).plan_nsignals),
            max((metrics).plan_nvcsws), max((metrics).plan_nivcsws),
            max((metrics).exec_reads), max((metrics).exec_writes),
            max((metrics).exec_user_time), max((metrics).exec_system_time),
            max((metrics).exec_minflts), max((metrics).exec_majflts),
            max((metrics).exec_nswaps),
            max((metrics).exec_msgsnds), max((metrics).exec_msgrcvs),
            max((metrics).exec_nsignals),
            max((metrics).exec_nvcsws), max((metrics).exec_nivcsws)
        )::@extschema@.powa_kcache_history_record
        FROM @extschema@.powa_kcache_metrics_current
        WHERE srvid = _srvid
        GROUP BY srvid, queryid, top, dbid, userid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_kcache_metrics) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_kcache_metrics_current WHERE srvid = _srvid;

    -- aggregate metrics_db table
    INSERT INTO @extschema@.powa_kcache_metrics_db (srvid, coalesce_range, dbid,
                                               top, metrics,
                                               mins_in_range, maxs_in_range)
        SELECT srvid, tstzrange(min((metrics).ts), max((metrics).ts),'[]'),
        dbid, top, array_agg(metrics),
        ROW(min((metrics).ts),
            min((metrics).plan_reads), min((metrics).plan_writes),
            min((metrics).plan_user_time), min((metrics).plan_system_time),
            min((metrics).plan_minflts), min((metrics).plan_majflts),
            min((metrics).plan_nswaps),
            min((metrics).plan_msgsnds), min((metrics).plan_msgrcvs),
            min((metrics).plan_nsignals),
            min((metrics).plan_nvcsws), min((metrics).plan_nivcsws),
            min((metrics).exec_reads), min((metrics).exec_writes),
            min((metrics).exec_user_time), min((metrics).exec_system_time),
            min((metrics).exec_minflts), min((metrics).exec_majflts),
            min((metrics).exec_nswaps),
            min((metrics).exec_msgsnds), min((metrics).exec_msgrcvs),
            min((metrics).exec_nsignals),
            min((metrics).exec_nvcsws), min((metrics).exec_nivcsws)
        )::@extschema@.powa_kcache_history_record,
        ROW(max((metrics).ts),
            max((metrics).plan_reads), max((metrics).plan_writes),
            max((metrics).plan_user_time), max((metrics).plan_system_time),
            max((metrics).plan_minflts), max((metrics).plan_majflts),
            max((metrics).plan_nswaps),
            max((metrics).plan_msgsnds), max((metrics).plan_msgrcvs),
            max((metrics).plan_nsignals),
            max((metrics).plan_nvcsws), max((metrics).plan_nivcsws),
            max((metrics).exec_reads), max((metrics).exec_writes),
            max((metrics).exec_user_time), max((metrics).exec_system_time),
            max((metrics).exec_minflts), max((metrics).exec_majflts),
            max((metrics).exec_nswaps),
            max((metrics).exec_msgsnds), max((metrics).exec_msgrcvs),
            max((metrics).exec_nsignals),
            max((metrics).exec_nvcsws), max((metrics).exec_nivcsws)
        )::@extschema@.powa_kcache_history_record
        FROM @extschema@.powa_kcache_metrics_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, top;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_kcache_metrics_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_kcache_metrics_current_db WHERE srvid = _srvid;
END
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_kcache_aggregate */

/*
 * powa_kcache purge
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_kcache_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_kcache_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_kcache_metrics
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_kcache_metrics) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_kcache_metrics_db
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_kcache_metrics_db) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ language plpgsql; /* end of powa_kcache_purge */

/*
 * powa_kcache reset
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_kcache_reset(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_kcache_reset', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_log('resetting @extschema@.powa_kcache_metrics(' || _srvid || ')');
    DELETE FROM @extschema@.powa_kcache_metrics WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('resetting @extschema@.powa_kcache_metrics_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_kcache_metrics_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('resetting @extschema@.powa_kcache_metrics_current(' || _srvid || ')');
    DELETE FROM @extschema@.powa_kcache_metrics_current WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('resetting @extschema@.powa_kcache_metrics_current_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_kcache_metrics_current_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('resetting @extschema@.powa_kcache_src_tmp(' || _srvid || ')');
    DELETE FROM @extschema@.powa_kcache_src_tmp WHERE srvid = _srvid;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_kcache_reset */

/* end of pg_stat_kcache integration - part 2 */

/* pg_qualstats integration - part 2 */

/*
 * powa_qualstats utility SRF for aggregating constvalues
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_aggregate_constvalues_current(
    IN _srvid integer,
    IN _ts_from timestamptz DEFAULT '-infinity'::timestamptz,
    IN _ts_to timestamptz DEFAULT 'infinity'::timestamptz,
    OUT srvid integer,
    OUT qualid bigint,
    OUT queryid bigint,
    OUT dbid oid,
    OUT userid oid,
    OUT tstzrange tstzrange,
    OUT mu @extschema@.qual_values[],
    OUT mf @extschema@.qual_values[],
    OUT lf @extschema@.qual_values[],
    OUT me @extschema@.qual_values[],
    OUT mer @extschema@.qual_values[],
    OUT men @extschema@.qual_values[])
RETURNS SETOF record STABLE AS $_$
SELECT
    -- Ordered aggregate of top 20 metrics for each kind of stats (most executed, most filetered, least filtered...)
    srvid, qualid, queryid, dbid, userid,
    tstzrange(min(min_constvalues_ts) , max(max_constvalues_ts) ,'[]') ,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::@extschema@.qual_values ORDER BY occurences_rank ASC) FILTER (WHERE occurences_rank <=20)  mu,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::@extschema@.qual_values ORDER BY filtered_rank ASC) FILTER (WHERE filtered_rank <=20)  mf,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::@extschema@.qual_values ORDER BY filtered_rank DESC) FILTER (WHERE filtered_rank >= nb_lines - 20)  lf, -- Keep last 20 lines from the same window function
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::@extschema@.qual_values ORDER BY execution_rank ASC) FILTER (WHERE execution_rank <=20)  me,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::@extschema@.qual_values ORDER BY err_estimate_ratio_rank ASC) FILTER (WHERE err_estimate_ratio_rank <=20)  mer,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::@extschema@.qual_values ORDER BY err_estimate_num_rank ASC) FILTER (WHERE err_estimate_num_rank <=20)  men
FROM (
    -- Establish rank for different stats (occurences, execution...) of each constvalues
    SELECT srvid, qualid, queryid, dbid, userid,
        min(mints) OVER (W) min_constvalues_ts, max(maxts) OVER (W) max_constvalues_ts,
        constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num,
        row_number() OVER (W ORDER BY sum_occurences DESC) occurences_rank,
        row_number() OVER (W ORDER BY CASE WHEN sum_execution_count = 0 THEN 0 ELSE sum_nbfiltered / sum_execution_count::numeric END DESC) filtered_rank,
        row_number() OVER (W ORDER BY sum_execution_count DESC) execution_rank,
        row_number() OVER (W ORDER BY avg_mean_err_estimate_ratio DESC) err_estimate_ratio_rank,
        row_number() OVER (W ORDER BY avg_mean_err_estimate_num DESC) err_estimate_num_rank,
        sum(1) OVER (W) nb_lines

    FROM (
        -- We group by constvalues and perform some aggregate to have stats on distinct constvalues
        SELECT srvid, qualid, queryid, dbid, userid,constvalues,
            min(ts) mints, max(ts) maxts ,
            sum(occurences) as sum_occurences,
            sum(nbfiltered) as sum_nbfiltered,
            sum(execution_count) as sum_execution_count,
            avg(mean_err_estimate_ratio) as avg_mean_err_estimate_ratio,
            avg(mean_err_estimate_num) as avg_mean_err_estimate_num
        FROM @extschema@.powa_qualstats_constvalues_history_current
        WHERE srvid = _srvid
          AND ts >= _ts_from AND ts <= _ts_to
        GROUP BY srvid, qualid, queryid, dbid, userid,constvalues
        ) distinct_constvalues
    WINDOW W AS (PARTITION BY srvid, qualid, queryid, dbid, userid)
    ) ranked_constvalues
GROUP BY srvid, qualid, queryid, dbid, userid
;
$_$ LANGUAGE sql
SET search_path = pg_catalog; /* end of powa_qualstats_aggregate_constvalues_current */

CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT uniquequalnodeid bigint,
    OUT dbid oid,
    OUT userid oid,
    OUT qualnodeid bigint,
    OUT occurences bigint,
    OUT execution_count bigint,
    OUT nbfiltered bigint,
    OUT mean_err_estimate_ratio double precision,
    OUT mean_err_estimate_num double precision,
    OUT queryid bigint,
    OUT constvalues varchar[],
    OUT quals @extschema@.qual_type[]
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
  is_v2 bool;
  v_pgqs text;
  v_pgss text;
  ratio_col text := 'qs.mean_err_estimate_ratio';
  num_col text := 'qs.mean_err_estimate_num';
  sql text;
BEGIN
    IF (_srvid = 0) THEN
        SELECT substr(extversion, 1, 1)::int >= 2, nspname INTO is_v2, v_pgqs
          FROM pg_catalog.pg_extension e
          JOIN pg_namespace n ON n.oid = e.extnamespace
          WHERE extname = 'pg_qualstats';

        SELECT nspname INTO v_pgss
          FROM pg_catalog.pg_extension e
          JOIN pg_namespace n ON n.oid = e.extnamespace
          WHERE extname = 'pg_stat_statements';

        IF is_v2 IS DISTINCT FROM 'true'::bool THEN
            ratio_col := 'NULL::double precision';
            num_col := 'NULL::double precision';
        END IF;

        sql := format($sql$
            SELECT now(), pgqs.uniquequalnodeid, pgqs.dbid, pgqs.userid,
                pgqs.qualnodeid, pgqs.occurences, pgqs.execution_count,
                pgqs.nbfiltered, pgqs.mean_err_estimate_ratio,
                pgqs.mean_err_estimate_num, pgqs.queryid, pgqs.constvalues,
                pgqs.quals
            FROM (
                SELECT coalesce(i.uniquequalid, i.uniquequalnodeid) AS uniquequalnodeid,
                    i.dbid, i.userid,  coalesce(i.qualid, i.qualnodeid) AS qualnodeid,
                    i.occurences, i.execution_count, i.nbfiltered,
                    i.mean_err_estimate_ratio, i.mean_err_estimate_num,
                    i.queryid,
                    array_agg(i.constvalue order by i.constant_position) AS constvalues,
                    array_agg(ROW(i.relid, i.attnum, i.opno, i.eval_type)::@extschema@.qual_type) AS quals
                FROM
                (
                    SELECT qs.dbid,
                    CASE WHEN lrelid IS NOT NULL THEN lrelid
                        WHEN rrelid IS NOT NULL THEN rrelid
                    END as relid,
                    qs.userid as userid,
                    CASE WHEN lrelid IS NOT NULL THEN lattnum
                        WHEN rrelid IS NOT NULL THEN rattnum
                    END as attnum,
                    qs.opno as opno,
                    qs.qualid as qualid,
                    qs.uniquequalid as uniquequalid,
                    qs.qualnodeid as qualnodeid,
                    qs.uniquequalnodeid as uniquequalnodeid,
                    qs.occurences as occurences,
                    qs.execution_count as execution_count,
                    qs.queryid as queryid,
                    qs.constvalue as constvalue,
                    qs.nbfiltered as nbfiltered,
                    %s AS mean_err_estimate_ratio,
                    %s AS mean_err_estimate_num,
                    qs.eval_type,
                    qs.constant_position
                    FROM %I.pg_qualstats() qs
                    WHERE (qs.lrelid IS NULL) != (qs.rrelid IS NULL)
                ) i
                GROUP BY coalesce(i.uniquequalid, i.uniquequalnodeid),
                    coalesce(i.qualid, i.qualnodeid), i.dbid, i.userid,
                    i.occurences, i.execution_count, i.nbfiltered,
                    i.mean_err_estimate_ratio, i.mean_err_estimate_num,
                    i.queryid
            ) pgqs
            JOIN (
                -- if we use remote capture, powa_statements won't be
                -- populated, so we have to to retrieve the content of both
                -- statements sources.  Since there can (and probably) be
                -- duplicates, we use a UNION on purpose
                SELECT s1.queryid, s1.dbid, s1.userid
                    FROM %I.pg_stat_statements s1
                UNION
                SELECT s2.queryid, s2.dbid, s2.userid
                    FROM @extschema@.powa_statements s2 WHERE s2.srvid = 0
            ) s USING(queryid, dbid, userid)
        -- we don't gather quals for databases that have been dropped
        JOIN pg_catalog.pg_database d ON d.oid = s.dbid
        JOIN pg_catalog.pg_roles r ON s.userid = r.oid
          AND NOT (r.rolname = ANY (string_to_array(
                    @extschema@.powa_get_guc('powa.ignored_users', ''),
                    ',')))
        WHERE pgqs.dbid NOT IN (SELECT oid FROM @extschema@.powa_databases WHERE dropped IS NOT NULL)
        $sql$, ratio_col, num_col, v_pgqs, v_pgss);
        RETURN QUERY EXECUTE sql;
    ELSE
        RETURN QUERY
            SELECT pgqs.ts, pgqs.uniquequalnodeid, pgqs.dbid, pgqs.userid,
                pgqs.qualnodeid, pgqs.occurences, pgqs.execution_count,
                pgqs.nbfiltered, pgqs.mean_err_estimate_ratio,
                pgqs.mean_err_estimate_num, pgqs.queryid, pgqs.constvalues,
                pgqs.quals
            FROM @extschema@.powa_qualstats_src_tmp pgqs
        WHERE pgqs.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_qualstats_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_snapshot(_srvid integer) RETURNS void as $PROC$
DECLARE
    result     bool;
    v_schema   text;
    v_funcname text := format('@extschema.%I(%s)',
                              'powa_qualstats_snapshot', _srvid);
    v_rowcount bigint;
BEGIN
  PERFORM @extschema@.powa_log(format('running %s', v_funcname));

  PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

  WITH capture AS (
    SELECT *
    FROM @extschema@.powa_qualstats_src(_srvid) q
    WHERE EXISTS (SELECT 1
      FROM @extschema@.powa_statements s
      WHERE s.srvid = _srvid
      AND q.queryid = s.queryid
      AND q.dbid = s.dbid
      AND q.userid = s.userid)
  ),
  missing_quals AS (
      INSERT INTO @extschema@.powa_qualstats_quals (srvid, qualid, queryid, dbid, userid, quals)
        SELECT DISTINCT _srvid AS srvid, qs.qualnodeid, qs.queryid, qs.dbid, qs.userid,
          array_agg(DISTINCT q::@extschema@.qual_type)
        FROM capture qs,
        LATERAL (SELECT (unnest(quals)).*) as q
        WHERE NOT EXISTS (
          SELECT 1
          FROM @extschema@.powa_qualstats_quals nh
          WHERE nh.srvid = _srvid
            AND nh.qualid = qs.qualnodeid
            AND nh.queryid = qs.queryid
            AND nh.dbid = qs.dbid
            AND nh.userid = qs.userid
        )
        GROUP BY srvid, qualnodeid, qs.queryid, qs.dbid, qs.userid
      RETURNING *
  ),
  by_qual AS (
      INSERT INTO @extschema@.powa_qualstats_quals_history_current (srvid, qualid, queryid,
        dbid, userid, ts, occurences, execution_count, nbfiltered,
        mean_err_estimate_ratio, mean_err_estimate_num)
      SELECT _srvid AS srvid, qs.qualnodeid, qs.queryid, qs.dbid, qs.userid,
          ts, sum(occurences), sum(execution_count), sum(nbfiltered),
          avg(mean_err_estimate_ratio), avg(mean_err_estimate_num)
        FROM capture as qs
        GROUP BY srvid, ts, qualnodeid, qs.queryid, qs.dbid, qs.userid
      RETURNING *
  ),
  by_qual_with_const AS (
      INSERT INTO @extschema@.powa_qualstats_constvalues_history_current(srvid, qualid,
        queryid, dbid, userid, ts, occurences, execution_count, nbfiltered,
        mean_err_estimate_ratio, mean_err_estimate_num, constvalues)
      SELECT _srvid, qualnodeid, qs.queryid, qs.dbid, qs.userid, ts,
        occurences, execution_count, nbfiltered, mean_err_estimate_ratio,
        mean_err_estimate_num, constvalues
      FROM capture as qs
  )
  SELECT COUNT(*) into v_rowcount
  FROM capture;

  PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
        v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_qualstats_src_tmp WHERE srvid = _srvid;
    END IF;

  result := true;

  -- pg_qualstats metrics are not accumulated, so we force a reset after every
  -- snapshot.  For local snapshot this is done here, remote snapshots will
  -- rely on the collector doing it through query_cleanup.
  IF (_srvid = 0) THEN
    SELECT n.nspname INTO STRICT v_schema
        FROM pg_catalog.pg_extension e
        JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
        AND e.extname = 'pg_qualstats';
    PERFORM format('%I.pg_qualstats_reset()', v_schema);
  END IF;
END
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_qualstats_snapshot */

/*
 * powa_qualstats aggregate
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
  result bool;
BEGIN
  PERFORM @extschema@.powa_log('running powa_qualstats_aggregate(' || _srvid || ')');

  PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

  INSERT INTO @extschema@.powa_qualstats_constvalues_history (
      srvid, qualid, queryid, dbid, userid, coalesce_range, most_used,
      most_filtering, least_filtering, most_executed, most_errestim_ratio,
    most_errestim_num)
    SELECT * FROM @extschema@.powa_qualstats_aggregate_constvalues_current(_srvid)
    WHERE srvid = _srvid;

  INSERT INTO @extschema@.powa_qualstats_quals_history (srvid, qualid, queryid, dbid,
      userid, coalesce_range, records, mins_in_range, maxs_in_range)
    SELECT srvid, qualid, queryid, dbid, userid, tstzrange(min(ts),
      max(ts),'[]'),
      array_agg((ts, occurences, execution_count, nbfiltered,
            mean_err_estimate_ratio,
            mean_err_estimate_num)::@extschema@.powa_qualstats_history_record),
    ROW(min(ts), min(occurences), min(execution_count), min(nbfiltered),
        min(mean_err_estimate_ratio), min(mean_err_estimate_num)
    )::@extschema@.powa_qualstats_history_record,
    ROW(max(ts), max(occurences), max(execution_count), max(nbfiltered),
        max(mean_err_estimate_ratio), max(mean_err_estimate_num)
    )::@extschema@.powa_qualstats_history_record
    FROM @extschema@.powa_qualstats_quals_history_current
    WHERE srvid = _srvid
    GROUP BY srvid, qualid, queryid, dbid, userid;

  DELETE FROM @extschema@.powa_qualstats_constvalues_history_current WHERE srvid = _srvid;
  DELETE FROM @extschema@.powa_qualstats_quals_history_current WHERE srvid = _srvid;
END
$PROC$ language plpgsql
SET search_path = pg_catalog
SET search_path = pg_catalog; /* end of powa_qualstats_aggregate */

/*
 * powa_qualstats_purge
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log('running powa_qualstats_purge(' || _srvid || ')');

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_qualstats_constvalues_history
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    DELETE FROM @extschema@.powa_qualstats_quals_history
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_qualstats_purge */

/*
 * powa_qualstats_reset
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_reset(_srvid integer)
RETURNS void as $PROC$
BEGIN
  PERFORM @extschema@.powa_log('running powa_qualstats_reset(' || _srvid || ')');

  PERFORM @extschema@.powa_log('resetting powa_qualstats_quals(' || _srvid || ')');
  DELETE FROM @extschema@.powa_qualstats_quals WHERE srvid = _srvid;
  -- cascaded :
  -- powa_qualstats_quals_history
  -- powa_qualstats_quals_history_current
  -- powa_qualstats_constvalues_history
  -- powa_qualstats_constvalues_history_current

  PERFORM @extschema@.powa_log('resetting powa_qualstats_src_tmp(' || _srvid || ')');
  DELETE FROM @extschema@.powa_qualstats_src_tmp WHERE srvid = _srvid;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_qualstats_reset */

/* end of pg_qualstats integration - part 2 */

-- nothing to do

/* end pg_track_settings integration */

/* pg_wait_sampling integration - part 2 */

CREATE OR REPLACE FUNCTION @extschema@.powa_wait_sampling_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT dbid oid,
    OUT event_type text,
    OUT event text,
    OUT queryid bigint,
    OUT count numeric
) RETURNS SETOF RECORD STABLE AS $PROC$
DECLARE
  v_pgws text;
  v_pgss text;
BEGIN
    IF (_srvid = 0) THEN
        SELECT nspname INTO STRICT v_pgws
        FROM pg_catalog.pg_extension e
        JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'pg_wait_sampling';

        SELECT nspname INTO STRICT v_pgss
        FROM pg_catalog.pg_extension e
        JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'pg_stat_statements';

        RETURN QUERY EXECUTE format($$
            -- the various background processes report wait events but don't have
            -- associated queryid.  Gather them all under a fake 0 dbid
            SELECT now(), COALESCE(pgss.dbid, 0) AS dbid, s.event_type,
                s.event, s.queryid, sum(s.count) as count
            FROM %I.pg_wait_sampling_profile s
            -- pg_wait_sampling doesn't offer a per (userid, dbid, queryid) view,
            -- only per pid, but pid can be reused for different databases or users
            -- so we cannot deduce db or user from it.  However, queryid should be
            -- unique across differet databases, so we retrieve the dbid this way.
            -- Note that the same queryid can exists for multiple entries if
            -- multiple users execute the query, so it's critical to retrieve a
            -- single row from pg_stat_statements per (dbid, queryid)
            LEFT JOIN (SELECT DISTINCT s2.dbid, s2.queryid
                FROM %I.pg_stat_statements(false) s2
            ) pgss ON pgss.queryid = s.queryid
            WHERE s.event_type IS NOT NULL AND s.event IS NOT NULL
            AND COALESCE(pgss.dbid, 0) NOT IN (
                SELECT oid FROM @extschema@.powa_databases
                WHERE dropped IS NOT NULL
            )
            GROUP BY pgss.dbid, s.event_type, s.event, s.queryid
          $$, v_pgws, v_pgss);
    ELSE
        RETURN QUERY
        SELECT s.ts, s.dbid, s.event_type, s.event, s.queryid, s.count
        FROM @extschema@.powa_wait_sampling_src_tmp s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_wait_sampling_src */

/*
 * powa_wait_sampling snapshot collection.
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_wait_sampling_snapshot(_srvid integer) RETURNS void as $PROC$
DECLARE
  result bool;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_wait_sampling_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    WITH capture AS (
        SELECT *
        FROM @extschema@.powa_wait_sampling_src(_srvid)
    ),

    by_query AS (
        INSERT INTO @extschema@.powa_wait_sampling_history_current (srvid, queryid, dbid,
                event_type, event, record)
            SELECT _srvid, queryid, dbid, event_type, event,
                (ts, count)::@extschema@.powa_wait_sampling_history_record
            FROM capture
    ),

    by_database AS (
        INSERT INTO @extschema@.powa_wait_sampling_history_current_db (srvid, dbid,
                event_type, event, record)
            SELECT _srvid AS srvid, dbid, event_type, event,
                (ts, sum(count))::@extschema@.powa_wait_sampling_history_record
            FROM capture
            GROUP BY srvid, ts, dbid, event_type, event
    )

    SELECT COUNT(*) into v_rowcount
    FROM capture;

    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_wait_sampling_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_wait_sampling_snapshot */

/*
 * powa_wait_sampling aggregation
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_wait_sampling_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    result     bool;
    v_funcname text := format('@extschema@.%I(%s)',
                              'powa_wait_sampling_aggregate', _srvid);
    v_rowcount bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate history table
    INSERT INTO @extschema@.powa_wait_sampling_history (coalesce_range, srvid, queryid,
            dbid, event_type, event, records, mins_in_range, maxs_in_range)
        SELECT tstzrange(min((record).ts), max((record).ts),'[]'),
            srvid, queryid, dbid, event_type, event, array_agg(record),
        ROW(min((record).ts),
            min((record).count))::@extschema@.powa_wait_sampling_history_record,
        ROW(max((record).ts),
            max((record).count))::@extschema@.powa_wait_sampling_history_record
        FROM @extschema@.powa_wait_sampling_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, queryid, dbid, event_type, event;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_wait_sampling_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_wait_sampling_history_current WHERE srvid = _srvid;

    -- aggregate history_db table
    INSERT INTO @extschema@.powa_wait_sampling_history_db (coalesce_range, srvid, dbid,
            event_type, event, records, mins_in_range, maxs_in_range)
        SELECT tstzrange(min((record).ts), max((record).ts),'[]'), srvid, dbid,
            event_type, event, array_agg(record),
        ROW(min((record).ts),
            min((record).count))::@extschema@.powa_wait_sampling_history_record,
        ROW(max((record).ts),
            max((record).count))::@extschema@.powa_wait_sampling_history_record
        FROM @extschema@.powa_wait_sampling_history_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, event_type, event;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_wait_sampling_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_wait_sampling_history_current_db WHERE srvid = _srvid;
END
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_wait_sampling_aggregate */

/*
 * powa_wait_sampling purge
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_wait_sampling_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := format('@extschema.%I(%s)',
                                 'powa_wait_sampling_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_wait_sampling_history
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_wait_sampling_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_wait_sampling_history_db
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_wait_sampling_history_db) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_wait_sampling_purge */

/*
 * powa_wait_sampling reset
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_wait_sampling_reset(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_wait_sampling_reset', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_log('resetting powa_wait_sampling_history(' || _srvid || ')');
    DELETE FROM @extschema@.powa_wait_sampling_history WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('resetting powa_wait_sampling_history_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_wait_sampling_history_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('resetting powa_wait_sampling_history_current(' || _srvid || ')');
    DELETE FROM @extschema@.powa_wait_sampling_history_current WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('resetting powa_wait_sampling_history_current_db(' || _srvid || ')');
    DELETE FROM @extschema@.powa_wait_sampling_history_current_db WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('resetting powa_wait_sampling_src_tmp(' || _srvid || ')');
    DELETE FROM @extschema@.powa_wait_sampling_src_tmp WHERE srvid = _srvid;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_wait_sampling_reset */

/* end of pg_wait_sampling integration - part 2 */

---------------
-- ACL handling
---------------

/*
 * powa_grant() will grant the appropriate ACL to the various powa_* pseudo
 * predefined roles.
 *
 * We try to avoid relying on external extensions like hstore to not have to
 * handle the possibility of it installed in some custom and not visible
 * schema, so the code is a bit more verbose than needed.
 */
CREATE FUNCTION @extschema@.powa_grant() RETURNS void
AS $$
DECLARE
    relname name;
    relkind char;
    powa_role name;
    rolname name;
    admin_role name;
    read_all_data_role name;
    read_all_metrics_role name;
    write_all_data_role name;
    snapshot_role name;
    signal_backend_role name;
    v_nb integer;
BEGIN
    FOR powa_role, rolname IN SELECT pr.powa_role, pr.rolname
                              FROM @extschema@.powa_roles pr
    LOOP
        IF rolname IS NULL THEN
            RAISE EXCEPTION 'powa_role % is NULL', powa_role;
        END IF;

        IF powa_role = 'powa_admin' THEN
            admin_role = rolname;
        ELSIF powa_role = 'powa_read_all_data' THEN
            read_all_data_role = rolname;
        ELSIF powa_role = 'powa_read_all_metrics' THEN
            read_all_metrics_role = rolname;
        ELSIF powa_role = 'powa_write_all_data' THEN
            write_all_data_role = rolname;
        ELSIF powa_role = 'powa_snapshot' THEN
            snapshot_role = rolname;
        ELSIF powa_role = 'powa_signal_backend' THEN
            signal_backend_role = rolname;
        ELSE
            RAISE EXCEPTION 'Unexpected powa_role %', powa_role;
        END IF;
    END LOOP;

    FOR relname, relkind IN
        SELECT c.relname, c.relkind
        FROM pg_depend d
        JOIN pg_extension e ON d.refclassid = 'pg_extension'::regclass
            AND e.oid = d.refobjid
            AND e.extname = 'powa'
        JOIN pg_class c ON d.classid = 'pg_class'::regclass
            AND c.oid = d.objid
    LOOP
        EXECUTE format('GRANT ALL ON @extschema@.%I TO %I',
                       relname, admin_role);

        IF relkind = 'S' THEN
            EXECUTE format('GRANT USAGE, SELECT, UPDATE ON @extschema@.%I TO %I',
                           relname, write_all_data_role);
        ELSE
            EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE '
                           'ON @extschema@.%I TO %I',
                           relname, write_all_data_role);
            EXECUTE format('REVOKE REFERENCES, TRIGGER ON @extschema@.%I FROM %I',
                           relname, write_all_data_role);
            EXECUTE format('REVOKE REFERENCES, TRIGGER ON @extschema@.%I FROM %I',
                           relname, snapshot_role);
            -- powa_snapshot can only write to snapshot-related data
            IF relname IN ('powa_roles', 'powa_servers', 'powa_extensions',
                           'powa_extension_functions', 'powa_extension_config',
                            'powa_modules', 'powa_module_config',
                            'powa_module_functions', 'powa_db_modules',
                            'powa_db_module_config',
                            'powa_db_module_functions',
                            'powa_db_module_src_queries', 'powa_catalogs',
                            'powa_catalog_src_queries')
                OR relkind = 'v'
            THEN
                EXECUTE format('GRANT SELECT '
                               'ON @extschema@.%I TO %I',
                               relname, snapshot_role);
            ELSE
                EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE '
                               'ON @extschema@.%I TO %I',
                               relname, snapshot_role);
            END IF;
        END IF;

        EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                       relname, signal_backend_role);

        -- powa_read_all_data only has SELECT privilege on non *_src_tmp tables
        --
        -- powa_read_all_metrics on has SELECT privileges on non *_src_tmp
        -- tables and non pg_qualstats constvalues related tables
        IF relname LIKE '%\_src\_tmp' THEN
            EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                           relname, read_all_data_role);
            EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                           relname, read_all_metrics_role);
        ELSIF relname LIKE '%qualstats\_constvalues%' THEN
            EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                           relname, read_all_metrics_role);
            EXECUTE format('GRANT SELECT ON @extschema@.%I TO %I',
                           relname, read_all_data_role);
        ELSE
            IF relkind = 'S' THEN
                EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                               relname, read_all_data_role);
                EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                               relname, read_all_metrics_role);
            ELSE
                EXECUTE format('GRANT SELECT ON @extschema@.%I TO %I',
                               relname, read_all_data_role);
                EXECUTE format('GRANT SELECT ON @extschema@.%I TO %I',
                               relname, read_all_metrics_role);
                EXECUTE format('REVOKE INSERT, UPDATE, DELETE, TRUNCATE, '
                               'REFERENCES, TRIGGER ON @extschema@.%I FROM %I',
                               relname, read_all_data_role);
                EXECUTE format('REVOKE INSERT, UPDATE, DELETE, TRUNCATE, '
                               'REFERENCES, TRIGGER ON @extschema@.%I FROM %I',
                               relname, read_all_metrics_role);
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_grant() */

/*
 * powa_revoke() will revoke any ACL from the various powa_* pseudo
 * predefined roles.
 *
 * This is mostly intended to help dropping the powa_* pseudo predefine roles.
 *
 * We don't try to revoke any ACL on non-powa relations, as powa_grant() won't
 * try to do that.  If users added some extra ACL they will have to take care
 * of it themselves.
 */
CREATE FUNCTION @extschema@.powa_revoke() RETURNS void
AS $$
DECLARE
    relname name;
    powa_role name;
    rolname name;
    admin_role name;
    read_all_data_role name;
    read_all_metrics_role name;
    write_all_data_role name;
    snapshot_role name;
    signal_backend_role name;
    v_nb integer;
BEGIN
    FOR powa_role, rolname IN SELECT pr.powa_role, pr.rolname
                              FROM @extschema@.powa_roles pr
    LOOP
        IF rolname IS NULL THEN
            RAISE EXCEPTION 'powa_role % is NULL', powa_role;
        END IF;

        IF powa_role = 'powa_admin' THEN
            admin_role = rolname;
        ELSIF powa_role = 'powa_read_all_data' THEN
            read_all_data_role = rolname;
        ELSIF powa_role = 'powa_read_all_metrics' THEN
            read_all_metrics_role = rolname;
        ELSIF powa_role = 'powa_write_all_data' THEN
            write_all_data_role = rolname;
        ELSIF powa_role = 'powa_snapshot' THEN
            snapshot_role = rolname;
        ELSIF powa_role = 'powa_signal_backend' THEN
            signal_backend_role = rolname;
        ELSE
            RAISE EXCEPTION 'Unexpected powa_role %', powa_role;
        END IF;
    END LOOP;

    FOR relname IN
        SELECT c.relname
        FROM pg_depend d
        JOIN pg_extension e ON d.refclassid = 'pg_extension'::regclass
            AND e.oid = d.refobjid
            AND e.extname = 'powa'
        JOIN pg_class c ON d.classid = 'pg_class'::regclass
            AND c.oid = d.objid
    LOOP
        EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                       relname, admin_role);
        EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                       relname, read_all_data_role);
        EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                       relname, read_all_metrics_role);
        EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                       relname, write_all_data_role);
        EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                       relname, snapshot_role);
        EXECUTE format('REVOKE ALL ON @extschema@.%I FROM %I',
                       relname, signal_backend_role);
    END LOOP;
END;
$$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_revoke() */

-- mass set proper ACL IIF none of the default pseudo predefined roles exist
DO
$$
DECLARE
    v_nb int;
BEGIN
    SELECT count(*) INTO v_nb
    FROM @extschema@.powa_roles p
    JOIN pg_catalog.pg_roles c ON c.rolname = p.powa_role;

    IF v_nb = 0 THEN
        RAISE NOTICE 'Creating default powa pseudo predefined roles';
        PERFORM @extschema@.setup_powa_roles();
    ELSE
        RAISE NOTICE 'Skipping default powa pseudo predefined roles';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Finally, activate any supported extension that's already locally installed.
SELECT @extschema@.powa_activate_extension(0, extname)
FROM @extschema@.powa_extensions p
JOIN pg_catalog.pg_extension e USING (extname);
