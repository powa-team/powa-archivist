-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION powa" to load this file. \quit

SET LOCAL statement_timeout = 0;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL standard_conforming_strings = on;
SET LOCAL client_min_messages = warning;
SET LOCAL search_path = pg_catalog;

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
    module text NOT NULL PRIMARY KEY
);

INSERT INTO @extschema@.powa_modules (module) VALUES
    ('pg_stat_bgwriter');

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
    (0, 'pg_stat_bgwriter', false);

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
    ('pg_stat_bgwriter', 'snapshot',  'powa_stat_bgwriter_snapshot',  'powa_stat_bgwriter_src'),
    ('pg_stat_bgwriter', 'aggregate', 'powa_stat_bgwriter_aggregate', NULL),
    ('pg_stat_bgwriter', 'purge',     'powa_stat_bgwriter_purge',     NULL),
    ('pg_stat_bgwriter', 'reset',     'powa_stat_bgwriter_reset',     NULL);

CREATE VIEW @extschema@.powa_functions AS
    SELECT srvid, 'extension' AS kind, extname AS name, operation, external,
        function_name, query_source, query_cleanup, enabled, priority
    FROM @extschema@.powa_extensions e
    JOIN @extschema@.powa_extension_functions f USING (extname)
    JOIN @extschema@.powa_extension_config c USING (extname)
    UNION ALL
    SELECT srvid, 'module' AS kind, module AS name, operation, false,
        function_name, query_source, NULL, enabled, 100
    FROM @extschema@.powa_module_functions f
    JOIN @extschema@.powa_module_config c USING (module);

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
     'SELECT relid,
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
     'SELECT relid,
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
     'SELECT relid,
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
     'SELECT si.relid, indexrelid, idx_scan, NULL AS last_idx_scan,
        idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit
      FROM pg_catalog.pg_stat_all_indexes si
      JOIN pg_catalog.pg_statio_all_indexes sit USING (indexrelid)'),
    -- pg_stat_all_indexes pg16+
    ('pg_stat_all_indexes', 160000, false,
     'SELECT si.relid, indexrelid, idx_scan, last_idx_scan,
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
               tmp_table text, enabled bool, priority int)
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
$_$ LANGUAGE sql;

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

CREATE TYPE @extschema@.powa_statements_history_record AS (
    ts timestamp with time zone,
    calls bigint,
    total_exec_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_dirtied bigint,
    local_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    plans bigint,
    total_plan_time double precision,
    wal_records bigint,
    wal_fpi bigint,
    wal_bytes numeric
);

/* pg_stat_statements operator support */
CREATE TYPE @extschema@.powa_statements_history_diff AS (
    intvl interval,
    calls bigint,
    total_exec_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_dirtied bigint,
    local_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    plans bigint,
    total_plan_time double precision,
    wal_records bigint,
    wal_fpi bigint,
    wal_bytes numeric
);

CREATE OR REPLACE FUNCTION @extschema@.powa_statements_history_mi(
    a @extschema@.powa_statements_history_record,
    b @extschema@.powa_statements_history_record)
RETURNS @extschema@.powa_statements_history_diff AS
$_$
DECLARE
    res @extschema@.powa_statements_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.calls = a.calls - b.calls;
    res.total_exec_time = a.total_exec_time - b.total_exec_time;
    res.rows = a.rows - b.rows;
    res.shared_blks_hit = a.shared_blks_hit - b.shared_blks_hit;
    res.shared_blks_read = a.shared_blks_read - b.shared_blks_read;
    res.shared_blks_dirtied = a.shared_blks_dirtied - b.shared_blks_dirtied;
    res.shared_blks_written = a.shared_blks_written - b.shared_blks_written;
    res.local_blks_hit = a.local_blks_hit - b.local_blks_hit;
    res.local_blks_read = a.local_blks_read - b.local_blks_read;
    res.local_blks_dirtied = a.local_blks_dirtied - b.local_blks_dirtied;
    res.local_blks_written = a.local_blks_written - b.local_blks_written;
    res.temp_blks_read = a.temp_blks_read - b.temp_blks_read;
    res.temp_blks_written = a.temp_blks_written - b.temp_blks_written;
    res.blk_read_time = a.blk_read_time - b.blk_read_time;
    res.blk_write_time = a.blk_write_time - b.blk_write_time;
    res.plans = a.plans - b.plans;
    res.total_plan_time = a.total_plan_time - b.total_plan_time;
    res.wal_records = a.wal_records - b.wal_records;
    res.wal_fpi = a.wal_fpi - b.wal_fpi;
    res.wal_bytes = a.wal_bytes - b.wal_bytes;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_statements_history_mi,
    LEFTARG = @extschema@.powa_statements_history_record,
    RIGHTARG = @extschema@.powa_statements_history_record
);

CREATE TYPE @extschema@.powa_statements_history_rate AS (
    sec integer,
    calls_per_sec double precision,
    runtime_per_sec double precision,
    rows_per_sec double precision,
    shared_blks_hit_per_sec double precision,
    shared_blks_read_per_sec double precision,
    shared_blks_dirtied_per_sec double precision,
    shared_blks_written_per_sec double precision,
    local_blks_hit_per_sec double precision,
    local_blks_read_per_sec double precision,
    local_blks_dirtied_per_sec double precision,
    local_blks_written_per_sec double precision,
    temp_blks_read_per_sec double precision,
    temp_blks_written_per_sec double precision,
    blk_read_time_per_sec double precision,
    blk_write_time_per_sec double precision,
    plans_per_sec double precision,
    plantime_per_sec double precision,
    wal_records_per_sec double precision,
    wal_fpi_per_sec double precision,
    wal_bytes_per_sec numeric
);

CREATE OR REPLACE FUNCTION @extschema@.powa_statements_history_div(
    a @extschema@.powa_statements_history_record,
    b @extschema@.powa_statements_history_record)
RETURNS @extschema@.powa_statements_history_rate AS
$_$
DECLARE
    res @extschema@.powa_statements_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.calls_per_sec = (a.calls - b.calls)::double precision / sec;
    res.runtime_per_sec = (a.total_exec_time - b.total_exec_time)::double precision / sec;
    res.rows_per_sec = (a.rows - b.rows)::double precision / sec;
    res.shared_blks_hit_per_sec = (a.shared_blks_hit - b.shared_blks_hit)::double precision / sec;
    res.shared_blks_read_per_sec = (a.shared_blks_read - b.shared_blks_read)::double precision / sec;
    res.shared_blks_dirtied_per_sec = (a.shared_blks_dirtied - b.shared_blks_dirtied)::double precision / sec;
    res.shared_blks_written_per_sec = (a.shared_blks_written - b.shared_blks_written)::double precision / sec;
    res.local_blks_hit_per_sec = (a.local_blks_hit - b.local_blks_hit)::double precision / sec;
    res.local_blks_read_per_sec = (a.local_blks_read - b.local_blks_read)::double precision / sec;
    res.local_blks_dirtied_per_sec = (a.local_blks_dirtied - b.local_blks_dirtied)::double precision / sec;
    res.local_blks_written_per_sec = (a.local_blks_written - b.local_blks_written)::double precision / sec;
    res.temp_blks_read_per_sec = (a.temp_blks_read - b.temp_blks_read)::double precision / sec;
    res.temp_blks_written_per_sec = (a.temp_blks_written - b.temp_blks_written)::double precision / sec;
    res.blk_read_time_per_sec = (a.blk_read_time - b.blk_read_time)::double precision / sec;
    res.blk_write_time_per_sec = (a.blk_write_time - b.blk_write_time)::double precision / sec;
    res.plans_per_sec = (a.plans - b.plans)::double precision / sec;
    res.plantime_per_sec = (a.total_plan_time - b.total_plan_time)::double precision / sec;
    res.wal_records_per_sec = (a.wal_records - b.wal_records)::double precision / sec;
    res.wal_fpi_per_sec = (a.wal_fpi - b.wal_fpi)::double precision / sec;
    res.wal_bytes_per_sec = (a.wal_bytes - b.wal_bytes)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_statements_history_div,
    LEFTARG = @extschema@.powa_statements_history_record,
    RIGHTARG = @extschema@.powa_statements_history_record
);
/* end of pg_stat_statements operator support */

CREATE TYPE @extschema@.powa_user_functions_history_record AS (
    ts timestamp with time zone,
    calls bigint,
    total_time double precision,
    self_time double precision
);

/* pg_stat_user_functions operator support */
CREATE TYPE @extschema@.powa_user_functions_history_diff AS (
    intvl interval,
    calls bigint,
    total_time double precision,
    self_time double precision

);

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_history_mi(
    a @extschema@.powa_user_functions_history_record,
    b @extschema@.powa_user_functions_history_record)
RETURNS @extschema@.powa_user_functions_history_diff AS
$_$
DECLARE
    res @extschema@.powa_user_functions_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.calls = a.calls - b.calls;
    res.total_time = a.total_time - b.total_time;
    res.self_time = a.self_time - b.self_time;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_user_functions_history_mi,
    LEFTARG = @extschema@.powa_user_functions_history_record,
    RIGHTARG = @extschema@.powa_user_functions_history_record
);

CREATE TYPE @extschema@.powa_user_functions_history_rate AS (
    sec integer,
    calls_per_sec double precision,
    total_time_per_sec double precision,
    self_time_per_sec double precision
);

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_history_div(
    a @extschema@.powa_user_functions_history_record,
    b @extschema@.powa_user_functions_history_record)
RETURNS @extschema@.powa_user_functions_history_rate AS
$_$
DECLARE
    res @extschema@.powa_user_functions_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.calls_per_sec = (a.calls - b.calls)::double precision / sec;
    res.total_time_per_sec = (a.total_time - b.total_time)::double precision / sec;
    res.self_time_per_sec = (a.self_time - b.self_time)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_user_functions_history_div,
    LEFTARG = @extschema@.powa_user_functions_history_record,
    RIGHTARG = @extschema@.powa_user_functions_history_record
);
/* end of pg_stat_user_functions operator support */

-- It combines info from pg_stat_all_indexes and pg_statio_all_indexes
CREATE TYPE @extschema@.powa_all_indexes_history_record AS (
    ts timestamp with time zone,
    -- pg_stat_all_indexes fields
    idx_scan bigint,
    last_idx_scan timestamp with time zone,
    idx_tup_read bigint,
    idx_tup_fetch bigint,
    -- pg_statio_all_indexes fields
    idx_blks_read bigint,
    idx_blks_hit bigint
);

/* pg_stat_all_indexes operator support */
CREATE TYPE @extschema@.powa_all_indexes_history_diff AS (
    intvl interval,
    idx_scan bigint,
    idx_tup_read bigint,
    idx_tup_fetch bigint,
    idx_blks_read bigint,
    idx_blks_hit bigint
);

CREATE OR REPLACE FUNCTION @extschema@.powa_all_indexes_history_mi(
    a @extschema@.powa_all_indexes_history_record,
    b @extschema@.powa_all_indexes_history_record)
RETURNS @extschema@.powa_all_indexes_history_diff AS
$_$
DECLARE
    res @extschema@.powa_all_indexes_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.idx_scan = a.idx_scan - b.idx_scan;
    res.idx_tup_read = a.idx_tup_read - b.idx_tup_read;
    res.idx_tup_fetch = a.idx_tup_fetch - b.idx_tup_fetch;
    res.idx_blks_read = a.idx_blks_read - b.idx_blks_read;
    res.idx_blks_hit = a.idx_blks_hit - b.idx_blks_hit;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_all_indexes_history_mi,
    LEFTARG = @extschema@.powa_all_indexes_history_record,
    RIGHTARG = @extschema@.powa_all_indexes_history_record
);

CREATE TYPE @extschema@.powa_all_indexes_history_rate AS (
    sec integer,
    idx_scan_per_sec double precision,
    idx_tup_read_per_sec double precision,
    idx_tup_fetch_per_sec double precision,
    idx_blks_read_per_sec double precision,
    idx_blks_hit_per_sec double precision
);

CREATE OR REPLACE FUNCTION @extschema@.powa_all_indexes_history_div(
    a @extschema@.powa_all_indexes_history_record,
    b @extschema@.powa_all_indexes_history_record)
RETURNS @extschema@.powa_all_indexes_history_rate AS
$_$
DECLARE
    res @extschema@.powa_all_indexes_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.idx_scan_per_sec = (a.idx_scan - b.idx_scan)::double precision / sec;
    res.idx_tup_read_per_sec = (a.idx_tup_read - b.idx_tup_read)::double precision / sec;
    res.idx_tup_fetch_per_sec = (a.idx_tup_fetch - b.idx_tup_fetch)::double precision / sec;
    res.idx_blks_read_per_sec = (a.idx_blks_read - b.idx_blks_read)::double precision / sec;
    res.idx_blks_hit_per_sec = (a.idx_blks_hit - b.idx_blks_hit)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_all_indexes_history_div,
    LEFTARG = @extschema@.powa_all_indexes_history_record,
    RIGHTARG = @extschema@.powa_all_indexes_history_record
);
/* end of pg_stat_all_indexes operator support */

-- We need a type different than powa_all_indexes_history_record as we can't
-- aggregate the last last_* timestamps
CREATE TYPE @extschema@.powa_all_indexes_history_db_record AS (
    ts timestamp with time zone,
    -- pg_stat_all_indexes fields
    idx_scan bigint,
    idx_tup_read bigint,
    idx_tup_fetch bigint,
    -- pg_statio_all_indexes fields
    idx_blks_read bigint,
    idx_blks_hit bigint
);

/* pg_stat_all_indexes_db operator support */
CREATE OR REPLACE FUNCTION @extschema@.powa_all_indexes_history_db_mi(
    a @extschema@.powa_all_indexes_history_db_record,
    b @extschema@.powa_all_indexes_history_db_record)
RETURNS @extschema@.powa_all_indexes_history_diff AS
$_$
DECLARE
    res @extschema@.powa_all_indexes_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.idx_scan = a.idx_scan - b.idx_scan;
    res.idx_tup_read = a.idx_tup_read - b.idx_tup_read;
    res.idx_tup_fetch = a.idx_tup_fetch - b.idx_tup_fetch;
    res.idx_blks_read = a.idx_blks_read - b.idx_blks_read;
    res.idx_blks_hit = a.idx_blks_hit - b.idx_blks_hit;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_all_indexes_history_db_mi,
    LEFTARG = @extschema@.powa_all_indexes_history_db_record,
    RIGHTARG = @extschema@.powa_all_indexes_history_db_record
);

CREATE OR REPLACE FUNCTION @extschema@.powa_all_indexes_history_db_div(
    a @extschema@.powa_all_indexes_history_db_record,
    b @extschema@.powa_all_indexes_history_db_record)
RETURNS @extschema@.powa_all_indexes_history_rate AS
$_$
DECLARE
    res @extschema@.powa_all_indexes_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.idx_scan_per_sec = (a.idx_scan - b.idx_scan)::double precision / sec;
    res.idx_tup_read_per_sec = (a.idx_tup_read - b.idx_tup_read)::double precision / sec;
    res.idx_tup_fetch_per_sec = (a.idx_tup_fetch - b.idx_tup_fetch)::double precision / sec;
    res.idx_blks_read_per_sec = (a.idx_blks_read - b.idx_blks_read)::double precision / sec;
    res.idx_blks_hit_per_sec = (a.idx_blks_hit - b.idx_blks_hit)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_all_indexes_history_db_div,
    LEFTARG = @extschema@.powa_all_indexes_history_db_record,
    RIGHTARG = @extschema@.powa_all_indexes_history_db_record
);
/* end of pg_stat_all_indexes_db operator support */

-- It combines info from pg_stat_all_tables and pg_statio_all_tables
CREATE TYPE @extschema@.powa_all_tables_history_record AS (
    ts timestamp with time zone,
    -- pg_stat_all_tables fields
    seq_scan bigint,
    last_seq_scan timestamp with time zone,
    seq_tup_read bigint,
    idx_scan bigint,
    last_idx_scan timestamp with time zone,
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
    -- pg_statio_all_tables fields
    heap_blks_read bigint,
    heap_blks_hit bigint,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    toast_blks_read bigint,
    toast_blks_hit bigint,
    tidx_blks_read bigint,
    tidx_blks_hit bigint
);

/* pg_stat_all_tables operator support */
CREATE TYPE @extschema@.powa_all_tables_history_diff AS (
    intvl interval,
    seq_scan bigint,
    seq_tup_read bigint,
    idx_scan bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    n_tup_newpage_upd bigint,
    n_liv_tup bigint,
    n_dead_tup bigint,
    n_mod_since_analyze bigint,
    n_ins_since_vacuum bigint,
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

CREATE OR REPLACE FUNCTION @extschema@.powa_all_tables_history_mi(
    a @extschema@.powa_all_tables_history_record,
    b @extschema@.powa_all_tables_history_record)
RETURNS @extschema@.powa_all_tables_history_diff AS
$_$
DECLARE
    res @extschema@.powa_all_tables_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.seq_scan = a.seq_scan - b.seq_scan;
    res.seq_tup_read = a.seq_tup_read - b.seq_tup_read;
    res.idx_scan = a.idx_scan - b.idx_scan;
    res.n_tup_ins = a.n_tup_ins - b.n_tup_ins;
    res.n_tup_upd = a.n_tup_upd - b.n_tup_upd;
    res.n_tup_del = a.n_tup_del - b.n_tup_del;
    res.n_tup_hot_upd = a.n_tup_hot_upd - b.n_tup_hot_upd;
    res.n_tup_newpage_upd = a.n_tup_newpage_upd - b.n_tup_newpage_upd;
    res.n_liv_tup = a.n_liv_tup - b.n_liv_tup;
    res.n_dead_tup = a.n_dead_tup - b.n_dead_tup;
    res.n_mod_since_analyze = a.n_mod_since_analyze - b.n_mod_since_analyze;
    res.n_ins_since_vacuum = a.n_ins_since_vacuum - b.n_ins_since_vacuum;
    res.vacuum_count = a.vacuum_count - b.vacuum_count;
    res.autovacuum_count = a.autovacuum_count - b.autovacuum_count;
    res.analyze_count = a.analyze_count - b.analyze_count;
    res.autoanalyze_count = a.autoanalyze_count - b.autoanalyze_count;
    res.heap_blks_read = a.heap_blks_read - b.heap_blks_read;
    res.heap_blks_hit = a.heap_blks_hit - b.heap_blks_hit;
    res.idx_blks_read = a.idx_blks_read - b.idx_blks_read;
    res.idx_blks_hit = a.idx_blks_hit - b.idx_blks_hit;
    res.toast_blks_read = a.toast_blks_read - b.toast_blks_read;
    res.toast_blks_hit = a.toast_blks_hit - b.toast_blks_hit;
    res.tidx_blks_read = a.tidx_blks_read - b.tidx_blks_read;
    res.tidx_blks_hit = a.tidx_blks_hit - b.tidx_blks_hit;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_all_tables_history_mi,
    LEFTARG = @extschema@.powa_all_tables_history_record,
    RIGHTARG = @extschema@.powa_all_tables_history_record
);

CREATE TYPE @extschema@.powa_all_tables_history_rate AS (
    sec integer,
    seq_scan_per_sec double precision,
    seq_tup_read_per_sec double precision,
    idx_scan_per_sec double precision,
    n_tup_ins_per_sec double precision,
    n_tup_upd_per_sec double precision,
    n_tup_del_per_sec double precision,
    n_tup_hot_upd_per_sec double precision,
    n_tup_newpage_upd_per_sec double precision,
    n_liv_tup_per_sec double precision,
    n_dead_tup_per_sec double precision,
    n_mod_since_analyze_per_sec double precision,
    n_ins_since_vacuum_per_sec double precision,
    vacuum_count_per_sec double precision,
    autovacuum_count_per_sec double precision,
    analyze_count_per_sec double precision,
    autoanalyze_count_per_sec double precision,
    heap_blks_read_per_sec double precision,
    heap_blks_hit_per_sec double precision,
    idx_blks_read_per_sec double precision,
    idx_blks_hit_per_sec double precision,
    toast_blks_read_per_sec double precision,
    toast_blks_hit_per_sec double precision,
    tidx_blks_read_per_sec double precision,
    tidx_blks_hit_per_sec double precision
);

CREATE OR REPLACE FUNCTION @extschema@.powa_all_tables_history_div(
    a @extschema@.powa_all_tables_history_record,
    b @extschema@.powa_all_tables_history_record)
RETURNS @extschema@.powa_all_tables_history_rate AS
$_$
DECLARE
    res @extschema@.powa_all_tables_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.seq_scan_per_sec = (a.seq_scan - b.seq_scan)::double precision / sec;
    res.seq_tup_read_per_sec = (a.seq_tup_read - b.seq_tup_read)::double precision / sec;
    res.idx_scan_per_sec = (a.idx_scan - b.idx_scan)::double precision / sec;
    res.n_tup_ins_per_sec = (a.n_tup_ins - b.n_tup_ins)::double precision / sec;
    res.n_tup_upd_per_sec = (a.n_tup_upd - b.n_tup_upd)::double precision / sec;
    res.n_tup_del_per_sec = (a.n_tup_del - b.n_tup_del)::double precision / sec;
    res.n_tup_hot_upd_per_sec = (a.n_tup_hot_upd - b.n_tup_hot_upd)::double precision / sec;
    res.n_tup_newpage_upd_per_sec = (a.n_tup_newpage_upd - b.n_tup_newpage_upd)::double precision / sec;
    res.n_liv_tup_per_sec = (a.n_liv_tup - b.n_liv_tup)::double precision / sec;
    res.n_dead_tup_per_sec = (a.n_dead_tup - b.n_dead_tup)::double precision / sec;
    res.n_mod_since_analyze_per_sec = (a.n_mod_since_analyze - b.n_mod_since_analyze)::double precision / sec;
    res.n_ins_since_vacuum_per_sec = (a.n_ins_since_vacuum - b.n_ins_since_vacuum)::double precision / sec;
    res.vacuum_count_per_sec = (a.vacuum_count - b.vacuum_count)::double precision / sec;
    res.autovacuum_count_per_sec = (a.autovacuum_count - b.autovacuum_count)::double precision / sec;
    res.analyze_count_per_sec = (a.analyze_count - b.analyze_count)::double precision / sec;
    res.autoanalyze_count_per_sec = (a.autoanalyze_count - b.autoanalyze_count)::double precision / sec;
    res.heap_blks_read_per_sec = (a.heap_blks_read - b.heap_blks_read)::double precision / sec;
    res.heap_blks_hit_per_sec = (a.heap_blks_hit - b.heap_blks_hit)::double precision / sec;
    res.idx_blks_read_per_sec = (a.idx_blks_read - b.idx_blks_read)::double precision / sec;
    res.idx_blks_hit_per_sec = (a.idx_blks_hit - b.idx_blks_hit)::double precision / sec;
    res.toast_blks_read_per_sec = (a.toast_blks_read - b.toast_blks_read)::double precision / sec;
    res.toast_blks_hit_per_sec = (a.toast_blks_hit - b.toast_blks_hit)::double precision / sec;
    res.tidx_blks_read_per_sec = (a.tidx_blks_read - b.tidx_blks_read)::double precision / sec;
    res.tidx_blks_hit_per_sec = (a.tidx_blks_hit - b.tidx_blks_hit)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_all_tables_history_div,
    LEFTARG = @extschema@.powa_all_tables_history_record,
    RIGHTARG = @extschema@.powa_all_tables_history_record
);
/* end of pg_stat_all_tables operator support */

-- We need a type different than powa_all_tables_history_record as we can't
-- aggregate the last last_* timestamps
CREATE TYPE @extschema@.powa_all_tables_history_db_record AS (
    ts timestamp with time zone,
    -- pg_stat_all_tables fields
    seq_scan bigint,
    seq_tup_read bigint,
    idx_scan bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    n_tup_newpage_upd bigint,
    n_liv_tup bigint,
    n_dead_tup bigint,
    n_mod_since_analyze bigint,
    n_ins_since_vacuum bigint,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint,
    -- pg_statio_all_tables fields
    heap_blks_read bigint,
    heap_blks_hit bigint,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    toast_blks_read bigint,
    toast_blks_hit bigint,
    tidx_blks_read bigint,
    tidx_blks_hit bigint
);

/* pg_stat_all_tables_db operator support */
CREATE OR REPLACE FUNCTION @extschema@.powa_all_tables_history_db_mi(
    a @extschema@.powa_all_tables_history_db_record,
    b @extschema@.powa_all_tables_history_db_record)
RETURNS @extschema@.powa_all_tables_history_diff AS
$_$
DECLARE
    res @extschema@.powa_all_tables_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.seq_scan = a.seq_scan - b.seq_scan;
    res.seq_tup_read = a.seq_tup_read - b.seq_tup_read;
    res.idx_scan = a.idx_scan - b.idx_scan;
    res.n_tup_ins = a.n_tup_ins - b.n_tup_ins;
    res.n_tup_upd = a.n_tup_upd - b.n_tup_upd;
    res.n_tup_del = a.n_tup_del - b.n_tup_del;
    res.n_tup_hot_upd = a.n_tup_hot_upd - b.n_tup_hot_upd;
    res.n_tup_newpage_upd = a.n_tup_newpage_upd - b.n_tup_newpage_upd;
    res.n_liv_tup = a.n_liv_tup - b.n_liv_tup;
    res.n_dead_tup = a.n_dead_tup - b.n_dead_tup;
    res.n_mod_since_analyze = a.n_mod_since_analyze - b.n_mod_since_analyze;
    res.n_ins_since_vacuum = a.n_ins_since_vacuum - b.n_ins_since_vacuum;
    res.vacuum_count = a.vacuum_count - b.vacuum_count;
    res.autovacuum_count = a.autovacuum_count - b.autovacuum_count;
    res.analyze_count = a.analyze_count - b.analyze_count;
    res.autoanalyze_count = a.autoanalyze_count - b.autoanalyze_count;
    res.heap_blks_read = a.heap_blks_read - b.heap_blks_read;
    res.heap_blks_hit = a.heap_blks_hit - b.heap_blks_hit;
    res.idx_blks_read = a.idx_blks_read - b.idx_blks_read;
    res.idx_blks_hit = a.idx_blks_hit - b.idx_blks_hit;
    res.toast_blks_read = a.toast_blks_read - b.toast_blks_read;
    res.toast_blks_hit = a.toast_blks_hit - b.toast_blks_hit;
    res.tidx_blks_read = a.tidx_blks_read - b.tidx_blks_read;
    res.tidx_blks_hit = a.tidx_blks_hit - b.tidx_blks_hit;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_all_tables_history_db_mi,
    LEFTARG = @extschema@.powa_all_tables_history_db_record,
    RIGHTARG = @extschema@.powa_all_tables_history_db_record
);

CREATE OR REPLACE FUNCTION @extschema@.powa_all_tables_history_db_div(
    a @extschema@.powa_all_tables_history_db_record,
    b @extschema@.powa_all_tables_history_db_record)
RETURNS @extschema@.powa_all_tables_history_rate AS
$_$
DECLARE
    res @extschema@.powa_all_tables_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.seq_scan_per_sec = (a.seq_scan - b.seq_scan)::double precision / sec;
    res.seq_tup_read_per_sec = (a.seq_tup_read - b.seq_tup_read)::double precision / sec;
    res.idx_scan_per_sec = (a.idx_scan - b.idx_scan)::double precision / sec;
    res.n_tup_ins_per_sec = (a.n_tup_ins - b.n_tup_ins)::double precision / sec;
    res.n_tup_upd_per_sec = (a.n_tup_upd - b.n_tup_upd)::double precision / sec;
    res.n_tup_del_per_sec = (a.n_tup_del - b.n_tup_del)::double precision / sec;
    res.n_tup_hot_upd_per_sec = (a.n_tup_hot_upd - b.n_tup_hot_upd)::double precision / sec;
    res.n_tup_newpage_upd_per_sec = (a.n_tup_newpage_upd - b.n_tup_newpage_upd)::double precision / sec;
    res.n_liv_tup_per_sec = (a.n_liv_tup - b.n_liv_tup)::double precision / sec;
    res.n_dead_tup_per_sec = (a.n_dead_tup - b.n_dead_tup)::double precision / sec;
    res.n_mod_since_analyze_per_sec = (a.n_mod_since_analyze - b.n_mod_since_analyze)::double precision / sec;
    res.n_ins_since_vacuum_per_sec = (a.n_ins_since_vacuum - b.n_ins_since_vacuum)::double precision / sec;
    res.vacuum_count_per_sec = (a.vacuum_count - b.vacuum_count)::double precision / sec;
    res.autovacuum_count_per_sec = (a.autovacuum_count - b.autovacuum_count)::double precision / sec;
    res.analyze_count_per_sec = (a.analyze_count - b.analyze_count)::double precision / sec;
    res.autoanalyze_count_per_sec = (a.autoanalyze_count - b.autoanalyze_count)::double precision / sec;
    res.heap_blks_read_per_sec = (a.heap_blks_read - b.heap_blks_read)::double precision / sec;
    res.heap_blks_hit_per_sec = (a.heap_blks_hit - b.heap_blks_hit)::double precision / sec;
    res.idx_blks_read_per_sec = (a.idx_blks_read - b.idx_blks_read)::double precision / sec;
    res.idx_blks_hit_per_sec = (a.idx_blks_hit - b.idx_blks_hit)::double precision / sec;
    res.toast_blks_read_per_sec = (a.toast_blks_read - b.toast_blks_read)::double precision / sec;
    res.toast_blks_hit_per_sec = (a.toast_blks_hit - b.toast_blks_hit)::double precision / sec;
    res.tidx_blks_read_per_sec = (a.tidx_blks_read - b.tidx_blks_read)::double precision / sec;
    res.tidx_blks_hit_per_sec = (a.tidx_blks_hit - b.tidx_blks_hit)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_all_tables_history_db_div,
    LEFTARG = @extschema@.powa_all_tables_history_db_record,
    RIGHTARG = @extschema@.powa_all_tables_history_db_record
);
/* end of pg_stat_all_tables_db operator support */

CREATE TYPE @extschema@.powa_stat_bgwriter_history_record AS (
    ts timestamp with time zone,
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time double precision,
    buffers_checkpoint bigint,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint
);

/*pg_stat_bgwriter operator support */
CREATE TYPE @extschema@.powa_stat_bgwriter_history_diff AS (
    intvl interval,
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time double precision,
    buffers_checkpoint bigint,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint
);

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_bgwriter_history_mi(
    a @extschema@.powa_stat_bgwriter_history_record,
    b @extschema@.powa_stat_bgwriter_history_record)
RETURNS @extschema@.powa_stat_bgwriter_history_diff AS
$_$
DECLARE
    res @extschema@.powa_stat_bgwriter_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.checkpoints_timed = a.checkpoints_timed - b.checkpoints_timed;
    res.checkpoints_req = a.checkpoints_req - b.checkpoints_req;
    res.checkpoint_write_time = a.checkpoint_write_time - b.checkpoint_write_time;
    res.checkpoint_sync_time = a.checkpoint_sync_time - b.checkpoint_sync_time;
    res.buffers_checkpoint = a.buffers_checkpoint - b.buffers_checkpoint;
    res.buffers_clean = a.buffers_clean - b.buffers_clean;
    res.maxwritten_clean = a.maxwritten_clean - b.maxwritten_clean;
    res.buffers_backend = a.buffers_backend - b.buffers_backend;
    res.buffers_backend_fsync = a.buffers_backend_fsync - b.buffers_backend_fsync;
    res.buffers_alloc = a.buffers_alloc - b.buffers_alloc;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_stat_bgwriter_history_mi,
    LEFTARG = @extschema@.powa_stat_bgwriter_history_record,
    RIGHTARG = @extschema@.powa_stat_bgwriter_history_record
);

CREATE TYPE @extschema@.powa_stat_bgwriter_history_rate AS (
    sec integer,
    checkpoints_timed_per_sec double precision,
    checkpoints_req_per_sec double precision,
    checkpoint_write_time_per_sec double precision,
    checkpoint_sync_time_per_sec double precision,
    buffers_checkpoint_per_sec double precision,
    buffers_clean_per_sec double precision,
    maxwritten_clean_per_sec double precision,
    buffers_backend_per_sec double precision,
    buffers_backend_fsync_per_sec double precision,
    buffers_alloc_per_sec double precision
);

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_bgwriter_history_div(
    a @extschema@.powa_stat_bgwriter_history_record,
    b @extschema@.powa_stat_bgwriter_history_record)
RETURNS @extschema@.powa_stat_bgwriter_history_rate AS
$_$
DECLARE
    res @extschema@.powa_stat_bgwriter_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.checkpoints_timed_per_sec = (a.checkpoints_timed - b.checkpoints_timed)::double precision / sec;
    res.checkpoints_req_per_sec = (a.checkpoints_req - b.checkpoints_req)::double precision / sec;
    res.checkpoint_write_time_per_sec = (a.checkpoint_write_time - b.checkpoint_write_time)::double precision / sec;
    res.checkpoint_sync_time_per_sec = (a.checkpoint_sync_time - b.checkpoint_sync_time)::double precision / sec;
    res.buffers_checkpoint_per_sec = (a.buffers_checkpoint - b.buffers_checkpoint)::double precision / sec;
    res.buffers_clean_per_sec = (a.buffers_clean - b.buffers_clean)::double precision / sec;
    res.maxwritten_clean_per_sec = (a.maxwritten_clean - b.maxwritten_clean)::double precision / sec;
    res.buffers_backend_per_sec = (a.buffers_backend - b.buffers_backend)::double precision / sec;
    res.buffers_backend_fsync_per_sec = (a.buffers_backend_fsync - b.buffers_backend_fsync)::double precision / sec;
    res.buffers_alloc_per_sec = (a.buffers_alloc - b.buffers_alloc)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_stat_bgwriter_history_div,
    LEFTARG = @extschema@.powa_stat_bgwriter_history_record,
    RIGHTARG = @extschema@.powa_stat_bgwriter_history_record
);
/* end of pg_stat_bgwriter operator support */


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
    blk_read_time double precision NOT NULL,
    blk_write_time double precision NOT NULL,
    plans bigint NOT NULL,
    total_plan_time double precision NOT NULL,
    wal_records bigint NOT NULL,
    wal_fpi bigint NOT NULL,
    wal_bytes numeric NOT NULL
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

CREATE UNLOGGED TABLE @extschema@.powa_stat_bgwriter_src_tmp (
    srvid integer NOT NULL,
    ts timestamp with time zone NOT NULL,
    checkpoints_timed bigint NOT NULL,
    checkpoints_req bigint NOT NULL,
    checkpoint_write_time double precision NOT NULL,
    checkpoint_sync_time double precision NOT NULL,
    buffers_checkpoint bigint NOT NULL,
    buffers_clean bigint NOT NULL,
    maxwritten_clean bigint NOT NULL,
    buffers_backend bigint NOT NULL,
    buffers_backend_fsync bigint NOT NULL,
    buffers_alloc bigint NOT NULL
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

CREATE INDEX powa_user_functions_history_funcid_ts ON @extschema@.powa_user_functions_history USING gist (srvid, funcid, coalesce_range);

CREATE TABLE @extschema@.powa_user_functions_history_current (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    funcid oid NOT NULL,
    record @extschema@.powa_user_functions_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_user_functions_history_current(srvid);

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

CREATE TABLE @extschema@.powa_stat_bgwriter_history (
    srvid integer NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_stat_bgwriter_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_stat_bgwriter_history_record NOT NULL,
    maxs_in_range @extschema@.powa_stat_bgwriter_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX powa_stat_bgwriter_history_ts ON @extschema@.powa_stat_bgwriter_history USING gist (srvid, coalesce_range);

CREATE TABLE @extschema@.powa_stat_bgwriter_history_current (
    srvid integer NOT NULL,
    record @extschema@.powa_stat_bgwriter_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.powa_stat_bgwriter_history_current(srvid);

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
$_$ LANGUAGE plpgsql; /* end of powa_activate_module */

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
$_$ LANGUAGE plpgsql; /* end of powa_deactivate_module */

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
$_$ LANGUAGE plpgsql; /* end of powa_activate_db_module */

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
        SELECT array_agg(dbname)
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
$_$ LANGUAGE plpgsql; /* end of powa_deactivate_db_module */

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
$_$ LANGUAGE plpgsql; /* end of powa_activate_extension */

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
$_$ LANGUAGE plpgsql; /* end of powa_register_server */

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
$_$ LANGUAGE plpgsql; /* powa_config_server */

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
$_$ LANGUAGE plpgsql; /* powa_deactivate_server */

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
$_$ LANGUAGE plpgsql; /* powa_deactivate_server */

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
        $_$;
    ELSE
        CREATE FUNCTION @extschema@.powa_get_guc (guc text, def text DEFAULT NULL) RETURNS text
        LANGUAGE plpgsql
        AS $_$
        BEGIN
            RETURN COALESCE(current_setting(guc, true), def);
        END;
        $_$;
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
$_$;

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
$_$ LANGUAGE plpgsql; /* end of powa_get_server_retention */

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

CREATE TYPE @extschema@.powa_kcache_history_record AS (
    ts timestamptz,
    plan_reads       bigint,             /* total reads, in bytes */
    plan_writes      bigint,             /* total writes, in bytes */
    plan_user_time   double precision,   /* total user CPU time used */
    plan_system_time double precision,   /* total system CPU time used */
    plan_minflts     bigint,             /* total page reclaims (soft page faults) */
    plan_majflts     bigint,             /* total page faults (hard page faults) */
    plan_nswaps      bigint,             /* total swaps */
    plan_msgsnds     bigint,             /* total IPC messages sent */
    plan_msgrcvs     bigint,             /* total IPC messages received */
    plan_nsignals    bigint,             /* total signals received */
    plan_nvcsws      bigint,             /* total voluntary context switches */
    plan_nivcsws     bigint,             /* total involuntary context switches */
    exec_reads       bigint,             /* total reads, in bytes */
    exec_writes      bigint,             /* total writes, in bytes */
    exec_user_time   double precision,   /* total user CPU time used */
    exec_system_time double precision,   /* total system CPU time used */
    exec_minflts     bigint,             /* total page reclaims (soft page faults) */
    exec_majflts     bigint,             /* total page faults (hard page faults) */
    exec_nswaps      bigint,             /* total swaps */
    exec_msgsnds     bigint,             /* total IPC messages sent */
    exec_msgrcvs     bigint,             /* total IPC messages received */
    exec_nsignals    bigint,             /* total signals received */
    exec_nvcsws      bigint,             /* total voluntary context switches */
    exec_nivcsws     bigint              /* total involuntary context switches */
);

/* pg_stat_kcache operator support */
CREATE TYPE @extschema@.powa_kcache_history_diff AS (
    intvl interval,
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
    exec_user_time   double precision,
    exec_system_time double precision,
    exec_minflts     bigint,
    exec_majflts     bigint,
    exec_nswaps      bigint,
    exec_msgsnds     bigint,
    exec_msgrcvs     bigint,
    exec_nsignals    bigint,
    exec_nvcsws      bigint,
    exec_nivcsws     bigint
);

CREATE OR REPLACE FUNCTION @extschema@.powa_kcache_history_mi(
    a @extschema@.powa_kcache_history_record,
    b @extschema@.powa_kcache_history_record)
RETURNS @extschema@.powa_kcache_history_diff AS
$_$
DECLARE
    res @extschema@.powa_kcache_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.plan_reads = a.plan_reads - b.plan_reads;
    res.plan_writes = a.plan_writes - b.plan_writes;
    res.plan_user_time = a.plan_user_time - b.plan_user_time;
    res.plan_system_time = a.plan_system_time - b.plan_system_time;
    res.plan_minflts = a.plan_minflts - b.plan_minflts;
    res.plan_majflts = a.plan_majflts - b.plan_majflts;
    res.plan_nswaps = a.plan_nswaps - b.plan_nswaps;
    res.plan_msgsnds = a.plan_msgsnds - b.plan_msgsnds;
    res.plan_msgrcvs = a.plan_msgrcvs - b.plan_msgrcvs;
    res.plan_nsignals = a.plan_nsignals - b.plan_nsignals;
    res.plan_nvcsws = a.plan_nvcsws - b.plan_nvcsws;
    res.plan_nivcsws = a.plan_nivcsws - b.plan_nivcsws;
    res.exec_reads = a.exec_reads - b.exec_reads;
    res.exec_writes = a.exec_writes - b.exec_writes;
    res.exec_user_time = a.exec_user_time - b.exec_user_time;
    res.exec_system_time = a.exec_system_time - b.exec_system_time;
    res.exec_minflts = a.exec_minflts - b.exec_minflts;
    res.exec_majflts = a.exec_majflts - b.exec_majflts;
    res.exec_nswaps = a.exec_nswaps - b.exec_nswaps;
    res.exec_msgsnds = a.exec_msgsnds - b.exec_msgsnds;
    res.exec_msgrcvs = a.exec_msgrcvs - b.exec_msgrcvs;
    res.exec_nsignals = a.exec_nsignals - b.exec_nsignals;
    res.exec_nvcsws = a.exec_nvcsws - b.exec_nvcsws;
    res.exec_nivcsws = a.exec_nivcsws - b.exec_nivcsws;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_kcache_history_mi,
    LEFTARG = @extschema@.powa_kcache_history_record,
    RIGHTARG = @extschema@.powa_kcache_history_record
);

CREATE TYPE @extschema@.powa_kcache_history_rate AS (
    sec integer,
    plan_reads_per_sec       double precision,
    plan_writes_per_sec      double precision,
    plan_user_time_per_sec   double precision,
    plan_system_time_per_sec double precision,
    plan_minflts_per_sec     double precision,
    plan_majflts_per_sec     double precision,
    plan_nswaps_per_sec      double precision,
    plan_msgsnds_per_sec     double precision,
    plan_msgrcvs_per_sec     double precision,
    plan_nsignals_per_sec    double precision,
    plan_nvcsws_per_sec      double precision,
    plan_nivcsws_per_sec     double precision,
    exec_reads_per_sec       double precision,
    exec_writes_per_sec      double precision,
    exec_user_time_per_sec   double precision,
    exec_system_time_per_sec double precision,
    exec_minflts_per_sec     double precision,
    exec_majflts_per_sec     double precision,
    exec_nswaps_per_sec      double precision,
    exec_msgsnds_per_sec     double precision,
    exec_msgrcvs_per_sec     double precision,
    exec_nsignals_per_sec    double precision,
    exec_nvcsws_per_sec      double precision,
    exec_nivcsws_per_sec     double precision
);

CREATE OR REPLACE FUNCTION @extschema@.powa_kcache_history_div(
    a @extschema@.powa_kcache_history_record,
    b @extschema@.powa_kcache_history_record)
RETURNS @extschema@.powa_kcache_history_rate AS
$_$
DECLARE
    res @extschema@.powa_kcache_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.plan_reads_per_sec = (a.plan_reads - b.plan_reads)::double precision / sec;
    res.plan_writes_per_sec = (a.plan_writes - b.plan_writes)::double precision / sec;
    res.plan_user_time_per_sec = (a.plan_user_time - b.plan_user_time)::double precision / sec;
    res.plan_system_time_per_sec = (a.plan_system_time - b.plan_system_time)::double precision / sec;
    res.plan_minflts_per_sec = (a.plan_minflts - b.plan_minflts)::double precision / sec;
    res.plan_majflts_per_sec = (a.plan_majflts - b.plan_majflts)::double precision / sec;
    res.plan_nswaps_per_sec = (a.plan_nswaps - b.plan_nswaps)::double precision / sec;
    res.plan_msgsnds_per_sec = (a.plan_msgsnds - b.plan_msgsnds)::double precision / sec;
    res.plan_msgrcvs_per_sec = (a.plan_msgrcvs - b.plan_msgrcvs)::double precision / sec;
    res.plan_nsignals_per_sec = (a.plan_nsignals - b.plan_nsignals)::double precision / sec;
    res.plan_nvcsws_per_sec = (a.plan_nvcsws - b.plan_nvcsws)::double precision / sec;
    res.plan_nivcsws_per_sec = (a.plan_nivcsws - b.plan_nivcsws)::double precision / sec;
    res.exec_reads_per_sec = (a.exec_reads - b.exec_reads)::double precision / sec;
    res.exec_writes_per_sec = (a.exec_writes - b.exec_writes)::double precision / sec;
    res.exec_user_time_per_sec = (a.exec_user_time - b.exec_user_time)::double precision / sec;
    res.exec_system_time_per_sec = (a.exec_system_time - b.exec_system_time)::double precision / sec;
    res.exec_minflts_per_sec = (a.exec_minflts - b.exec_minflts)::double precision / sec;
    res.exec_majflts_per_sec = (a.exec_majflts - b.exec_majflts)::double precision / sec;
    res.exec_nswaps_per_sec = (a.exec_nswaps - b.exec_nswaps)::double precision / sec;
    res.exec_msgsnds_per_sec = (a.exec_msgsnds - b.exec_msgsnds)::double precision / sec;
    res.exec_msgrcvs_per_sec = (a.exec_msgrcvs - b.exec_msgrcvs)::double precision / sec;
    res.exec_nsignals_per_sec = (a.exec_nsignals - b.exec_nsignals)::double precision / sec;
    res.exec_nvcsws_per_sec = (a.exec_nvcsws - b.exec_nvcsws)::double precision / sec;
    res.exec_nivcsws_per_sec = (a.exec_nivcsws - b.exec_nivcsws)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_kcache_history_div,
    LEFTARG = @extschema@.powa_kcache_history_record,
    RIGHTARG = @extschema@.powa_kcache_history_record
);

/* end of pg_stat_kcache operator support */

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

CREATE TYPE @extschema@.powa_qualstats_history_record AS (
  ts timestamptz,
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

/* pg_qualstats operator support */
CREATE TYPE @extschema@.powa_qualstats_history_diff AS (
    intvl interval,
    occurences bigint,
    execution_count bigint,
    nbfiltered bigint,
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision
);

CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_history_mi(
    a @extschema@.powa_qualstats_history_record,
    b @extschema@.powa_qualstats_history_record)
RETURNS @extschema@.powa_qualstats_history_diff AS
$_$
DECLARE
    res @extschema@.powa_qualstats_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.occurences = a.occurences - b.occurences;
    res.execution_count = a.execution_count - b.execution_count;
    res.nbfiltered = a.nbfiltered - b.nbfiltered;
    res.mean_err_estimate_ratio = a.mean_err_estimate_ratio - b.mean_err_estimate_ratio;
    res.mean_err_estimate_num = a.mean_err_estimate_num - b.mean_err_estimate_num;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_qualstats_history_mi,
    LEFTARG = @extschema@.powa_qualstats_history_record,
    RIGHTARG = @extschema@.powa_qualstats_history_record
);

CREATE TYPE @extschema@.powa_qualstats_history_rate AS (
    sec integer,
    occurences_per_sec double precision,
    execution_count_per_sec double precision,
    nbfiltered_per_sec double precision,
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision
);

CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_history_div(
    a @extschema@.powa_qualstats_history_record,
    b @extschema@.powa_qualstats_history_record)
RETURNS @extschema@.powa_qualstats_history_rate AS
$_$
DECLARE
    res @extschema@.powa_qualstats_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.occurences_per_sec = (a.occurences - b.occurences)::double precision / sec;
    res.execution_count_per_sec = (a.execution_count - b.execution_count)::double precision / sec;
    res.nbfiltered_per_sec = (a.nbfiltered - b.nbfiltered)::double precision / sec;
    res.mean_err_estimate_ratio = (a.mean_err_estimate_ratio - b.mean_err_estimate_ratio)::double precision / sec;
    res.mean_err_estimate_num = (a.mean_err_estimate_num - b.mean_err_estimate_num)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_qualstats_history_div,
    LEFTARG = @extschema@.powa_qualstats_history_record,
    RIGHTARG = @extschema@.powa_qualstats_history_record
);
/* end of pg_qualstats operator support */

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

CREATE TYPE @extschema@.powa_wait_sampling_history_record AS (
    ts timestamptz,
    count bigint
);

/* pg_wait_sampling operator support */
CREATE TYPE @extschema@.powa_wait_sampling_history_diff AS (
    intvl interval,
    count bigint
);

CREATE OR REPLACE FUNCTION @extschema@.powa_wait_sampling_history_mi(
    a @extschema@.powa_wait_sampling_history_record,
    b @extschema@.powa_wait_sampling_history_record)
RETURNS @extschema@.powa_wait_sampling_history_diff AS
$_$
DECLARE
    res @extschema@.powa_wait_sampling_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.count = a.count - b.count;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@.- (
    PROCEDURE = @extschema@.powa_wait_sampling_history_mi,
    LEFTARG = @extschema@.powa_wait_sampling_history_record,
    RIGHTARG = @extschema@.powa_wait_sampling_history_record
);

CREATE TYPE @extschema@.powa_wait_sampling_history_rate AS (
    sec integer,
    count_per_sec double precision
);

CREATE OR REPLACE FUNCTION @extschema@.powa_wait_sampling_history_div(
    a @extschema@.powa_wait_sampling_history_record,
    b @extschema@.powa_wait_sampling_history_record)
RETURNS @extschema@.powa_wait_sampling_history_rate AS
$_$
DECLARE
    res @extschema@.powa_wait_sampling_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.count_per_sec = (a.count - b.count)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR @extschema@./ (
    PROCEDURE = @extschema@.powa_wait_sampling_history_div,
    LEFTARG = @extschema@.powa_wait_sampling_history_record,
    RIGHTARG = @extschema@.powa_wait_sampling_history_record
);

/* end of pg_wait_sampling operator support */

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
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_user_functions_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_indexes_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_indexes_history_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_indexes_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_indexes_history_current_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_tables_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_tables_history_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_tables_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_all_tables_history_current_db','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_stat_bgwriter_history','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_stat_bgwriter_history_current','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_extensions','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_extension_functions','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_extension_config','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_module_functions','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_module_config','WHERE added_manually');
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
$_$; /* end of powa_check_created_extensions */

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
$_$; /* end of powa_check_dropped_extensions */

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
$PROC$ language plpgsql; /* end of powa_prevent_concurrent_snapshot() */

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
  v_coalesce bigint;
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
$PROC$ LANGUAGE plpgsql; /* end of powa_take_snapshot(int) */

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
$PROC$ LANGUAGE plpgsql; /* end of powa_databases_src */

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
$PROC$ language plpgsql; /* end of powa_databases_snapshot */

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
    OUT blk_read_time double precision,
    OUT blk_write_time double precision,
    OUT plans bigint,
    OUT total_plan_time float8,
    OUT wal_records bigint,
    OUT wal_fpi bigint,
    OUT wal_bytes numeric
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

        IF (v_pgss[1] = 1 AND v_pgss[2] >= 10) THEN
            RETURN QUERY EXECUTE format($$SELECT now(),
                pgss.userid, pgss.dbid, pgss.toplevel, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_exec_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time,
                pgss.plans, pgss.total_plan_time,
                pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes
            FROM %I.pg_stat_statements pgss
            JOIN pg_catalog.pg_database d ON d.oid = pgss.dbid
            JOIN pg_catalog.pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        @extschema@.powa_get_guc('powa.ignored_users', ''),
                        ',')))
            $$, v_nsp);
        ELSIF (v_pgss[1] = 1 AND v_pgss[2] >= 8) THEN
            RETURN QUERY EXECUTE format($$SELECT now(),
                pgss.userid, pgss.dbid, true::boolean, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_exec_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time,
                pgss.plans, pgss.total_plan_time,
                pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes
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
                pgss.temp_blks_written, pgss.blk_read_time,pgss.blk_write_time,
                0::bigint, 0::double precision,
                0::bigint, 0::bigint, 0::numeric
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
            pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time,
            pgss.plans, pgss.total_plan_time,
            pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes
        FROM @extschema@.powa_statements_src_tmp pgss WHERE srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_src */

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
                temp_blks_written, blk_read_time, blk_write_time,
                plans, total_plan_time,
                wal_records, wal_fpi, wal_bytes
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
                sum(temp_blks_written), sum(blk_read_time), sum(blk_write_time),
                sum(plans), sum(total_plan_time),
                sum(wal_records), sum(wal_fpi), sum(wal_bytes)
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
$PROC$ LANGUAGE plpgsql; /* end of powa_user_functions_src */

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
    )
    INSERT INTO @extschema@.powa_user_functions_history_current
        SELECT _srvid, dbid, funcid,
        ROW(ts, calls,
            total_time,
            self_time)::@extschema@.powa_user_functions_history_record AS record
        FROM func;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_user_functions_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_user_functions_snapshot */

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
            ROW(ts, idx_scan, last_idx_scan, idx_tup_read, idx_tup_fetch,
                idx_blks_read, idx_blks_hit
            )::@extschema@.powa_all_indexes_history_record AS record
            FROM rel
    ),

    by_database AS (
        INSERT INTO @extschema@.powa_all_indexes_history_current_db
        (srvid, dbid, record)
            SELECT _srvid AS srvid, dbid,
            ROW(ts,
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
$PROC$ language plpgsql; /* end of powa_all_indexes_snapshot */

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
$PROC$ language plpgsql; /* end of powa_all_tables_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_bgwriter_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT checkpoints_timed bigint,
    OUT checkpoints_req bigint,
    OUT checkpoint_write_time double precision,
    OUT checkpoint_sync_time double precision,
    OUT buffers_checkpoint bigint,
    OUT buffers_clean bigint,
    OUT maxwritten_clean bigint,
    OUT buffers_backend bigint,
    OUT buffers_backend_fsync bigint,
    OUT buffers_alloc bigint
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY SELECT now(),
            s.checkpoints_timed, s.checkpoints_req, s.checkpoint_write_time,
            s.checkpoint_sync_time, s.buffers_checkpoint, s.buffers_clean,
            s.maxwritten_clean, s.buffers_backend, s.buffers_backend_fsync,
            s.buffers_alloc
        FROM pg_catalog.pg_stat_bgwriter AS s;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.checkpoints_timed, s.checkpoints_req, s.checkpoint_write_time,
            s.checkpoint_sync_time, s.buffers_checkpoint, s.buffers_clean,
            s.maxwritten_clean, s.buffers_backend, s.buffers_backend_fsync,
            s.buffers_alloc
        FROM @extschema@.powa_stat_bgwriter_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_bgwriter_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_bgwriter_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_bgwriter_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert background writer statistics
    WITH rel AS (
        SELECT *
        FROM @extschema@.powa_stat_bgwriter_src(_srvid)
    )
    INSERT INTO @extschema@.powa_stat_bgwriter_history_current
        SELECT _srvid,
        ROW(ts, checkpoints_timed, checkpoints_req, checkpoint_write_time,
            checkpoint_sync_time, buffers_checkpoint, buffers_clean,
            maxwritten_clean, buffers_backend, buffers_backend_fsync,
            buffers_alloc)::@extschema@.powa_stat_bgwriter_history_record AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_stat_bgwriter_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_stat_bgwriter_snapshot */


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
$PROC$ LANGUAGE plpgsql; /* end of powa_databases_purge */


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
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_user_functions_purge */

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
$PROC$ LANGUAGE plpgsql; /* end of powa_all_tables_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_bgwriter_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_bgwriter_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_bgwriter_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_bgwriter_purge */


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
                min((record).blk_read_time),min((record).blk_write_time),
                min((record).plans),min((record).total_plan_time),
                min((record).wal_records),min((record).wal_fpi),
                min((record).wal_bytes)
            )::@extschema@.powa_statements_history_record,
            ROW(max((record).ts),
                max((record).calls),max((record).total_exec_time),
                max((record).rows),
                max((record).shared_blks_hit),max((record).shared_blks_read),
                max((record).shared_blks_dirtied),max((record).shared_blks_written),
                max((record).local_blks_hit),max((record).local_blks_read),
                max((record).local_blks_dirtied),max((record).local_blks_written),
                max((record).temp_blks_read),max((record).temp_blks_written),
                max((record).blk_read_time),max((record).blk_write_time),
                max((record).plans),max((record).total_plan_time),
                max((record).wal_records),max((record).wal_fpi),
                max((record).wal_bytes)
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
                min((record).blk_read_time),min((record).blk_write_time),
                min((record).plans),min((record).total_plan_time),
                min((record).wal_records),min((record).wal_fpi),
                min((record).wal_bytes)
            )::@extschema@.powa_statements_history_record,
            ROW(max((record).ts),
                max((record).calls),max((record).total_exec_time),
                max((record).rows),
                max((record).shared_blks_hit),max((record).shared_blks_read),
                max((record).shared_blks_dirtied),max((record).shared_blks_written),
                max((record).local_blks_hit),max((record).local_blks_read),
                max((record).local_blks_dirtied),max((record).local_blks_written),
                max((record).temp_blks_read),max((record).temp_blks_written),
                max((record).blk_read_time),max((record).blk_write_time),
                max((record).plans),max((record).total_plan_time),
                max((record).wal_records),max((record).wal_fpi),
                max((record).wal_bytes)
            )::@extschema@.powa_statements_history_record
        FROM @extschema@.powa_statements_history_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_statements_history_current_db WHERE srvid = _srvid;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_aggregate */

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_aggregate(_srvid integer)
RETURNS void AS $PROC$
BEGIN
    PERFORM @extschema@.powa_log('running powa_user_functions_aggregate(' || _srvid ||')');

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate user_functions table
    INSERT INTO @extschema@.powa_user_functions_history
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

    DELETE FROM @extschema@.powa_user_functions_history_current WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_user_functions_aggregate */

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
                min((record).idx_scan), min((record).last_idx_scan),
                min((record).idx_tup_read), min((record).idx_tup_fetch),
                min((record).idx_blks_read), min((record).idx_blks_hit)
            )::@extschema@.powa_all_indexes_history_record,
            ROW(max((record).ts),
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
                min((record).idx_scan),
                min((record).idx_tup_read), min((record).idx_tup_fetch),
                min((record).idx_blks_read), min((record).idx_blks_hit)
            )::@extschema@.powa_all_indexes_history_db_record,
            ROW(max((record).ts),
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
$PROC$ LANGUAGE plpgsql; /* end of powa_all_indexes_aggregate */

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
$PROC$ LANGUAGE plpgsql; /* end of powa_all_tables_aggregate */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_bgwriter_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_bgwriter_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate bgwriter table
    INSERT INTO @extschema@.powa_stat_bgwriter_history
        SELECT srvid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).checkpoints_timed),
                min((record).checkpoints_req),
                min((record).checkpoint_write_time),
                min((record).checkpoint_sync_time),
                min((record).buffers_checkpoint),
                min((record).buffers_clean),
                min((record).maxwritten_clean),
                min((record).buffers_backend),
                min((record).buffers_backend_fsync),
                min((record).buffers_alloc))::@extschema@.powa_stat_bgwriter_history_record,
            ROW(max((record).ts),
                max((record).checkpoints_timed),
                max((record).checkpoints_req),
                max((record).checkpoint_write_time),
                max((record).checkpoint_sync_time),
                max((record).buffers_checkpoint),
                max((record).buffers_clean),
                max((record).maxwritten_clean),
                max((record).buffers_backend),
                max((record).buffers_backend_fsync),
                max((record).buffers_alloc))::@extschema@.powa_stat_bgwriter_history_record
        FROM @extschema@.powa_stat_bgwriter_history_current
        WHERE srvid = _srvid
        GROUP BY srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_stat_bgwriter_history_current WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_bgwriter_aggregate */

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
    RETURN true;
END;
$function$; /* end of powa_reset */

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
$function$; /* end of powa_statements_reset */

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM @extschema@.powa_log('Resetting powa_user_functions_history(' || _srvid || ')');
    DELETE FROM @extschema@.powa_user_functions_history WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_user_functions_history_current(' || _srvid || ')');
    DELETE FROM @extschema@.powa_user_functions_history_current WHERE srvid = _srvid;

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
$function$; /* end of powa_all_indexes_reset */

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
$function$; /* end of powa_all_tables_reset */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_bgwriter_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM @extschema@.powa_log('Resetting powa_stat_bgwriter_history(' || _srvid || ')');
    DELETE FROM @extschema@.powa_stat_bgwriter_history WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_stat_bgwriter_history_current(' || _srvid || ')');
    DELETE FROM @extschema@.powa_stat_bgwriter_history_current WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log('Resetting powa_stat_bgwriter_src_tmp(' || _srvid || ')');
    DELETE FROM @extschema@.powa_stat_bgwriter_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_stat_bgwriter_reset */

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
$PROC$ LANGUAGE plpgsql; /* end of powa_kcache_src */

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
$PROC$ language plpgsql; /* end of powa_kcache_snapshot */

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
$PROC$ language plpgsql; /* end of powa_kcache_aggregate */

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
$PROC$ language plpgsql; /* end of powa_kcache_reset */

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
$_$ LANGUAGE sql; /* end of powa_qualstats_aggregate_constvalues_current */

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
$PROC$ LANGUAGE plpgsql; /* end of powa_qualstats_src */

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
$PROC$ language plpgsql; /* end of powa_qualstats_snapshot */

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
$PROC$ language plpgsql; /* end of powa_qualstats_aggregate */

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
$PROC$ language plpgsql; /* end of powa_qualstats_purge */

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
$PROC$ language plpgsql; /* end of powa_qualstats_reset */

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
$PROC$ LANGUAGE plpgsql; /* end of powa_wait_sampling_src */

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
$PROC$ language plpgsql; /* end of powa_wait_sampling_snapshot */

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
$PROC$ language plpgsql; /* end of powa_wait_sampling_aggregate */

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
$PROC$ language plpgsql; /* end of powa_wait_sampling_purge */

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
$PROC$ language plpgsql; /* end of powa_wait_sampling_reset */

/* end of pg_wait_sampling integration - part 2 */

-- Finally, activate any supported extension that's already locally installed.
SELECT @extschema@.powa_activate_extension(0, extname)
FROM @extschema@.powa_extensions p
JOIN pg_catalog.pg_extension e USING (extname);
