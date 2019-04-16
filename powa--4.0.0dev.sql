-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "CREATE EXTENSION powa" to load this file. \quit

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET client_min_messages = warning;
SET escape_string_warning = off;
SET search_path = public, pg_catalog;

-- in case the extension is not in shared_preload_libraries, we don't want to
-- fail on powa.debug GUC not being present
LOAD 'powa';

CREATE TABLE powa_servers(
    id serial PRIMARY KEY,
    hostname text NOT NULL,
    alias text,
    port integer NOT NULL,
    username text NOT NULL,
    password text,
    dbname text NOT NULL,
    frequency integer NOT NULL default 60,
    retention interval NOT NULL default '1 day'::interval,
    UNIQUE (hostname, port),
    UNIQUE(alias)
);
INSERT INTO powa_servers VALUES (0, '', '<local>', 0, '', NULL, '', -1, '0 second');

CREATE TABLE powa_snapshot_metas(
    srvid integer PRIMARY KEY,
    coalesce_seq bigint NOT NULL default (1),
    snapts timestamp with time zone NOT NULL default '-infinity'::timestamptz,
    aggts timestamp with time zone NOT NULL default '-infinity'::timestamptz,
    purgets timestamp with time zone NOT NULL default '-infinity'::timestamptz,
    errors text[],
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
INSERT INTO powa_snapshot_metas (srvid) VALUES (0);

CREATE TABLE powa_databases(
    srvid   integer NOT NULL,
    oid     oid,
    datname name,
    dropped timestamp with time zone,
    PRIMARY KEY (srvid, oid),
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE powa_statements (
    srvid integer NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    query text NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES powa_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

ALTER TABLE ONLY powa_statements
    ADD CONSTRAINT powa_statements_pkey PRIMARY KEY (srvid, queryid, dbid, userid);

CREATE INDEX powa_statements_dbid_idx ON powa_statements(srvid, dbid);
CREATE INDEX powa_statements_userid_idx ON powa_statements(userid);

CREATE FUNCTION powa_stat_user_functions(IN dbid oid, OUT funcid oid,
    OUT calls bigint,
    OUT total_time double precision,
    OUT self_time double precision)
    RETURNS SETOF record
    LANGUAGE c COST 100
AS '$libdir/powa', 'powa_stat_user_functions';

CREATE FUNCTION powa_stat_all_rel(IN dbid oid,
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
    RETURNS SETOF record
    LANGUAGE c COST 100
AS '$libdir/powa', 'powa_stat_all_rel';

CREATE TYPE powa_statements_history_record AS (
    ts timestamp with time zone,
    calls bigint,
    total_time double precision,
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
    blk_write_time double precision
);

/* pg_stat_statements operator support */
CREATE TYPE powa_statements_history_diff AS (
    intvl interval,
    calls bigint,
    total_time double precision,
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
    blk_write_time double precision
);

CREATE OR REPLACE FUNCTION powa_statements_history_mi(
    a powa_statements_history_record,
    b powa_statements_history_record)
RETURNS powa_statements_history_diff AS
$_$
DECLARE
    res powa_statements_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.calls = a.calls - b.calls;
    res.total_time = a.total_time - b.total_time;
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

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR - (
    PROCEDURE = powa_statements_history_mi,
    LEFTARG = powa_statements_history_record,
    RIGHTARG = powa_statements_history_record
);

CREATE TYPE powa_statements_history_rate AS (
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
    blk_write_time_per_sec double precision
);

CREATE OR REPLACE FUNCTION powa_statements_history_div(
    a powa_statements_history_record,
    b powa_statements_history_record)
RETURNS powa_statements_history_rate AS
$_$
DECLARE
    res powa_statements_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.calls_per_sec = (a.calls - b.calls)::double precision / sec;
    res.runtime_per_sec = (a.total_time - b.total_time)::double precision / sec;
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

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR / (
    PROCEDURE = powa_statements_history_div,
    LEFTARG = powa_statements_history_record,
    RIGHTARG = powa_statements_history_record
);
/* end of pg_stat_statements operator support */

CREATE TYPE powa_user_functions_history_record AS (
    ts timestamp with time zone,
    calls bigint,
    total_time double precision,
    self_time double precision
);

/* pg_stat_user_functions operator support */
CREATE TYPE powa_user_functions_history_diff AS (
    intvl interval,
    calls bigint,
    total_time double precision,
    self_time double precision

);

CREATE OR REPLACE FUNCTION powa_user_functions_history_mi(
    a powa_user_functions_history_record,
    b powa_user_functions_history_record)
RETURNS powa_user_functions_history_diff AS
$_$
DECLARE
    res powa_user_functions_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.calls = a.calls - b.calls;
    res.total_time = a.total_time - b.total_time;
    res.self_time = a.self_time - b.self_time;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR - (
    PROCEDURE = powa_user_functions_history_mi,
    LEFTARG = powa_user_functions_history_record,
    RIGHTARG = powa_user_functions_history_record
);

CREATE TYPE powa_user_functions_history_rate AS (
    sec integer,
    calls_per_sec double precision,
    total_time_per_sec double precision,
    self_time_per_sec double precision
);

CREATE OR REPLACE FUNCTION powa_user_functions_history_div(
    a powa_user_functions_history_record,
    b powa_user_functions_history_record)
RETURNS powa_user_functions_history_rate AS
$_$
DECLARE
    res powa_user_functions_history_rate;
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

CREATE OPERATOR / (
    PROCEDURE = powa_user_functions_history_div,
    LEFTARG = powa_user_functions_history_record,
    RIGHTARG = powa_user_functions_history_record
);
/* end of pg_stat_user_functions operator support */

CREATE TYPE powa_all_relations_history_record AS (
    ts timestamp with time zone,
    numscan bigint,
    tup_returned bigint,
    tup_fetched bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    n_liv_tup bigint,
    n_dead_tup bigint,
    n_mod_since_analyze bigint,
    blks_read bigint,
    blks_hit bigint,
    last_vacuum timestamp with time zone,
    vacuum_count bigint,
    last_autovacuum timestamp with time zone,
    autovacuum_count bigint,
    last_analyze timestamp with time zone,
    analyze_count bigint,
    last_autoanalyze timestamp with time zone,
    autoanalyze_count bigint
);

/* pg_stat_all_relations operator support */
CREATE TYPE powa_all_relations_history_diff AS (
    intvl interval,
    numscan bigint,
    tup_returned bigint,
    tup_fetched bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    n_liv_tup bigint,
    n_dead_tup bigint,
    n_mod_since_analyze bigint,
    blks_read bigint,
    blks_hit bigint,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint
);

CREATE OR REPLACE FUNCTION powa_all_relations_history_mi(
    a powa_all_relations_history_record,
    b powa_all_relations_history_record)
RETURNS powa_all_relations_history_diff AS
$_$
DECLARE
    res powa_all_relations_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.numscan = a.numscan - b.numscan;
    res.tup_returned = a.tup_returned - b.tup_returned;
    res.tup_fetched = a.tup_fetched - b.tup_fetched;
    res.n_tup_ins = a.n_tup_ins - b.n_tup_ins;
    res.n_tup_upd = a.n_tup_upd - b.n_tup_upd;
    res.n_tup_del = a.n_tup_del - b.n_tup_del;
    res.n_tup_hot_upd = a.n_tup_hot_upd - b.n_tup_hot_upd;
    res.n_liv_tup = a.n_liv_tup - b.n_liv_tup;
    res.n_dead_tup = a.n_dead_tup - b.n_dead_tup;
    res.n_mod_since_analyze = a.n_mod_since_analyze - b.n_mod_since_analyze;
    res.blks_read = a.blks_read - b.blks_read;
    res.blks_hit = a.blks_hit - b.blks_hit;
    res.vacuum_count = a.vacuum_count - b.vacuum_count;
    res.autovacuum_count = a.autovacuum_count - b.autovacuum_count;
    res.analyze_count = a.analyze_count - b.analyze_count;
    res.autoanalyze_count = a.autoanalyze_count - b.autoanalyze_count;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR - (
    PROCEDURE = powa_all_relations_history_mi,
    LEFTARG = powa_all_relations_history_record,
    RIGHTARG = powa_all_relations_history_record
);

CREATE TYPE powa_all_relations_history_rate AS (
    sec integer,
    numscan_per_sec double precision,
    tup_returned_per_sec double precision,
    tup_fetched_per_sec double precision,
    n_tup_ins_per_sec double precision,
    n_tup_upd_per_sec double precision,
    n_tup_del_per_sec double precision,
    n_tup_hot_upd_per_sec double precision,
    n_liv_tup_per_sec double precision,
    n_dead_tup_per_sec double precision,
    n_mod_since_analyze_per_sec double precision,
    blks_read_per_sec double precision,
    blks_hit_per_sec double precision,
    vacuum_count_per_sec double precision,
    autovacuum_count_per_sec double precision,
    analyze_count_per_sec double precision,
    autoanalyze_count_per_sec double precision
);

CREATE OR REPLACE FUNCTION powa_all_relations_history_div(
    a powa_all_relations_history_record,
    b powa_all_relations_history_record)
RETURNS powa_all_relations_history_rate AS
$_$
DECLARE
    res powa_all_relations_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.numscan_per_sec = (a.numscan - b.numscan)::double precision / sec;
    res.tup_returned_per_sec = (a.tup_returned - b.tup_returned)::double precision / sec;
    res.tup_fetched_per_sec = (a.tup_fetched - b.tup_fetched)::double precision / sec;
    res.n_tup_ins_per_sec = (a.n_tup_ins - b.n_tup_ins)::double precision / sec;
    res.n_tup_upd_per_sec = (a.n_tup_upd - b.n_tup_upd)::double precision / sec;
    res.n_tup_del_per_sec = (a.n_tup_del - b.n_tup_del)::double precision / sec;
    res.n_tup_hot_upd_per_sec = (a.n_tup_hot_upd - b.n_tup_hot_upd)::double precision / sec;
    res.n_liv_tup_per_sec = (a.n_liv_tup - b.n_liv_tup)::double precision / sec;
    res.n_dead_tup_per_sec = (a.n_dead_tup - b.n_dead_tup)::double precision / sec;
    res.n_mod_since_analyze_per_sec = (a.n_mod_since_analyze - b.n_mod_since_analyze)::double precision / sec;
    res.blks_read_per_sec = (a.blks_read - b.blks_read)::double precision / sec;
    res.blks_hit_per_sec = (a.blks_hit - b.blks_hit)::double precision / sec;
    res.vacuum_count_per_sec = (a.vacuum_count - b.vacuum_count)::double precision / sec;
    res.autovacuum_count_per_sec = (a.autovacuum_count - b.autovacuum_count)::double precision / sec;
    res.analyze_count_per_sec = (a.analyze_count - b.analyze_count)::double precision / sec;
    res.autoanalyze_count_per_sec = (a.autoanalyze_count - b.autoanalyze_count)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR / (
    PROCEDURE = powa_all_relations_history_div,
    LEFTARG = powa_all_relations_history_record,
    RIGHTARG = powa_all_relations_history_record
);
/* end of pg_stat_all_relations operator support */


CREATE UNLOGGED TABLE public.powa_databases_src_tmp(
    srvid integer NOT NULL,
    oid oid NOT NULL,
    datname name NOT NULL
);

CREATE UNLOGGED TABLE public.powa_statements_src_tmp (
    srvid integer NOT NULL,
    ts  timestamp with time zone NOT NULL,
    userid oid NOT NULL,
    dbid oid NOT NULL,
    queryid bigint NOT NULL,
    query text NOT NULL,
    calls bigint NOT NULL,
    total_time double precision NOT NULL,
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
    blk_write_time double precision NOT NULL
);

CREATE UNLOGGED TABLE public.powa_user_functions_src_tmp(
    srvid integer NOT NULL,
    ts  timestamp with time zone NOT NULL,
    dbid oid NOT NULL,
    funcid oid NOT NULL,
    calls bigint NOT NULL,
    total_time double precision NOT NULL,
    self_time double precision NOT NULL
);

CREATE UNLOGGED TABLE public.powa_all_relations_src_tmp(
    srvid integer NOT NULL,
    ts  timestamp with time zone NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    numscan bigint NOT NULL,
    tup_returned bigint NOT NULL,
    tup_fetched bigint NOT NULL,
    n_tup_ins bigint NOT NULL,
    n_tup_upd bigint NOT NULL,
    n_tup_del bigint NOT NULL,
    n_tup_hot_upd bigint NOT NULL,
    n_liv_tup bigint NOT NULL,
    n_dead_tup bigint NOT NULL,
    n_mod_since_analyze bigint NOT NULL,
    blks_read bigint NOT NULL,
    blks_hit bigint NOT NULL,
    last_vacuum timestamp with time zone,
    vacuum_count bigint NOT NULL,
    last_autovacuum timestamp with time zone,
    autovacuum_count bigint NOT NULL,
    last_analyze timestamp with time zone,
    analyze_count bigint NOT NULL,
    last_autoanalyze timestamp with time zone,
    autoanalyze_count bigint NOT NULL
);

CREATE TABLE powa_statements_history (
    srvid integer NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_statements_history_record[] NOT NULL,
    mins_in_range powa_statements_history_record NOT NULL,
    maxs_in_range powa_statements_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX powa_statements_history_query_ts ON powa_statements_history USING gist (srvid, queryid, coalesce_range);

CREATE TABLE powa_statements_history_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_statements_history_record[] NOT NULL,
    mins_in_range powa_statements_history_record NOT NULL,
    maxs_in_range powa_statements_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX powa_statements_history_db_ts ON powa_statements_history_db USING gist (srvid, dbid, coalesce_range);

CREATE TABLE powa_statements_history_current (
    srvid integer NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    record powa_statements_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_statements_history_current(srvid);

CREATE TABLE powa_statements_history_current_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    record powa_statements_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_statements_history_current_db(srvid);

CREATE TABLE powa_user_functions_history (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    funcid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_user_functions_history_record[] NOT NULL,
    mins_in_range powa_user_functions_history_record NOT NULL,
    maxs_in_range powa_user_functions_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX powa_user_functions_history_funcid_ts ON powa_user_functions_history USING gist (srvid, funcid, coalesce_range);

CREATE TABLE powa_user_functions_history_current (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    funcid oid NOT NULL,
    record powa_user_functions_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_user_functions_history_current(srvid);

CREATE TABLE powa_all_relations_history (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_all_relations_history_record[] NOT NULL,
    mins_in_range powa_all_relations_history_record NOT NULL,
    maxs_in_range powa_all_relations_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX powa_all_relations_history_relid_ts ON powa_all_relations_history USING gist (srvid, relid, coalesce_range);

CREATE TABLE powa_all_relations_history_current (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    record powa_all_relations_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_all_relations_history_current(srvid);


CREATE TABLE powa_functions (
    srvid integer NOT NULL,
    module text NOT NULL,
    operation text NOT NULL,
    function_name text NOT NULL,
    query_source text default NULL,
    added_manually boolean NOT NULL default true,
    enabled boolean NOT NULL default true,
    priority numeric NOT NULL default 10,
    CHECK (operation IN ('snapshot','aggregate','purge','unregister','reset')),
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO powa_functions (srvid, module, operation, function_name, query_source, added_manually, enabled, priority) VALUES
    (0, 'pg_stat_statements',       'snapshot',  'powa_databases_snapshot',       'powa_databases_src',      false, true, -3),
    (0, 'pg_stat_statements',       'snapshot',  'powa_statements_snapshot',      'powa_statements_src',     false, true, -2),
    (0, 'powa_stat_user_functions', 'snapshot',  'powa_user_functions_snapshot',  'powa_user_functions_src', false, true, default),
    (0, 'powa_stat_all_relations',  'snapshot',  'powa_all_relations_snapshot',   'powa_all_relations_src',  false, true, default),
    (0, 'pg_stat_statements',       'aggregate', 'powa_statements_aggregate',     NULL,                      false, true, default),
    (0, 'powa_stat_user_functions', 'aggregate', 'powa_user_functions_aggregate', NULL,                      false, true, default),
    (0, 'powa_stat_all_relations',  'aggregate', 'powa_all_relations_aggregate',  NULL,                      false, true, default),
    (0, 'pg_stat_statements',       'purge',     'powa_statements_purge',         NULL,                      false, true, default),
    (0, 'pg_stat_statements',       'purge',     'powa_databases_purge',          NULL,                      false, true, default),
    (0, 'powa_stat_user_functions', 'purge',     'powa_user_functions_purge',     NULL,                      false, true, default),
    (0, 'powa_stat_all_relations',  'purge',     'powa_all_relations_purge',      NULL,                      false, true, default),
    (0, 'pg_stat_statements',       'reset',     'powa_statements_reset',         NULL,                      false, true, default),
    (0, 'powa_stat_user_functions', 'reset',     'powa_user_functions_reset',     NULL,                      false, true, default),
    (0, 'powa_stat_all_relations',  'reset',     'powa_all_relations_reset',      NULL,                      false, true, default);

-- Register the extension if needed, and set the enabled flag to on
CREATE FUNCTION powa_activate_extension(_srvid integer, _extname text) RETURNS boolean
AS $_$
DECLARE
    v_ext_registered boolean;
BEGIN
    SELECT COUNT(*) > 0 INTO v_ext_registered
    FROM powa_functions
    WHERE module = _extname
    AND srvid = _srvid;

    -- the rows may already be present, but the enabled flag could be off,
    -- so enabled it everywhere it's disabled.  We don't check for other cases,
    -- for instance if part of the needed rows were deleted.
    IF (v_ext_registered) THEN
        UPDATE powa_functions
        SET enabled = true
        WHERE enabled = false
        AND srvid = _srvid
        AND extname = _extname;

        return true;
    END IF;

    IF (_extname = 'pg_stat_statements') THEN
        INSERT INTO powa_functions(srvid, module, operation, function_name,
            query_source, added_manually, enabled, priority)
        VALUES
        (_srvid, 'pg_stat_statements', 'snapshot',  'powa_databases_snapshot',    'powa_databases_src',  true, true, -1),
        (_srvid, 'pg_stat_statements', 'snapshot',  'powa_statements_snapshot',  'powa_statements_src', true, true, default),
        (_srvid, 'pg_stat_statements', 'aggregate', 'powa_statements_aggregate', NULL,                  true, true, default),
        (_srvid, 'pg_stat_statements', 'purge',     'powa_statements_purge',     NULL,                  true, true, default),
        (_srvid, 'pg_stat_statements', 'purge',     'powa_databases_purge',      NULL,                  true, true, default),
        (_srvid, 'pg_stat_statements', 'reset',     'powa_statements_reset',     NULL,                  true, true, default);
    ELSIF (_extname = 'powa_stat_user_functions') THEN
        INSERT INTO powa_functions(srvid, module, operation, function_name,
            query_source, added_manually, enabled, priority)
        VALUES
         (_srvid, 'powa_stat_user_functions', 'snapshot',  'powa_user_functions_snapshot',  'powa_user_functions_src', false, true, default),
         (_srvid, 'powa_stat_user_functions', 'aggregate', 'powa_user_functions_aggregate', NULL,                      false, true, default),
         (_srvid, 'powa_stat_user_functions', 'purge',     'powa_user_functions_purge',     NULL,                      false, true, default),
         (_srvid, 'powa_stat_user_functions', 'reset',     'powa_user_functions_reset',     NULL,                      false, true, default);
    ELSIF (_extname = 'powa_stat_all_relations') THEN
        INSERT INTO powa_functions(srvid, module, operation, function_name,
            query_source, added_manually, enabled, priority)
        VALUES
        (_srvid, 'powa_stat_all_relations',  'snapshot',  'powa_all_relations_snapshot',   'powa_all_relations_src',  false, true, default),
        (_srvid, 'powa_stat_all_relations',  'aggregate', 'powa_all_relations_aggregate',  NULL,                      false, true, default),
        (_srvid, 'powa_stat_all_relations',  'purge',     'powa_all_relations_purge',      NULL,                      false, true, default),
        (_srvid, 'powa_stat_all_relations',  'reset',     'powa_all_relations_reset',      NULL,                      false, true, default);
    ELSIF (_extname = 'pg_stat_kcache') THEN
        RETURN powa_kcache_register(_srvid);
    ELSIF (_extname = 'pg_qualstats') THEN
        RETURN powa_qualstats_register(_srvid);
    ELSIF (_extname = 'pg_wait_sampling') THEN
        RETURN powa_wait_sampling_register(_srvid);
    ELSIF (_extname = 'pg_track_settings') THEN
        RETURN powa_track_settings_register(_srvid);
    ELSE
        return false;
    END IF;

    return true;
END;
$_$ LANGUAGE plpgsql; /* end of powa_activate_extension */

-- Register the extension if needed, and set the enabled flag to on
CREATE FUNCTION powa_deactivate_extension(_srvid integer, _extname text) RETURNS boolean
AS $_$
BEGIN
    UPDATE powa_functions
    SET enabled = false
    WHERE module = _extname;

    return true;
END;
$_$ LANGUAGE plpgsql; /* end of powa_deactivate_extension */

CREATE FUNCTION powa_register_server(hostname text,
    port integer DEFAULT 5432,
    alias text DEFAULT NULL,
    username text DEFAULT 'powa',
    password text DEFAULT NULL,
    dbname text DEFAULT 'powa',
    frequency integer DEFAULT 300,
    retention interval DEFAULT '1 day'::interval,
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
        coalesce(retention, '1 day')::interval
    INTO port, username, dbname, frequency, retention;

    INSERT INTO powa_servers
        (alias, hostname, port, username, password, dbname, frequency, retention)
    VALUES
        (alias, hostname, port, username, password, dbname, frequency, retention)
    RETURNING id INTO v_srvid;

    INSERT INTO powa_snapshot_metas(srvid) VALUES (v_srvid);

    -- always register pgss, as it's mandatory
    SELECT powa_activate_extension(v_srvid, 'pg_stat_statements') INTO v_ok;
    IF (NOT v_ok) THEN
        RAISE EXCEPTION 'Could not activate pg_stat_statements';
    END IF;
    -- and also the powa stats functions
    SELECT powa_activate_extension(v_srvid, 'powa_stat_user_functions') INTO v_ok;
    IF (NOT v_ok) THEN
        RAISE EXCEPTION 'Could not activate pg_stat_statements';
    END IF;
    SELECT powa_activate_extension(v_srvid, 'powa_stat_all_relations') INTO v_ok;
    IF (NOT v_ok) THEN
        RAISE EXCEPTION 'Could not activate pg_stat_statements';
    END IF;

    -- If no extra extensions were asked, we're done
    IF extensions IS NULL THEN
        RETURN true;
    END IF;

    FOREACH v_extname IN ARRAY extensions
    LOOP
        IF (v_extname != 'pg_stat_statements') THEN
            SELECT powa_activate_extension(v_srvid, v_extname) INTO v_ok;

            IF (NOT v_ok) THEN
                RAISE WARNING 'Could not activate extension % on server %:%',
                    v_extname, hostname, port;
            END IF;
        END IF;
    END LOOP;
    RETURN true;
END;
$_$ LANGUAGE plpgsql; /* end of powa_register_server */

CREATE FUNCTION powa_log (msg text) RETURNS void
LANGUAGE plpgsql
AS $_$
BEGIN
    IF current_setting('powa.debug')::bool THEN
        RAISE WARNING '%', msg;
    ELSE
        RAISE DEBUG '%', msg;
    END IF;
END;
$_$;

CREATE FUNCTION powa_get_server_retention(_srvid integer)
RETURNS interval AS $_$
DECLARE
    v_ret interval = NULL;
BEGIN
    IF (_srvid = 0) THEN
        v_ret := current_setting('powa.retention')::interval;
    ELSE
        SELECT retention INTO v_ret
        FROM powa_servers
        WHERE id = _srvid;
    END IF;

    IF (v_ret IS NULL) THEN
        RAISE EXCEPTION 'Not retention found for server %', _srvid;
    END IF;

    RETURN v_ret;
END;
$_$ LANGUAGE plpgsql; /* end of powa_get_server_retention */

/* pg_stat_kcache integration - part 1 */

CREATE UNLOGGED TABLE public.powa_kcache_src_tmp(
    srvid integer NOT NULL,
    ts timestamp with time zone NOT NULL,
    queryid bigint NOT NULL,
    userid oid NOT NULL,
    dbid oid NOT NULL,
    reads bigint NOT NULL,
    writes bigint NOT NULL,
    user_time double precision NOT NULL,
    system_time double precision NOT NULL,
    minflts     bigint NOT NULL,
    majflts     bigint NOT NULL,
    nswaps      bigint NOT NULL,
    msgsnds     bigint NOT NULL,
    msgrcvs     bigint NOT NULL,
    nsignals    bigint NOT NULL,
    nvcsws      bigint NOT NULL,
    nivcsws     bigint NOT NULL
);

CREATE TYPE public.powa_kcache_type AS (
    ts timestamptz,
    reads bigint,                   /* total reads, in bytes */
    writes bigint,                  /* total writes, in bytes */
    user_time double precision,     /* total user CPU time used */
    system_time double precision,   /* total system CPU time used */
    minflts     bigint,             /* total page reclaims (soft page faults) */
    majflts     bigint,             /* total page faults (hard page faults) */
    nswaps      bigint,             /* total swaps */
    msgsnds     bigint,             /* total IPC messages sent */
    msgrcvs     bigint,             /* total IPC messages received */
    nsignals    bigint,             /* total signals received */
    nvcsws      bigint,             /* total voluntary context switches */
    nivcsws     bigint              /* total involuntary context switches */
);

/* pg_stat_kcache operator support */
CREATE TYPE powa_kcache_diff AS (
    intvl interval,
    reads bigint,
    writes bigint,
    user_time double precision,
    system_time double precision,
    minflts     bigint,
    majflts     bigint,
    nswaps      bigint,
    msgsnds     bigint,
    msgrcvs     bigint,
    nsignals    bigint,
    nvcsws      bigint,
    nivcsws     bigint
);

CREATE OR REPLACE FUNCTION powa_kcache_mi(
    a powa_kcache_type,
    b powa_kcache_type)
RETURNS powa_kcache_diff AS
$_$
DECLARE
    res powa_kcache_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.reads = a.reads - b.reads;
    res.writes = a.writes - b.writes;
    res.user_time = a.user_time - b.user_time;
    res.system_time = a.system_time - b.system_time;
    res.minflts = a.minflts - b.minflts;
    res.majflts = a.majflts - b.majflts;
    res.nswaps = a.nswaps - b.nswaps;
    res.msgsnds = a.msgsnds - b.msgsnds;
    res.msgrcvs = a.msgrcvs - b.msgrcvs;
    res.nsignals = a.nsignals - b.nsignals;
    res.nvcsws = a.nvcsws - b.nvcsws;
    res.nivcsws = a.nivcsws - b.nivcsws;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR - (
    PROCEDURE = powa_kcache_mi,
    LEFTARG = powa_kcache_type,
    RIGHTARG = powa_kcache_type
);

CREATE TYPE powa_kcache_rate AS (
    sec integer,
    reads_per_sec double precision,
    writes_per_sec double precision,
    user_time_per_sec double precision,
    system_time_per_sec double precision,
    minflts_per_sec     bigint,
    majflts_per_sec     bigint,
    nswaps_per_sec      bigint,
    msgsnds_per_sec     bigint,
    msgrcvs_per_sec     bigint,
    nsignals_per_sec    bigint,
    nvcsws_per_sec      bigint,
    nivcsws_per_sec     bigint
);

CREATE OR REPLACE FUNCTION powa_kcache_div(
    a powa_kcache_type,
    b powa_kcache_type)
RETURNS powa_kcache_rate AS
$_$
DECLARE
    res powa_kcache_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.reads_per_sec = (a.reads - b.reads)::double precision / sec;
    res.writes_per_sec = (a.writes - b.writes)::double precision / sec;
    res.user_time_per_sec = (a.user_time - b.user_time)::double precision / sec;
    res.system_time_per_sec = (a.system_time - b.system_time)::double precision / sec;
    res.minflts_per_sec = (a.minflts_per_sec - b.minflts_per_sec)::bigint / sec;
    res.majflts_per_sec = (a.majflts_per_sec - b.majflts_per_sec)::bigint / sec;
    res.nswaps_per_sec = (a.nswaps_per_sec - b.nswaps_per_sec)::bigint / sec;
    res.msgsnds_per_sec = (a.msgsnds_per_sec - b.msgsnds_per_sec)::bigint / sec;
    res.msgrcvs_per_sec = (a.msgrcvs_per_sec - b.msgrcvs_per_sec)::bigint / sec;
    res.nsignals_per_sec = (a.nsignals_per_sec - b.nsignals_per_sec)::bigint / sec;
    res.nvcsws_per_sec = (a.nvcsws_per_sec - b.nvcsws_per_sec)::bigint / sec;
    res.nivcsws_per_sec = (a.nivcsws_per_sec - b.nivcsws_per_sec)::bigint / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR / (
    PROCEDURE = powa_kcache_div,
    LEFTARG = powa_kcache_type,
    RIGHTARG = powa_kcache_type
);

/* end of pg_stat_kcache operator support */

CREATE TABLE public.powa_kcache_metrics (
    srvid integer NOT NULL,
    coalesce_range tstzrange NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    metrics public.powa_kcache_type[] NOT NULL,
    mins_in_range public.powa_kcache_type NOT NULL,
    maxs_in_range public.powa_kcache_type NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, queryid, dbid, userid),
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX ON public.powa_kcache_metrics (srvid, queryid);

CREATE TABLE public.powa_kcache_metrics_db (
    srvid integer NOT NULL,
    coalesce_range tstzrange NOT NULL,
    dbid oid NOT NULL,
    metrics public.powa_kcache_type[] NOT NULL,
    mins_in_range public.powa_kcache_type NOT NULL,
    maxs_in_range public.powa_kcache_type NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, dbid),
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE public.powa_kcache_metrics_current (
    srvid integer NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    metrics powa_kcache_type NULL NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_kcache_metrics_current(srvid);

CREATE TABLE public.powa_kcache_metrics_current_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    metrics powa_kcache_type NULL NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_kcache_metrics_current_db(srvid);

/* end of pg_stat_kcache integration - part 1 */

/* pg_qualstats integration - part 1 */

CREATE TYPE public.qual_type AS (
    relid oid,
    attnum integer,
    opno oid,
    eval_type "char"
);

CREATE TYPE public.qual_values AS (
    constants text[],
    occurences bigint,
    execution_count bigint,
    nbfiltered bigint
);

CREATE TYPE powa_qualstats_history_item AS (
  ts timestamptz,
  occurences bigint,
  execution_count bigint,
  nbfiltered bigint
);

CREATE UNLOGGED TABLE public.powa_qualstats_src_tmp(
    srvid integer NOT NULL,
    ts timestamp with time zone NOT NULL,
    uniquequalnodeid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    qualnodeid bigint NOT NULL,
    occurences bigint NOT NULL,
    execution_count bigint NOT NULL,
    nbfiltered bigint NOT NULL,
    queryid bigint NOT NULL,
    constvalues varchar[] NOT NULL,
    quals qual_type[] NOT NULL
);

/* pg_stat_qualstats operator support */
CREATE TYPE powa_qualstats_history_diff AS (
    intvl interval,
    occurences bigint,
    execution_count bigint,
    nbfiltered bigint
);

CREATE OR REPLACE FUNCTION powa_qualstats_history_mi(
    a powa_qualstats_history_item,
    b powa_qualstats_history_item)
RETURNS powa_qualstats_history_diff AS
$_$
DECLARE
    res powa_qualstats_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.occurences = a.occurences - b.occurences;
    res.execution_count = a.execution_count - b.execution_count;
    res.nbfiltered = a.nbfiltered - b.nbfiltered;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR - (
    PROCEDURE = powa_qualstats_history_mi,
    LEFTARG = powa_qualstats_history_item,
    RIGHTARG = powa_qualstats_history_item
);

CREATE TYPE powa_qualstats_history_rate AS (
    sec integer,
    occurences_per_sec double precision,
    execution_count_per_sec double precision,
    nbfiltered_per_sec double precision
);

CREATE OR REPLACE FUNCTION powa_qualstats_history_div(
    a powa_qualstats_history_item,
    b powa_qualstats_history_item)
RETURNS powa_qualstats_history_rate AS
$_$
DECLARE
    res powa_qualstats_history_rate;
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

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR / (
    PROCEDURE = powa_qualstats_history_div,
    LEFTARG = powa_qualstats_history_item,
    RIGHTARG = powa_qualstats_history_item
);
/* end of pg_stat_qualstats operator support */

CREATE TABLE public.powa_qualstats_quals (
    srvid integer NOT NULL,
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    quals public.qual_type[],
    PRIMARY KEY (srvid, qualid, queryid, dbid, userid),
    FOREIGN KEY (srvid, queryid, dbid, userid) REFERENCES powa_statements(srvid, queryid, dbid, userid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_qualstats_quals(srvid, queryid);

CREATE TABLE public.powa_qualstats_quals_history (
    srvid integer NOT NULL,
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    coalesce_range tstzrange,
    records powa_qualstats_history_item[],
    mins_in_range powa_qualstats_history_item,
    maxs_in_range powa_qualstats_history_item,
    FOREIGN KEY (srvid, qualid, queryid, dbid, userid) REFERENCES public.powa_qualstats_quals (srvid, qualid, queryid, dbid, userid) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX powa_qualstats_quals_history_query_ts ON powa_qualstats_quals_history USING gist (srvid, queryid, coalesce_range);

CREATE TABLE public.powa_qualstats_quals_history_current (
    srvid integer NOT NULL,
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    ts timestamptz,
    occurences bigint,
    execution_count   bigint,
    nbfiltered bigint,
    FOREIGN KEY (srvid, qualid, queryid, dbid, userid) REFERENCES powa_qualstats_quals(srvid, qualid, queryid, dbid, userid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_qualstats_quals_history_current(srvid);

CREATE TABLE public.powa_qualstats_constvalues_history (
    srvid integer NOT NULL,
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    coalesce_range tstzrange,
    most_used qual_values[],
    most_filtering qual_values[],
    least_filtering qual_values[],
    most_executed qual_values[],
    FOREIGN KEY (srvid, qualid, queryid, dbid, userid) REFERENCES public.powa_qualstats_quals (srvid, qualid, queryid, dbid, userid) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_qualstats_constvalues_history USING gist (srvid, queryid, qualid, coalesce_range);
CREATE INDEX ON powa_qualstats_constvalues_history (srvid, qualid, queryid);

CREATE TABLE public.powa_qualstats_constvalues_history_current (
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
    FOREIGN KEY (srvid, qualid, queryid, dbid, userid) REFERENCES public.powa_qualstats_quals (srvid, qualid, queryid, dbid, userid) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_qualstats_constvalues_history_current(srvid);


/* end of pg_qualstats_integration - part 1 */

/* pg_wait_sampling integration - part 1 */

CREATE UNLOGGED TABLE public.powa_wait_sampling_src_tmp(
    srvid integer NOT NULL,
    ts timestamp with time zone NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    queryid bigint NOT NULL,
    count numeric NOT NULL
);

CREATE TYPE public.wait_sampling_type AS (
    ts timestamptz,
    count bigint
);

/* pg_wait_sampling operator support */
CREATE TYPE wait_sampling_diff AS (
    intvl interval,
    count bigint
);

CREATE OR REPLACE FUNCTION wait_sampling_mi(
    a wait_sampling_type,
    b wait_sampling_type)
RETURNS wait_sampling_diff AS
$_$
DECLARE
    res wait_sampling_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.count = a.count - b.count;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR - (
    PROCEDURE = wait_sampling_mi,
    LEFTARG = wait_sampling_type,
    RIGHTARG = wait_sampling_type
);

CREATE TYPE wait_sampling_rate AS (
    sec integer,
    count_per_sec double precision
);

CREATE OR REPLACE FUNCTION wait_sampling_div(
    a wait_sampling_type,
    b wait_sampling_type)
RETURNS wait_sampling_rate AS
$_$
DECLARE
    res wait_sampling_rate;
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

CREATE OPERATOR / (
    PROCEDURE = wait_sampling_div,
    LEFTARG = wait_sampling_type,
    RIGHTARG = wait_sampling_type
);

/* end of pg_wait_sampling operator support */

CREATE TABLE public.powa_wait_sampling_history (
    srvid integer NOT NULL REFERENCES powa_servers(id),
    coalesce_range tstzrange NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    records public.wait_sampling_type[] NOT NULL,
    mins_in_range public.wait_sampling_type NOT NULL,
    maxs_in_range public.wait_sampling_type NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, queryid, dbid, event_type, event)
);

CREATE INDEX powa_wait_sampling_history_query_ts ON public.powa_wait_sampling_history USING gist (srvid, queryid, coalesce_range);

CREATE TABLE public.powa_wait_sampling_history_db (
    srvid integer NOT NULL REFERENCES powa_servers(id),
    coalesce_range tstzrange NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    records public.wait_sampling_type[] NOT NULL,
    mins_in_range public.wait_sampling_type NOT NULL,
    maxs_in_range public.wait_sampling_type NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, dbid, event_type, event)
);

CREATE INDEX powa_wait_sampling_history_db_ts ON powa_wait_sampling_history_db USING gist (srvid, dbid, coalesce_range);

CREATE TABLE public.powa_wait_sampling_history_current (
    srvid integer NOT NULL REFERENCES powa_servers(id),
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    record wait_sampling_type NOT NULL
);
CREATE INDEX ON powa_wait_sampling_history_current(srvid);

CREATE TABLE public.powa_wait_sampling_history_current_db (
    srvid integer NOT NULL REFERENCES powa_servers(id),
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    record wait_sampling_type NOT NULL
);
CREATE INDEX ON powa_wait_sampling_history_current_db(srvid);

/* end of pg_wait_sampling integration - part 1 */

-- Mark all of powa's tables as "to be dumped"
SELECT pg_catalog.pg_extension_config_dump('powa_statements','');
SELECT pg_catalog.pg_extension_config_dump('powa_statements_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_statements_history_db','');
SELECT pg_catalog.pg_extension_config_dump('powa_statements_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_statements_history_current_db','');
SELECT pg_catalog.pg_extension_config_dump('powa_user_functions_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_user_functions_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_all_relations_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_all_relations_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_functions','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('powa_kcache_metrics','');
SELECT pg_catalog.pg_extension_config_dump('powa_kcache_metrics_db','');
SELECT pg_catalog.pg_extension_config_dump('powa_kcache_metrics_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_kcache_metrics_current_db','');
SELECT pg_catalog.pg_extension_config_dump('powa_qualstats_quals','');
SELECT pg_catalog.pg_extension_config_dump('powa_qualstats_quals_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_qualstats_quals_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_qualstats_constvalues_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_qualstats_constvalues_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_wait_sampling_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_wait_sampling_history_db','');
SELECT pg_catalog.pg_extension_config_dump('powa_wait_sampling_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_wait_sampling_history_current_db','');

CREATE OR REPLACE FUNCTION public.powa_check_created_extensions()
RETURNS event_trigger
LANGUAGE plpgsql
AS $_$
DECLARE
BEGIN
    -- in case the extension is not in shared_preload_libraries, we don't want
    -- to fail on powa.debug GUC not being present
    LOAD 'powa';

    /* We have for now no way for a proper handling of this event,
     * as we don't have a table with the list of supported extensions.
     * So just call every powa_*_register() function we know each time an
     * extension is created. Powa should be in a dedicated database and the
     * register function handle to be called several time, so it's not critical
     */
    PERFORM public.powa_kcache_register();
    PERFORM public.powa_qualstats_register();
    PERFORM public.powa_track_settings_register();
    PERFORM public.powa_wait_sampling_register();
END;
$_$; /* end of powa_check_created_extensions */

CREATE EVENT TRIGGER powa_check_created_extensions
    ON ddl_command_end
    WHEN tag IN ('CREATE EXTENSION')
    EXECUTE PROCEDURE public.powa_check_created_extensions() ;

CREATE OR REPLACE FUNCTION public.powa_check_dropped_extensions()
RETURNS event_trigger
LANGUAGE plpgsql
AS $_$
DECLARE
    funcname text;
    v_state   text;
    v_msg     text;
    v_detail  text;
    v_hint    text;
    v_context text;
BEGIN
    -- in case the extension is not in shared_preload_libraries, we don't want
    -- to fail on powa.debug GUC not being present
    LOAD 'powa';

    -- We unregister extensions regardless the "enabled" field
    WITH ext AS (
        SELECT object_name
        FROM pg_event_trigger_dropped_objects() d
        WHERE d.object_type = 'extension'
    )
    SELECT function_name INTO funcname
    FROM powa_functions f
    JOIN ext ON f.module = ext.object_name
    WHERE operation = 'unregister'
    ORDER BY module;

    IF ( funcname IS NOT NULL ) THEN
        BEGIN
            PERFORM powa_log(format('running %I', funcname));
            EXECUTE 'SELECT ' || quote_ident(funcname) || '()';
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;
            RAISE WARNING 'powa_check_dropped_extensions(): function "%" failed:
                state  : %
                message: %
                detail : %
                hint   : %
                context: %', funcname, v_state, v_msg, v_detail, v_hint, v_context;
        END;
    END IF;
END;
$_$; /* end of powa_check_dropped_extensions */

CREATE EVENT TRIGGER powa_check_dropped_extensions
    ON sql_drop
    WHEN tag IN ('DROP EXTENSION')
    EXECUTE PROCEDURE public.powa_check_dropped_extensions() ;

CREATE OR REPLACE FUNCTION powa_prevent_concurrent_snapshot(_srvid integer = 0)
RETURNS void
AS $PROC$
BEGIN
    BEGIN
        PERFORM 1
        FROM powa_snapshot_metas
        WHERE srvid = _srvid
        FOR UPDATE NOWAIT;
    EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Could not lock the powa_snapshot_metas record, '
        'a concurrent snapshot is probably running';
    END;
END;
$PROC$ language plpgsql; /* end of powa_prevent_concurrent_snapshot() */

CREATE OR REPLACE FUNCTION powa_take_snapshot(_srvid integer = 0) RETURNS integer
AS $PROC$
DECLARE
  purgets timestamp with time zone;
  purge_seq  bigint;
  funcname   text;
  v_state    text;
  v_msg      text;
  v_detail   text;
  v_hint     text;
  v_context  text;
  v_title    text = 'PoWA - ';
  v_rowcount bigint;
  v_nb_err int = 0;
  v_errs     text[] = '{}';
  v_pattern  text = 'powa_take_snapshot(%s): function "%s" failed:
              state  : %s
              message: %s
              detail : %s
              hint   : %s
              context: %s';
  v_pattern_simple text = 'powa_take_snapshot(%s): function "%s" failed: %s';
BEGIN
    PERFORM set_config('application_name',
        v_title || ' snapshot database list',
        false);
    PERFORM powa_log('start of powa_take_snapshot(' || _srvid || ')');

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    UPDATE powa_snapshot_metas
    SET coalesce_seq = coalesce_seq + 1,
        errors = NULL,
        snapts = now()
    WHERE srvid = _srvid
    RETURNING coalesce_seq INTO purge_seq;

    PERFORM powa_log(format('coalesce_seq(%s): %s', _srvid, purge_seq));

    -- For all enabled snapshot functions in the powa_functions table, execute
    FOR funcname IN SELECT function_name
                 FROM powa_functions
                 WHERE operation='snapshot'
                 AND enabled
                 AND srvid = _srvid
                 ORDER BY priority, module
    LOOP
      -- Call all of them, for the current srvid
      BEGIN
        PERFORM powa_log(format('calling snapshot function: %I', funcname));
        PERFORM set_config('application_name',
            v_title || quote_ident(funcname) || '(' || _srvid || ')', false);

        EXECUTE format('SELECT %I(%s)', funcname, _srvid);
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS
              v_state   = RETURNED_SQLSTATE,
              v_msg     = MESSAGE_TEXT,
              v_detail  = PG_EXCEPTION_DETAIL,
              v_hint    = PG_EXCEPTION_HINT,
              v_context = PG_EXCEPTION_CONTEXT;

          RAISE warning '%', format(v_pattern, _srvid, funcname, v_state, v_msg,
            v_detail, v_hint, v_context);

          v_errs := array_append(v_errs, format(v_pattern_simple, _srvid,
                funcname, v_msg));

          v_nb_err = v_nb_err + 1;
      END;
    END LOOP;

    -- Coalesce datas if needed
    IF ( (purge_seq % current_setting('powa.coalesce')::bigint ) = 0 )
    THEN
      PERFORM powa_log(
        format('coalesce needed, srvid: %s - seq: %s - coalesce seq: %s',
        _srvid, purge_seq, current_setting('powa.coalesce')::bigint ));

      FOR funcname IN SELECT function_name
                   FROM powa_functions
                   WHERE operation='aggregate'
                   AND enabled
                   AND srvid = _srvid
                   ORDER BY module
      LOOP
        -- Call all of them, for the current srvid
        BEGIN
          PERFORM powa_log(format('calling aggregate function: %I(%s)',
                funcname, _srvid));

          PERFORM set_config('application_name',
              v_title || quote_ident(funcname) || '(' || _srvid || ')', false);

          EXECUTE format('SELECT %I(%s)', funcname, _srvid);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;

            RAISE warning '%', format(v_pattern, _srvid, funcname, v_state, v_msg,
              v_detail, v_hint, v_context);

            v_errs := array_append(v_errs, format(v_pattern_simple, _srvid,
                  funcname, v_msg));

            v_nb_err = v_nb_err + 1;
        END;
      END LOOP;

      PERFORM set_config('application_name',
          v_title || 'UPDATE powa_snapshot_metas.aggets',
          false);
      UPDATE powa_snapshot_metas
      SET aggts = now()
      WHERE srvid = _srvid;
    END IF;

    -- We also purge, at the pass after the coalesce
    IF ( (purge_seq % (current_setting('powa.coalesce')::bigint )) = 1 )
    THEN
      PERFORM powa_log(
        format('purge needed, srvid: %s - seq: %s coalesce seq: %s',
        _srvid, purge_seq, current_setting('powa.coalesce')));

      FOR funcname IN SELECT function_name
                   FROM powa_functions
                   WHERE operation='purge'
                   AND enabled
                   AND srvid = _srvid
                   ORDER BY module
      LOOP
        -- Call all of them, for the current srvid
        BEGIN
          PERFORM powa_log(format('calling purge function: %I(%s)',
                funcname, _srvid));
          PERFORM set_config('application_name',
              v_title || quote_ident(funcname) || '(' || _srvid || ')',
              false);

          EXECUTE format('SELECT %I(%s)', funcname, _srvid);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;

            RAISE warning '%', format(v_pattern, _srvid, funcname, v_state, v_msg,
              v_detail, v_hint, v_context);

            v_errs := array_append(v_errs, format(v_pattern_simple, _srvid,
                  funcname, v_msg));

            v_nb_err = v_nb_err + 1;
        END;
      END LOOP;

      PERFORM set_config('application_name',
          v_title || 'UPDATE powa_snapshot_metas.purgets',
          false);
      UPDATE powa_snapshot_metas
      SET purgets = now()
      WHERE srvid = _srvid;
    END IF;

    IF (v_nb_err > 0) THEN
      UPDATE powa_snapshot_metas
      SET errors = v_errs
      WHERE srvid = _srvid;
    END IF;

    PERFORM powa_log('end of powa_take_snapshot(' || _srvid || ')');
    PERFORM set_config('application_name',
        v_title || 'snapshot finished',
        false);

    return v_nb_err;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_take_snapshot(int) */

CREATE OR REPLACE FUNCTION powa_databases_src(IN _srvid integer,
    OUT oid oid,
    OUT datname name)
RETURNS SETOF record AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY SELECT d.oid, d.datname
        FROM pg_database d;
    ELSE
        RETURN QUERY SELECT d.oid, d.datname
        FROM powa_databases_src_tmp d
        WHERE srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_databases_src */

CREATE OR REPLACE FUNCTION powa_databases_snapshot(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_databases_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- Keep track of existing databases
    PERFORM powa_log('Maintaining database list...');

    WITH missing AS (
        SELECT _srvid AS srvid, d.oid, d.datname
        FROM powa_databases_src(_srvid) d
        LEFT JOIN powa_databases p ON d.oid = p.oid AND p.srvid = _srvid
        WHERE p.oid IS NULL
    )
    INSERT INTO powa_databases
    SELECT * FROM missing;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('missing db: %s', v_rowcount));

    -- Keep track of renamed databases
    WITH renamed AS (
        SELECT d.oid, d.datname
        FROM powa_databases_src(_srvid) AS d
        JOIN powa_databases AS p ON d.oid = p.oid AND p.srvid = _srvid
        WHERE d.datname != p.datname
    )
    UPDATE powa_databases AS p
    SET datname = r.datname
    FROM renamed AS r
    WHERE p.oid = r.oid
      AND p.srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('renamed db: %s', v_rowcount));

    -- Keep track of when databases are dropped
    WITH dropped AS (
        SELECT p.oid
        FROM powa_databases p
        LEFT JOIN powa_databases_src(_srvid) d ON p.oid = d.oid
        WHERE d.oid IS NULL
          AND p.dropped IS NULL
          AND p.srvid = _srvid)
    UPDATE powa_databases p
    SET dropped = now()
    FROM dropped d
    WHERE p.oid = d.oid
      AND p.srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('dropped db: %s', v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_databases_src_tmp WHERE srvid = _srvid;
    END IF;
END;
$PROC$ language plpgsql; /* end of powa_databases_snapshot */

CREATE OR REPLACE FUNCTION powa_statements_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT userid oid,
    OUT dbid oid,
    OUT queryid bigint,
    OUT query text,
    OUT calls bigint,
    OUT total_time double precision,
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
    OUT blk_write_time double precision
)
RETURNS SETOF record AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY SELECT now(),
            pgss.userid, pgss.dbid, pgss.queryid, pgss.query, pgss.calls,
            pgss.total_time, pgss.rows, pgss.shared_blks_hit,
            pgss.shared_blks_read, pgss.shared_blks_dirtied,
            pgss.shared_blks_written, pgss.local_blks_hit,
            pgss.local_blks_read, pgss.local_blks_dirtied,
            pgss.local_blks_written, pgss.temp_blks_read,
            pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time
        FROM pg_stat_statements pgss
        JOIN pg_database d ON d.oid = pgss.dbid
        JOIN pg_roles r ON pgss.userid = r.oid
        WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
        AND NOT (r.rolname = ANY (string_to_array(
                    coalesce(current_setting('powa.ignored_users'), ''),
                    ',')));
    ELSE
        RETURN QUERY SELECT pgss.ts,
            pgss.userid, pgss.dbid, pgss.queryid, pgss.query, pgss.calls,
            pgss.total_time, pgss.rows, pgss.shared_blks_hit,
            pgss.shared_blks_read, pgss.shared_blks_dirtied,
            pgss.shared_blks_written, pgss.local_blks_hit,
            pgss.local_blks_read, pgss.local_blks_dirtied,
            pgss.local_blks_written, pgss.temp_blks_read,
            pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time
        FROM powa_statements_src_tmp pgss WHERE srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_src */

CREATE OR REPLACE FUNCTION powa_statements_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_statements_snapshot';
    v_rowcount    bigint;
BEGIN
    -- In this function, we capture statements, and also aggregate counters by database
    -- so that the first screens of powa stay reactive even though there may be thousands
    -- of different statements
    -- We only capture databases that are still there
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    WITH capture AS(
        SELECT *
        FROM powa_statements_src(_srvid)
    ),

    missing_statements AS(
        INSERT INTO powa_statements (srvid, queryid, dbid, userid, query)
            SELECT _srvid, queryid, dbid, userid, query
            FROM capture c
            WHERE NOT EXISTS (SELECT 1
                              FROM powa_statements ps
                              WHERE ps.queryid = c.queryid
                              AND ps.dbid = c.dbid
                              AND ps.userid = c.userid
                              AND ps.srvid = _srvid
            )
    ),

    by_query AS (
        INSERT INTO powa_statements_history_current
            SELECT _srvid, queryid, dbid, userid,
            ROW(
                ts, calls, total_time, rows, shared_blks_hit, shared_blks_read,
                shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read,
                local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
                blk_read_time, blk_write_time
            )::powa_statements_history_record AS record
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_statements_history_current_db
            SELECT _srvid, dbid,
            ROW(
                ts, sum(calls), sum(total_time), sum(rows), sum(shared_blks_hit), sum(shared_blks_read),
                sum(shared_blks_dirtied), sum(shared_blks_written), sum(local_blks_hit), sum(local_blks_read),
                sum(local_blks_dirtied), sum(local_blks_written), sum(temp_blks_read), sum(temp_blks_written),
                sum(blk_read_time), sum(blk_write_time)
            )::powa_statements_history_record AS record
            FROM capture
            GROUP BY dbid, ts
    )

    SELECT count(*) INTO v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_statements_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true; -- For now we don't care. What could we do on error except crash anyway?
END;
$PROC$ language plpgsql; /* end of powa_statements_snapshot */

CREATE OR REPLACE FUNCTION powa_user_functions_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT dbid oid,
    OUT funcid oid,
    OUT calls bigint,
    OUT total_time double precision,
    OUT self_time double precision
) RETURNS SETOF record AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY SELECT now(), d.oid, r.funcid, r.calls, r.total_time,
            r.self_time
        FROM pg_database d, powa_stat_user_functions(oid) r;
    ELSE
        RETURN QUERY SELECT r.ts, r.dbid, r.funcid, r.calls, r.total_time,
            r.self_time
        FROM powa_user_functions_src_tmp r
        WHERE r.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_user_functions_src */

CREATE OR REPLACE FUNCTION powa_user_functions_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_user_functions_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- Insert cluster-wide user function statistics
    WITH func AS (
        SELECT *
        FROM powa_user_functions_src(_srvid)
    )
    INSERT INTO powa_user_functions_history_current
        SELECT _srvid, dbid, funcid,
        ROW(ts, calls,
            total_time,
            self_time)::powa_user_functions_history_record AS record
        FROM func;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_user_functions_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_user_functions_snapshot */

CREATE OR REPLACE FUNCTION powa_all_relations_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT dbid oid,
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
    OUT autoanalyze_count bigint
) RETURNS SETOF record AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY SELECT now(),
            d.oid AS dbid, r.relid, r.numscan, r.tup_returned, r.tup_fetched,
            r.n_tup_ins, r.n_tup_upd, r.n_tup_del, r.n_tup_hot_upd,
            r.n_liv_tup, r.n_dead_tup, r.n_mod_since_analyze, r.blks_read,
            r.blks_hit, r.last_vacuum, r.vacuum_count, r.last_autovacuum,
            r.autovacuum_count, r.last_analyze, r.analyze_count,
            r.last_autoanalyze, r.autoanalyze_count
        FROM pg_database d, powa_stat_all_rel(d.oid) as r;
    ELSE
        RETURN QUERY SELECT r.ts,
            r.dbid, r.relid, r.numscan, r.tup_returned, r.tup_fetched,
            r.n_tup_ins, r.n_tup_upd, r.n_tup_del, r.n_tup_hot_upd,
            r.n_liv_tup, r.n_dead_tup, r.n_mod_since_analyze, r.blks_read,
            r.blks_hit, r.last_vacuum, r.vacuum_count, r.last_autovacuum,
            r.autovacuum_count, r.last_analyze, r.analyze_count,
            r.last_autoanalyze, r.autoanalyze_count
        FROM powa_all_relations_src_tmp AS r
        WHERE r.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_relations_src */

CREATE OR REPLACE FUNCTION powa_all_relations_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_all_relations_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- Insert cluster-wide relation statistics
    WITH rel AS (
        SELECT *
        FROM powa_all_relations_src(_srvid)
    )
    INSERT INTO powa_all_relations_history_current
        SELECT _srvid, dbid, relid,
        ROW(ts,numscan, tup_returned, tup_fetched,
            n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
            n_liv_tup, n_dead_tup, n_mod_since_analyze,
            blks_read, blks_hit, last_vacuum, vacuum_count,
            last_autovacuum, autovacuum_count, last_analyze,
            analyze_count, last_autoanalyze,
            autoanalyze_count)::powa_all_relations_history_record AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_all_relations_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_all_relations_snapshot */


CREATE OR REPLACE FUNCTION powa_databases_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_databases_purge(' || _srvid || ')';
    v_rowcount    bigint;
    v_dropped_dbid oid[];
    v_retention   interval;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    SELECT powa_get_server_retention(_srvid) INTO v_retention;

    -- Cleanup old dropped databases, over retention
    -- This will cascade automatically to powa_statements and other
    WITH dropped_databases AS
      ( DELETE FROM powa_databases
        WHERE dropped < (now() - v_retention * 1.2)
        AND srvid = _srvid
        RETURNING oid
        )
    SELECT array_agg(oid) INTO v_dropped_dbid FROM dropped_databases;

    perform powa_log(format('%I (powa_databases) - rowcount: %s)',
           v_funcname,array_length(v_dropped_dbid,1)));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_databases_purge */


CREATE OR REPLACE FUNCTION powa_statements_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_statements_purge(' || _srvid || ')';
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    SELECT powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_statements_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_hitory) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_statements_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_purge */

CREATE OR REPLACE FUNCTION powa_user_functions_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_user_functions_purge(' || _srvid || ')';
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    SELECT powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_user_functions_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_user_functions_purge */

CREATE OR REPLACE FUNCTION powa_all_relations_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_all_relations_purge(' || _srvid || ')';
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    SELECT powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_all_relations_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_relations_purge */

CREATE OR REPLACE FUNCTION powa_statements_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_statements_aggregate(' || _srvid || ')';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate statements table
    INSERT INTO powa_statements_history
        SELECT srvid, queryid, dbid, userid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).calls),min((record).total_time),min((record).rows),
                min((record).shared_blks_hit),min((record).shared_blks_read),
                min((record).shared_blks_dirtied),min((record).shared_blks_written),
                min((record).local_blks_hit),min((record).local_blks_read),
                min((record).local_blks_dirtied),min((record).local_blks_written),
                min((record).temp_blks_read),min((record).temp_blks_written),
                min((record).blk_read_time),min((record).blk_write_time))::powa_statements_history_record,
            ROW(max((record).ts),
                max((record).calls),max((record).total_time),max((record).rows),
                max((record).shared_blks_hit),max((record).shared_blks_read),
                max((record).shared_blks_dirtied),max((record).shared_blks_written),
                max((record).local_blks_hit),max((record).local_blks_read),
                max((record).local_blks_dirtied),max((record).local_blks_written),
                max((record).temp_blks_read),max((record).temp_blks_written),
                max((record).blk_read_time),max((record).blk_write_time))::powa_statements_history_record
        FROM powa_statements_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, queryid, dbid, userid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_statements_history_current WHERE srvid = _srvid;

    -- aggregate db table
    INSERT INTO powa_statements_history_db
        SELECT srvid, dbid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).calls),min((record).total_time),min((record).rows),
                min((record).shared_blks_hit),min((record).shared_blks_read),
                min((record).shared_blks_dirtied),min((record).shared_blks_written),
                min((record).local_blks_hit),min((record).local_blks_read),
                min((record).local_blks_dirtied),min((record).local_blks_written),
                min((record).temp_blks_read),min((record).temp_blks_written),
                min((record).blk_read_time),min((record).blk_write_time))::powa_statements_history_record,
            ROW(max((record).ts),
                max((record).calls),max((record).total_time),max((record).rows),
                max((record).shared_blks_hit),max((record).shared_blks_read),
                max((record).shared_blks_dirtied),max((record).shared_blks_written),
                max((record).local_blks_hit),max((record).local_blks_read),
                max((record).local_blks_dirtied),max((record).local_blks_written),
                max((record).temp_blks_read),max((record).temp_blks_written),
                max((record).blk_read_time),max((record).blk_write_time))::powa_statements_history_record
        FROM powa_statements_history_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_statements_history_current_db WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_aggregate */

CREATE OR REPLACE FUNCTION powa_user_functions_aggregate(_srvid integer)
RETURNS void AS $PROC$
BEGIN
    PERFORM powa_log('running powa_user_functions_aggregate(' || _srvid ||')');

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate user_functions table
    INSERT INTO powa_user_functions_history
        SELECT srvid, dbid, funcid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts), min((record).calls),min((record).total_time),
                min((record).self_time))::powa_user_functions_history_record,
            ROW(max((record).ts), max((record).calls),max((record).total_time),
                max((record).self_time))::powa_user_functions_history_record
        FROM powa_user_functions_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, funcid;

    DELETE FROM powa_user_functions_history_current WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_user_functions_aggregate */

CREATE OR REPLACE FUNCTION powa_all_relations_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_all_relations_aggregate(' || _srvid || ')';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate all_relations table
    INSERT INTO powa_all_relations_history
        SELECT srvid, dbid, relid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).numscan),min((record).tup_returned),min((record).tup_fetched),
                min((record).n_tup_ins),min((record).n_tup_upd),
                min((record).n_tup_del),min((record).n_tup_hot_upd),
                min((record).n_liv_tup),min((record).n_dead_tup),
                min((record).n_mod_since_analyze),min((record).blks_read),
                min((record).blks_hit),min((record).last_vacuum),
                min((record).vacuum_count),min((record).last_autovacuum),
                min((record).autovacuum_count),min((record).last_analyze),
                min((record).analyze_count),min((record).last_autoanalyze),
                min((record).autoanalyze_count))::powa_all_relations_history_record,
            ROW(max((record).ts),
                max((record).numscan),max((record).tup_returned),max((record).tup_fetched),
                max((record).n_tup_ins),max((record).n_tup_upd),
                max((record).n_tup_del),max((record).n_tup_hot_upd),
                max((record).n_liv_tup),max((record).n_dead_tup),
                max((record).n_mod_since_analyze),max((record).blks_read),
                max((record).blks_hit),max((record).last_vacuum),
                max((record).vacuum_count),max((record).last_autovacuum),
                max((record).autovacuum_count),max((record).last_analyze),
                max((record).analyze_count),max((record).last_autoanalyze),
                max((record).autoanalyze_count))::powa_all_relations_history_record
        FROM powa_all_relations_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, relid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_all_relations_history_current WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_relations_aggregate */

CREATE OR REPLACE FUNCTION public.powa_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
  funcname text;
  v_state   text;
  v_msg     text;
  v_detail  text;
  v_hint    text;
  v_context text;
BEGIN
    -- Find reset function for every supported datasource, including pgss
    -- Also call reset function even if they're not enabled
    FOR funcname IN SELECT function_name
                 FROM powa_functions
                 WHERE operation='reset'
                 AND srvid = _srvid
                 ORDER BY module LOOP
      -- Call all of them, for the current srvid
      BEGIN
          EXECUTE format('SELECT %I(%s)', funcname, _srvid);
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS
              v_state   = RETURNED_SQLSTATE,
              v_msg     = MESSAGE_TEXT,
              v_detail  = PG_EXCEPTION_DETAIL,
              v_hint    = PG_EXCEPTION_HINT,
              v_context = PG_EXCEPTION_CONTEXT;
          RAISE warning 'powa_reset(): function "%(%)" failed:
              state  : %
              message: %
              detail : %
              hint   : %
              context: %',
              _srvid, funcname, v_state, v_msg, v_detail, v_hint, v_context;

      END;
    END LOOP;
    RETURN true;
END;
$function$; /* end of powa_reset */

CREATE OR REPLACE FUNCTION public.powa_statements_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM powa_log('Resetting powa_statements_history(' || _srvid || ')');
    DELETE FROM powa_statements_history WHERE srvid = _srvid;

    PERFORM powa_log('Resetting powa_statements_history_current(' || _srvid || ')');
    DELETE FROM powa_statements_history_current WHERE srvid = _srvid;

    PERFORM powa_log('Resetting powa_statements_history_db(' || _srvid || ')');
    DELETE FROM powa_statements_history_db WHERE srvid = _srvid;

    PERFORM powa_log('Resetting powa_statements_history_current_db(' || _srvid || ')');
    DELETE FROM powa_statements_history_current_db WHERE srvid = _srvid;

    PERFORM powa_log('Resetting powa_statements(' || _srvid || ')');

    -- if 3rd part datasource has FK on it, throw everything away
    DELETE FROM powa_statements WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_statements_reset */

CREATE OR REPLACE FUNCTION public.powa_user_functions_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM powa_log('Resetting powa_user_functions_history(' || _srvid || ')');
    DELETE FROM powa_user_functions_history WHERE srvid = _srvid;

    PERFORM powa_log('Resetting powa_user_functions_history_current(' || _srvid || ')');
    DELETE FROM powa_user_functions_history_current WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_user_functions_reset */

CREATE OR REPLACE FUNCTION public.powa_all_relations_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM powa_log('Resetting powa_all_relations_history(' || _srvid || ')');
    DELETE FROM powa_all_relations_history WHERE srvid = _srvid;

    PERFORM powa_log('Resetting powa_all_relations_history_current(' || _srvid || ')');
    DELETE FROM powa_all_relations_history_current WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_all_relations_reset */

/* pg_stat_kcache integration - part 2 */

/*
 * register pg_stat_kcache extension
 */
CREATE OR REPLACE function public.powa_kcache_register(_srvid integer = 0) RETURNS bool AS
$_$
DECLARE
    v_func_present bool;
    v_ext_present bool;
BEGIN
    -- Only check for extension availability for local server
    IF (_srvid = 0) THEN
        SELECT COUNT(*) = 1 INTO v_ext_present
        FROM pg_extension
        WHERE extname = 'pg_stat_kcache';
    ELSE
        v_ext_present = true;
    END IF;

    IF ( v_ext_present ) THEN
        SELECT COUNT(*) > 0 INTO v_func_present
        FROM public.powa_functions
        WHERE module = 'pg_stat_kcache'
        AND srvid = _srvid;

        IF ( NOT v_func_present) THEN
            PERFORM powa_log('registering pg_stat_kcache');

            INSERT INTO powa_functions (srvid, module, operation, function_name, query_source, added_manually, enabled, priority)
            VALUES (_srvid, 'pg_stat_kcache', 'snapshot',   'powa_kcache_snapshot',   'powa_kcache_src', false, true, -1),
                   (_srvid, 'pg_stat_kcache', 'aggregate',  'powa_kcache_aggregate',  NULL,       false, true, default),
                   (_srvid, 'pg_stat_kcache', 'unregister', 'powa_kcache_unregister', NULL,       false, true, default),
                   (_srvid, 'pg_stat_kcache', 'purge',      'powa_kcache_purge',      NULL,       false, true, default),
                   (_srvid, 'pg_stat_kcache', 'reset',      'powa_kcache_reset',      NULL,       false, true, default);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql; /* end of powa_kcache_register */

/*
 * unregister pg_stat_kcache extension
 */
CREATE OR REPLACE function public.powa_kcache_unregister() RETURNS bool AS
$_$
BEGIN
    PERFORM powa_log('unregistering pg_stat_kcache');
    DELETE FROM public.powa_functions WHERE module = 'pg_stat_kcache';
    RETURN true;
END;
$_$
language plpgsql; /* end of powa_kcache_unregister */

CREATE OR REPLACE FUNCTION powa_kcache_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT queryid bigint, OUT userid oid, OUT dbid oid,
    OUT reads bigint, OUT writes bigint,
    OUT user_time double precision, OUT system_time double precision,
    OUT minflts bigint, OUT majflts bigint,
    OUT nswaps bigint,
    OUT msgsnds bigint, OUT msgrcvs bigint,
    OUT nsignals bigint,
    OUT nvcsws bigint, OUT nivcsws bigint
) RETURNS SETOF record AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY SELECT now(),
            k.queryid, k.userid, k.dbid, k.reads, k.writes, k.user_time,
            k.system_time, k.minflts, k.majflts, k.nswaps, k.msgsnds,
            k.msgrcvs, k.nsignals, k.nvcsws, k.nivcsws
        FROM pg_stat_kcache() k
        JOIN pg_roles r ON r.oid = k.userid
        WHERE NOT (r.rolname = ANY (string_to_array(
                    coalesce(current_setting('powa.ignored_users'), ''),
                    ',')))
        AND k.dbid NOT IN (SELECT oid FROM powa_databases WHERE dropped IS NOT NULL);
    ELSE
        RETURN QUERY SELECT k.ts,
            k.queryid, k.userid, k.dbid, k.reads, k.writes, k.user_time,
            k.system_time, k.minflts, k.majflts, k.nswaps, k.msgsnds,
            k.msgrcvs, k.nsignals, k.nvcsws, k.nivcsws
        FROM powa_kcache_src_tmp k
        WHERE k.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_kcache_src */

/*
 * powa_kcache snapshot collection.
 */
CREATE OR REPLACE FUNCTION powa_kcache_snapshot(_srvid integer) RETURNS void as $PROC$
DECLARE
  result bool;
    v_funcname    text := 'powa_kcache_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    WITH capture AS (
        SELECT *
        FROM powa_kcache_src(_srvid)
    ),

    by_query AS (
        INSERT INTO powa_kcache_metrics_current (srvid, queryid, dbid, userid, metrics)
            SELECT _srvid, queryid, dbid, userid,
              (ts, reads, writes, user_time, system_time, minflts, majflts,
               nswaps, msgsnds, msgrcvs, nsignals, nvcsws, nivcsws)::powa_kcache_type
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_kcache_metrics_current_db (srvid, dbid, metrics)
            SELECT _srvid AS srvid, dbid,
              (ts, sum(reads), sum(writes), sum(user_time), sum(system_time),
               sum(minflts), sum(majflts), sum(nswaps), sum(msgsnds),
               sum(msgrcvs), sum(nsignals), sum(nvcsws), sum(nivcsws))::powa_kcache_type
            FROM capture
            GROUP BY ts, srvid, dbid
    )

    SELECT COUNT(*) into v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_kcache_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END
$PROC$ language plpgsql; /* end of powa_kcache_snapshot */

/*
 * powa_kcache aggregation
 */
CREATE OR REPLACE FUNCTION powa_kcache_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    result     bool;
    v_funcname text := 'powa_kcache_aggregate(' || _srvid || ')';
    v_rowcount bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate metrics table
    INSERT INTO powa_kcache_metrics (coalesce_range, srvid, queryid, dbid, userid, metrics, mins_in_range, maxs_in_range)
        SELECT tstzrange(min((metrics).ts), max((metrics).ts),'[]'),
        srvid, queryid, dbid, userid, array_agg(metrics),
        ROW(min((metrics).ts),
            min((metrics).reads),min((metrics).writes),min((metrics).user_time),
            min((metrics).system_time), min((metrics).minflts),
            min((metrics).majflts), min((metrics).nswaps),
            min((metrics).msgsnds), min((metrics).msgrcvs),
            min((metrics).nsignals), min((metrics).nvcsws),
            min((metrics).nivcsws))::powa_kcache_type,
        ROW(max((metrics).ts),
            max((metrics).reads),max((metrics).writes),max((metrics).user_time),
            max((metrics).system_time), max((metrics).minflts),
            max((metrics).majflts), max((metrics).nswaps),
            max((metrics).msgsnds), max((metrics).msgrcvs),
            max((metrics).nsignals), max((metrics).nvcsws),
            max((metrics).nivcsws))::powa_kcache_type
        FROM powa_kcache_metrics_current
        WHERE srvid = _srvid
        GROUP BY srvid, queryid, dbid, userid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_kcache_metrics) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_kcache_metrics_current WHERE srvid = _srvid;

    -- aggregate metrics_db table
    INSERT INTO powa_kcache_metrics_db (srvid, coalesce_range, dbid, metrics, mins_in_range, maxs_in_range)
        SELECT srvid, tstzrange(min((metrics).ts), max((metrics).ts),'[]'),
        dbid, array_agg(metrics),
        ROW(min((metrics).ts),
            min((metrics).reads),min((metrics).writes),min((metrics).user_time),
            min((metrics).system_time), min((metrics).minflts),
            min((metrics).majflts), min((metrics).nswaps),
            min((metrics).msgsnds), min((metrics).msgrcvs),
            min((metrics).nsignals), min((metrics).nvcsws),
            min((metrics).nivcsws))::powa_kcache_type,
        ROW(max((metrics).ts),
            max((metrics).reads),max((metrics).writes),max((metrics).user_time),
            max((metrics).system_time), max((metrics).minflts),
            max((metrics).majflts), max((metrics).nswaps),
            max((metrics).msgsnds), max((metrics).msgrcvs),
            max((metrics).nsignals), max((metrics).nvcsws),
            max((metrics).nivcsws))::powa_kcache_type
        FROM powa_kcache_metrics_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_kcache_metrics_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_kcache_metrics_current_db WHERE srvid = _srvid;
END
$PROC$ language plpgsql; /* end of powa_kcache_aggregate */

/*
 * powa_kcache purge
 */
CREATE OR REPLACE FUNCTION powa_kcache_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_kcache_purge(' || _srvid || ')';
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    SELECT powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_kcache_metrics
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_kcache_metrics) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_kcache_metrics_db
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_kcache_metrics_db) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ language plpgsql; /* end of powa_kcache_purge */

/*
 * powa_kcache reset
 */
CREATE OR REPLACE FUNCTION powa_kcache_reset(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_kcache_reset(' || _srvid || ')';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_log('resetting powa_kcache_metrics(' || _srvid || ')');
    DELETE FROM powa_kcache_metrics WHERE srvid = _srvid;

    PERFORM powa_log('resetting powa_kcache_metrics_db(' || _srvid || ')');
    DELETE FROM powa_kcache_metrics_db WHERE srvid = _srvid;

    PERFORM powa_log('resetting powa_kcache_metrics_current(' || _srvid || ')');
    DELETE FROM powa_kcache_metrics_current WHERE srvid = _srvid;

    PERFORM powa_log('resetting powa_kcache_metrics_current_db(' || _srvid || ')');
    DELETE FROM powa_kcache_metrics_current_db WHERE srvid = _srvid;
END;
$PROC$ language plpgsql; /* end of powa_kcache_reset */

-- By default, try to register pg_stat_kcache, in case it's alreay here
SELECT * FROM public.powa_kcache_register();

/* end of pg_stat_kcache integration - part 2 */

/* pg_qualstats integration - part 2 */

/*
 * powa_qualstats_register
 */
CREATE OR REPLACE function public.powa_qualstats_register(_srvid integer = 0) RETURNS bool AS
$_$
DECLARE
    v_func_present bool;
    v_ext_present bool;
BEGIN
    IF (_srvid = 0) THEN
        SELECT COUNT(*) = 1 INTO v_ext_present
        FROM pg_extension
        WHERE extname = 'pg_qualstats';
    ELSE
        v_ext_present = true;
    END IF;

    IF ( v_ext_present) THEN
        SELECT COUNT(*) > 0 INTO v_func_present
        FROM public.powa_functions
        WHERE module = 'pg_qualstats'
        AND srvid = _srvid;

        IF ( NOT v_func_present) THEN
            PERFORM powa_log('registering pg_qualstats');

            INSERT INTO powa_functions (srvid, module, operation, function_name, query_source, added_manually, enabled)
            VALUES (_srvid, 'pg_qualstats', 'snapshot',   'powa_qualstats_snapshot',   'powa_qualstats_src', false, true),
                   (_srvid, 'pg_qualstats', 'aggregate',  'powa_qualstats_aggregate',  NULL,                 false, true),
                   (_srvid, 'pg_qualstats', 'unregister', 'powa_qualstats_unregister', NULL,                 false, true),
                   (_srvid, 'pg_qualstats', 'purge',      'powa_qualstats_purge',      NULL,                 false, true),
                   (_srvid, 'pg_qualstats', 'reset',      'powa_qualstats_reset',      NULL,                 false, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql; /* end of powa_qualstats_register */

/*
 * powa_qualstats utility SRF for aggregating constvalues
 */
CREATE OR REPLACE FUNCTION powa_qualstats_aggregate_constvalues_current(
    IN _srvid integer,
    IN _ts_from timestamptz DEFAULT '-infinity'::timestamptz,
    IN _ts_to timestamptz DEFAULT 'infinity'::timestamptz,
    OUT srvid integer,
    OUT qualid bigint,
    OUT queryid bigint,
    OUT dbid oid,
    OUT userid oid,
    OUT tstzrange tstzrange,
    OUT mu qual_values[],
    OUT mf qual_values[],
    OUT lf qual_values[],
    OUT me qual_values[])
RETURNS SETOF record AS $_$
WITH consts AS (
  SELECT q.srvid, q.qualid, q.queryid, q.dbid, q.userid,
    min(q.ts) as mints, max(q.ts) as maxts,
    sum(q.occurences) as occurences,
    sum(q.nbfiltered) as nbfiltered,
    sum(q.execution_count) as execution_count,
    q.constvalues
  FROM powa_qualstats_constvalues_history_current q
  WHERE q.srvid = _srvid
  AND q.ts >= _ts_from AND q.ts <= _ts_to
  GROUP BY q.srvid, q.qualid, q.queryid, q.dbid, q.userid, q.constvalues
),
groups AS (
  SELECT c.srvid, c.qualid, c.queryid, c.dbid, c.userid,
    tstzrange(min(c.mints), max(c.maxts),'[]')
  FROM consts c
  GROUP BY c.srvid, c.qualid, c.queryid, c.dbid, c.userid
)
SELECT *
FROM groups,
LATERAL (
  SELECT array_agg(constvalues) as mu
  FROM (
    SELECT (constvalues, occurences, nbfiltered, execution_count)::qual_values as constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY occurences desc
    LIMIT 20
  ) s
) as mu,
LATERAL (
  SELECT array_agg(constvalues) as mf
  FROM (
    SELECT (constvalues, occurences, nbfiltered, execution_count)::qual_values as constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY CASE WHEN execution_count = 0 THEN 0 ELSE nbfiltered / execution_count::numeric END DESC
    LIMIT 20
  ) s
) as mf,
LATERAL (
  SELECT array_agg(constvalues) as lf
  FROM (
    SELECT (constvalues, occurences, nbfiltered, execution_count)::qual_values as constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY CASE WHEN execution_count = 0 THEN 0 ELSE nbfiltered / execution_count::numeric END DESC
    LIMIT 20
  ) s
) as lf,
LATERAL (
  SELECT array_agg(constvalues) as me
  FROM (
    SELECT (constvalues, occurences, nbfiltered, execution_count)::qual_values as constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY execution_count desc
    LIMIT 20
  ) s
) as me;
$_$ LANGUAGE sql; /* end of powa_qualstats_aggregate_constvalues_current */

CREATE OR REPLACE FUNCTION powa_qualstats_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT uniquequalnodeid bigint,
    OUT dbid oid,
    OUT userid oid,
    OUT qualnodeid bigint,
    OUT occurences bigint,
    OUT execution_count bigint,
    OUT nbfiltered bigint,
    OUT queryid bigint,
    OUT constvalues varchar[],
    OUT quals qual_type[]
) RETURNS SETOF record AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY
            SELECT now(), pgqs.uniquequalnodeid, pgqs.dbid, pgqs.userid,
                pgqs.qualnodeid, pgqs.occurences, pgqs.execution_count,
                pgqs.nbfiltered, pgqs.queryid, pgqs.constvalues, pgqs.quals
            FROM (
                SELECT coalesce(i.uniquequalid, i.uniquequalnodeid) AS uniquequalnodeid,
                    i.dbid, i.userid,  coalesce(i.qualid, i.qualnodeid) AS qualnodeid,
                    i.occurences, i.execution_count, i.nbfiltered, i.queryid,
                    array_agg(i.constvalue order by i.constant_position) AS constvalues,
                    array_agg(ROW(i.relid, i.attnum, i.opno, i.eval_type)::qual_type) AS quals
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
                    qs.eval_type,
                    qs.constant_position
                    FROM pg_qualstats() qs
                    WHERE (qs.lrelid IS NULL) != (qs.rrelid IS NULL)
                ) i
                GROUP BY coalesce(i.uniquequalid, i.uniquequalnodeid),
                    coalesce(i.qualid, i.qualnodeid), i.dbid, i.userid,
                    i.occurences, i.execution_count, i.nbfiltered, i.queryid
            ) pgqs
            JOIN (
                -- if we use remote capture, powa_statements won't be
                -- populated, so we have to to retrieve the content of both
                -- statements sources.  Since there can (and probably) be
                -- duplicates, we use a UNION on purpose
                SELECT s1.queryid, s1.dbid, s1.userid
                    FROM pg_stat_statements s1
                UNION
                SELECT s2.queryid, s2.dbid, s2.userid
                    FROM powa_statements s2 WHERE s2.srvid = 0
            ) s USING(queryid, dbid, userid)
        -- we don't gather quals for databases that have been dropped
        JOIN pg_database d ON d.oid = s.dbid
        JOIN pg_roles r ON s.userid = r.oid
          AND NOT (r.rolname = ANY (string_to_array(
                    coalesce(current_setting('powa.ignored_users'), ''),
                    ',')))
        WHERE pgqs.dbid NOT IN (SELECT oid FROM powa_databases WHERE dropped IS NOT NULL);
    ELSE
        RETURN QUERY
            SELECT pgqs.ts, pgqs.uniquequalnodeid, pgqs.dbid, pgqs.userid,
                pgqs.qualnodeid, pgqs.occurences, pgqs.execution_count,
                pgqs.nbfiltered, pgqs.queryid, pgqs.constvalues, pgqs.quals
            FROM powa_qualstats_src_tmp pgqs
        WHERE pgqs.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_qualstats_src */

CREATE OR REPLACE FUNCTION powa_qualstats_snapshot(_srvid integer) RETURNS void as $PROC$
DECLARE
    result     bool;
    v_funcname text := 'powa_qualstats_snapshot';
    v_rowcount bigint;
BEGIN
  PERFORM powa_log(format('running %I', v_funcname));

  PERFORM powa_prevent_concurrent_snapshot(_srvid);

  WITH capture AS (
    SELECT *
    FROM powa_qualstats_src(_srvid)
  ),
  missing_quals AS (
      INSERT INTO powa_qualstats_quals (srvid, qualid, queryid, dbid, userid, quals)
        SELECT DISTINCT _srvid AS srvid, qs.qualnodeid, qs.queryid, qs.dbid, qs.userid,
          array_agg(DISTINCT q::qual_type)
        FROM capture qs,
        LATERAL (SELECT (unnest(quals)).*) as q
        WHERE NOT EXISTS (
          SELECT 1
          FROM powa_qualstats_quals nh
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
      INSERT INTO powa_qualstats_quals_history_current (srvid, qualid, queryid, dbid, userid, ts, occurences, execution_count, nbfiltered)
      SELECT _srvid AS srvid, qs.qualnodeid, qs.queryid, qs.dbid, qs.userid, ts, sum(occurences), sum(execution_count), sum(nbfiltered)
        FROM capture as qs
        GROUP BY srvid, ts, qualnodeid, qs.queryid, qs.dbid, qs.userid
      RETURNING *
  ),
  by_qual_with_const AS (
      INSERT INTO powa_qualstats_constvalues_history_current(srvid, qualid, queryid, dbid, userid, ts, occurences, execution_count, nbfiltered, constvalues)
      SELECT _srvid, qualnodeid, qs.queryid, qs.dbid, qs.userid, ts, occurences, execution_count, nbfiltered, constvalues
      FROM capture as qs
  )
  SELECT COUNT(*) into v_rowcount
  FROM capture;

  perform powa_log(format('%I - rowcount: %s',
        v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_qualstats_src_tmp WHERE srvid = _srvid;
    END IF;

  result := true;
  PERFORM pg_qualstats_reset();
END
$PROC$ language plpgsql; /* end of powa_qualstats_snapshot */

/*
 * powa_qualstats aggregate
 */
CREATE OR REPLACE FUNCTION powa_qualstats_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
  result bool;
BEGIN
  PERFORM powa_log('running powa_qualstats_aggregate(' || _srvid || ')');

  PERFORM powa_prevent_concurrent_snapshot(_srvid);

  INSERT INTO powa_qualstats_constvalues_history (
      srvid, qualid, queryid, dbid, userid, coalesce_range, most_used,
      most_filtering, least_filtering, most_executed)
    SELECT * FROM powa_qualstats_aggregate_constvalues_current(_srvid)
    WHERE srvid = _srvid;

  INSERT INTO powa_qualstats_quals_history (srvid, qualid, queryid, dbid,
      userid, coalesce_range, records, mins_in_range, maxs_in_range)
    SELECT srvid, qualid, queryid, dbid, userid, tstzrange(min(ts),
      max(ts),'[]'), array_agg((ts, occurences, execution_count, nbfiltered)::powa_qualstats_history_item),
    ROW(min(ts), min(occurences), min(execution_count), min(nbfiltered))::powa_qualstats_history_item,
    ROW(max(ts), max(occurences), max(execution_count), max(nbfiltered))::powa_qualstats_history_item
    FROM powa_qualstats_quals_history_current
    WHERE srvid = _srvid
    GROUP BY srvid, qualid, queryid, dbid, userid;

  DELETE FROM powa_qualstats_constvalues_history_current WHERE srvid = _srvid;
  DELETE FROM powa_qualstats_quals_history_current WHERE srvid = _srvid;
END
$PROC$ language plpgsql; /* end of powa_qualstats_aggregate */

/*
 * powa_qualstats_purge
 */
CREATE OR REPLACE FUNCTION powa_qualstats_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_retention   interval;
BEGIN
    PERFORM powa_log('running powa_qualstats_purge(' || _srvid || ')');

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    SELECT powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_qualstats_constvalues_history
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    DELETE FROM powa_qualstats_quals_history
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;
END;
$PROC$ language plpgsql; /* end of powa_qualstats_purge */

/*
 * powa_qualstats_reset
 */
CREATE OR REPLACE FUNCTION powa_qualstats_reset(_srvid integer)
RETURNS void as $PROC$
BEGIN
  PERFORM powa_log('running powa_qualstats_reset(' || _srvid || ')');

  PERFORM powa_log('resetting powa_qualstats_quals(' || _srvid || ')');
  DELETE FROM powa_qualstats_quals WHERE srvid = _srvid;
  -- cascaded :
  -- powa_qualstats_quals_history
  -- powa_qualstats_quals_history_current
  -- powa_qualstats_constvalues_history
  -- powa_qualstats_constvalues_history_current
END;
$PROC$ language plpgsql; /* end of powa_qualstats_reset */

/*
 * powa_qualstats_unregister
 */
CREATE OR REPLACE function public.powa_qualstats_unregister() RETURNS bool AS
$_$
BEGIN
    PERFORM powa_log('unregistering pg_qualstats');
    DELETE FROM public.powa_functions WHERE module = 'pg_qualstats';
    RETURN true;
END;
$_$
language plpgsql; /* end of powa_qualstats_unregister */

SELECT * FROM public.powa_qualstats_register();

/* end of pg_qualstats integration - part 2 */

/* pg_track_settings integration */

CREATE OR REPLACE FUNCTION powa_track_settings_register(_srvid integer = 0) RETURNS bool AS $_$
DECLARE
    v_func_present bool;
    v_ext_present bool;
BEGIN
    IF (_srvid = 0) THEN
        SELECT COUNT(*) = 1 INTO v_ext_present
        FROM pg_extension
        WHERE extname = 'pg_track_settings';
    ELSE
        v_ext_present = true;
    END IF;

    IF ( v_ext_present ) THEN
        SELECT COUNT(*) > 0 INTO v_func_present
        FROM public.powa_functions
        WHERE module = 'pg_track_settings'
        AND srvid = _srvid;

        IF ( NOT v_func_present) THEN
            PERFORM powa_log('registering pg_track_settings');

            -- This extension handles its own storage, just its snapshot
            -- function and an unregister function.
            INSERT INTO powa_functions (srvid, module, operation, function_name, query_source, added_manually, enabled)
            VALUES (_srvid, 'pg_track_settings', 'snapshot',   'pg_track_settings_snapshot_settings', 'pg_track_settings_settings_src', true, true),
                   (_srvid, 'pg_track_settings', 'snapshot',   'pg_track_settings_snapshot_rds',      'pg_track_settings_rds_src',      true, true),
                   (_srvid, 'pg_track_settings', 'snapshot',   'pg_track_settings_snapshot_reboot',   'pg_track_settings_reboot_src',   true, true),
                   (_srvid, 'pg_track_settings', 'reset',      'pg_track_settings_reset',             NULL,                             true, true),
                   (_srvid, 'pg_track_settings', 'unregister', 'powa_track_settings_unregister',      NULL,                             true, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$ language plpgsql; /* end of pg_track_settings_register */

CREATE OR REPLACE function public.powa_track_settings_unregister(_srvid integer = 0) RETURNS bool AS
$_$
BEGIN
    PERFORM powa_log('unregistering pg_track_settings');
    DELETE FROM public.powa_functions
    WHERE module = 'pg_track_settings'
    AND srvid = _srvid;
    RETURN true;
END;
$_$
language plpgsql; /* end of powa_track_settings_unregister */

-- By default, try to register pg_track_settings, in case it's alreay here
SELECT * FROM public.powa_track_settings_register();

/* end pg_track_settings integration */

/* pg_wait_sampling integration - part 2 */

/*
 * register pg_wait_sampling extension
 */
CREATE OR REPLACE function public.powa_wait_sampling_register(_srvid integer = 0) RETURNS bool AS
$_$
DECLARE
    v_func_present bool;
    v_ext_present bool;
BEGIN
    -- Only check for extension availability for local server
    IF (_srvid = 0) THEN
        SELECT COUNT(*) = 1 INTO v_ext_present
        FROM pg_extension
        WHERE extname = 'pg_wait_sampling';
    ELSE
        v_ext_present = true;
    END IF;

    IF ( v_ext_present ) THEN
        SELECT COUNT(*) > 0 INTO v_func_present
        FROM public.powa_functions
        WHERE module = 'pg_wait_sampling'
        AND srvid = _srvid;

        IF ( NOT v_func_present) THEN
            PERFORM powa_log('registering pg_wait_sampling');

            INSERT INTO powa_functions (srvid, module, operation, function_name, query_source, added_manually, enabled)
            VALUES (_srvid, 'pg_wait_sampling', 'snapshot',   'powa_wait_sampling_snapshot',   'powa_wait_sampling_src', false, true),
                   (_srvid, 'pg_wait_sampling', 'aggregate',  'powa_wait_sampling_aggregate',  NULL,                     false, true),
                   (_srvid, 'pg_wait_sampling', 'unregister', 'powa_wait_sampling_unregister', NULL,                     false, true),
                   (_srvid, 'pg_wait_sampling', 'purge',      'powa_wait_sampling_purge',      NULL,                     false, true),
                   (_srvid, 'pg_wait_sampling', 'reset',      'powa_wait_sampling_reset',      NULL,                     false, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql; /* end of powa_wait_sampling_register */

/*
 * unregister pg_wait_sampling extension
 */
CREATE OR REPLACE function public.powa_wait_sampling_unregister() RETURNS bool AS
$_$
BEGIN
    PERFORM powa_log('unregistering pg_wait_sampling');
    DELETE FROM public.powa_functions WHERE module = 'pg_wait_sampling';
    RETURN true;
END;
$_$
language plpgsql;

CREATE OR REPLACE FUNCTION powa_wait_sampling_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT dbid oid,
    OUT event_type text,
    OUT event text,
    OUT queryid bigint,
    OUT count numeric
) RETURNS SETOF RECORD AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        RETURN QUERY
            -- the various background processes report wait events but don't have
            -- associated queryid.  Gather them all under a fake 0 dbid
            SELECT now(), COALESCE(pgss.dbid, 0) AS dbid, s.event_type,
                s.event, s.queryid, sum(s.count) as count
            FROM pg_wait_sampling_profile s
            -- pg_wait_sampling doesn't offer a per (userid, dbid, queryid) view,
            -- only per pid, but pid can be reused for different databases or users
            -- so we cannot deduce db or user from it.  However, queryid should be
            -- unique across differet databases, so we retrieve the dbid this way.
            LEFT JOIN pg_stat_statements(false) pgss ON pgss.queryid = s.queryid
            WHERE s.event_type IS NOT NULL AND s.event IS NOT NULL
            AND COALESCE(pgss.dbid, 0) NOT IN (SELECT oid FROM powa_databases WHERE dropped IS NOT NULL)
            GROUP BY pgss.dbid, s.event_type, s.event, s.queryid;
    ELSE
        RETURN QUERY
        SELECT s.ts, s.dbid, s.event_type, s.event, s.queryid, s.count
        FROM powa_wait_sampling_src_tmp s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_wait_sampling_src */

/*
 * powa_wait_sampling snapshot collection.
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_snapshot(_srvid integer) RETURNS void as $PROC$
DECLARE
  result bool;
    v_funcname    text := 'powa_wait_sampling_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    WITH capture AS (
        SELECT *
        FROM powa_wait_sampling_src(_srvid)
    ),

    by_query AS (
        INSERT INTO powa_wait_sampling_history_current (srvid, queryid, dbid,
                event_type, event, record)
            SELECT _srvid, queryid, dbid, event_type, event, (ts, count)::wait_sampling_type
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_wait_sampling_history_current_db (srvid, dbid,
                event_type, event, record)
            SELECT _srvid AS srvid, dbid, event_type, event, (ts, sum(count))::wait_sampling_type
            FROM capture
            GROUP BY srvid, ts, dbid, event_type, event
    )

    SELECT COUNT(*) into v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_wait_sampling_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END
$PROC$ language plpgsql; /* end of powa_wait_sampling_snapshot */

/*
 * powa_wait_sampling aggregation
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    result     bool;
    v_funcname text := 'powa_wait_sampling_aggregate(' || _srvid || ')';
    v_rowcount bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate history table
    INSERT INTO powa_wait_sampling_history (coalesce_range, srvid, queryid,
            dbid, event_type, event, records, mins_in_range, maxs_in_range)
        SELECT tstzrange(min((record).ts), max((record).ts),'[]'),
            srvid, queryid, dbid, event_type, event, array_agg(record),
        ROW(min((record).ts),
            min((record).count))::wait_sampling_type,
        ROW(max((record).ts),
            max((record).count))::wait_sampling_type
        FROM powa_wait_sampling_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, queryid, dbid, event_type, event;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_wait_sampling_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_wait_sampling_history_current WHERE srvid = _srvid;

    -- aggregate history_db table
    INSERT INTO powa_wait_sampling_history_db (coalesce_range, srvid, dbid,
            event_type, event, records, mins_in_range, maxs_in_range)
        SELECT tstzrange(min((record).ts), max((record).ts),'[]'), srvid, dbid,
            event_type, event, array_agg(record),
        ROW(min((record).ts),
            min((record).count))::wait_sampling_type,
        ROW(max((record).ts),
            max((record).count))::wait_sampling_type
        FROM powa_wait_sampling_history_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, event_type, event;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_wait_sampling_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_wait_sampling_history_current_db WHERE srvid = _srvid;
END
$PROC$ language plpgsql; /* end of powa_wait_sampling_aggregate */

/*
 * powa_wait_sampling purge
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_wait_sampling_purge(' || _srvid || ')';
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    SELECT powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_wait_sampling_history
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_wait_sampling_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_wait_sampling_history_db
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_wait_sampling_history_db) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ language plpgsql; /* end of powa_wait_sampling_purge */

/*
 * powa_wait_sampling reset
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_reset(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_wait_sampling_reset(' || _srvid || ')';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_log('resetting powa_wait_sampling_history(' || _srvid || ')');
    DELETE FROM powa_wait_sampling_history WHERE srvid = _srvid;

    PERFORM powa_log('resetting powa_wait_sampling_history_db(' || _srvid || ')');
    DELETE FROM powa_wait_sampling_history_db WHERE srvid = _srvid;

    PERFORM powa_log('resetting powa_wait_sampling_history_current(' || _srvid || ')');
    DELETE FROM powa_wait_sampling_history_current WHERE srvid = _srvid;

    PERFORM powa_log('resetting powa_wait_sampling_history_current_db(' || _srvid || ')');
    DELETE FROM powa_wait_sampling_history_current_db WHERE srvid = _srvid;
END;
$PROC$ language plpgsql; /* end of powa_wait_sampling_reset */

-- By default, try to register pg_wait_sampling, in case it's alreay here
SELECT * FROM public.powa_wait_sampling_register();

/* end of pg_wait_sampling integration - part 2 */
