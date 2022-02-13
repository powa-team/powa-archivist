-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "CREATE EXTENSION powa" to load this file. \quit

SET LOCAL statement_timeout = 0;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL standard_conforming_strings = on;
SET LOCAL client_min_messages = warning;
SET LOCAL escape_string_warning = off;
SET LOCAL search_path = public, pg_catalog;

CREATE TABLE powa_servers(
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
INSERT INTO public.powa_servers VALUES (0, '', '<local>', 0, '', NULL, '', -1, 100, '0 second');

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
INSERT INTO public.powa_snapshot_metas (srvid) VALUES (0);

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
    last_present_ts timestamptz NULL DEFAULT now(),
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, dbid) REFERENCES powa_databases(srvid, oid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

ALTER TABLE ONLY powa_statements
    ADD CONSTRAINT powa_statements_pkey PRIMARY KEY (srvid, queryid, dbid, userid);

CREATE INDEX powa_statements_dbid_idx ON powa_statements(srvid, dbid);
CREATE INDEX powa_statements_userid_idx ON powa_statements(userid);
CREATE INDEX powa_statements_mru_idx ON powa_statements (last_present_ts);

CREATE FUNCTION powa_stat_user_functions(IN dbid oid, OUT funcid oid,
    OUT calls bigint,
    OUT total_time double precision,
    OUT self_time double precision)
    RETURNS SETOF record STABLE
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
    RETURNS SETOF record STABLE
    LANGUAGE c COST 100
AS '$libdir/powa', 'powa_stat_all_rel';

CREATE TYPE powa_statements_history_record AS (
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
CREATE TYPE powa_statements_history_diff AS (
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
    blk_write_time_per_sec double precision,
    plans_per_sec double precision,
    plantime_per_sec double precision,
    wal_records_per_sec double precision,
    wal_fpi_per_sec double precision,
    wal_bytes_per_sec numeric
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

CREATE TYPE powa_all_relations_history_db_record AS (
    ts timestamp with time zone,
    seq_scan bigint,
    idx_scan bigint,
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

/* pg_stat_all_relations_db operator support */
CREATE TYPE powa_all_relations_history_db_diff AS (
    intvl interval,
    seq_scan bigint,
    idx_scan bigint,
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

CREATE OR REPLACE FUNCTION powa_all_relations_history_db_mi(
    a powa_all_relations_history_db_record,
    b powa_all_relations_history_db_record)
RETURNS powa_all_relations_history_db_diff AS
$_$
DECLARE
    res powa_all_relations_history_db_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.seq_scan = a.seq_scan - b.seq_scan;
    res.idx_scan = a.idx_scan - b.idx_scan;
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
    PROCEDURE = powa_all_relations_history_db_mi,
    LEFTARG = powa_all_relations_history_db_record,
    RIGHTARG = powa_all_relations_history_db_record
);

CREATE TYPE powa_all_relations_history_db_rate AS (
    sec integer,
    seq_scan_per_sec double precision,
    idx_scan_per_sec double precision,
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

CREATE OR REPLACE FUNCTION powa_all_relations_history_db_div(
    a powa_all_relations_history_db_record,
    b powa_all_relations_history_db_record)
RETURNS powa_all_relations_history_db_rate AS
$_$
DECLARE
    res powa_all_relations_history_db_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.seq_scan_per_sec = (a.seq_scan - b.seq_scan)::double precision / sec;
    res.idx_scan_per_sec = (a.idx_scan - b.idx_scan)::double precision / sec;
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
    PROCEDURE = powa_all_relations_history_db_div,
    LEFTARG = powa_all_relations_history_db_record,
    RIGHTARG = powa_all_relations_history_db_record
);
/* end of pg_stat_all_relations_db operator support */

CREATE TYPE powa_stat_bgwriter_history_record AS (
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
CREATE TYPE powa_stat_bgwriter_history_diff AS (
    intvl interval,
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time double precision,
    buffers_checkpoint double precision,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint
);

CREATE OR REPLACE FUNCTION powa_stat_bgwriter_history_mi(
    a powa_stat_bgwriter_history_record,
    b powa_stat_bgwriter_history_record)
RETURNS powa_stat_bgwriter_history_diff AS
$_$
DECLARE
    res powa_stat_bgwriter_history_diff;
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

CREATE OPERATOR - (
    PROCEDURE = powa_stat_bgwriter_history_mi,
    LEFTARG = powa_stat_bgwriter_history_record,
    RIGHTARG = powa_stat_bgwriter_history_record
);

CREATE TYPE powa_stat_bgwriter_history_rate AS (
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

CREATE OR REPLACE FUNCTION powa_stat_bgwriter_history_div(
    a powa_stat_bgwriter_history_record,
    b powa_stat_bgwriter_history_record)
RETURNS powa_stat_bgwriter_history_rate AS
$_$
DECLARE
    res powa_stat_bgwriter_history_rate;
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

CREATE OPERATOR / (
    PROCEDURE = powa_stat_bgwriter_history_div,
    LEFTARG = powa_stat_bgwriter_history_record,
    RIGHTARG = powa_stat_bgwriter_history_record
);
/* end of pg_stat_bgwriter operator support */


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

CREATE UNLOGGED TABLE public.powa_stat_bgwriter_src_tmp(
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

CREATE TABLE powa_all_relations_history_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_all_relations_history_db_record[] NOT NULL,
    mins_in_range powa_all_relations_history_db_record NOT NULL,
    maxs_in_range powa_all_relations_history_db_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX powa_all_relations_history_db_dbid_ts ON powa_all_relations_history_db USING gist (srvid, dbid, coalesce_range);

CREATE TABLE powa_all_relations_history_current (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    record powa_all_relations_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_all_relations_history_current(srvid);

CREATE TABLE powa_all_relations_history_current_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    record powa_all_relations_history_db_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_all_relations_history_current_db(srvid);

CREATE TABLE powa_stat_bgwriter_history (
    srvid integer NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_stat_bgwriter_history_record[] NOT NULL,
    mins_in_range powa_stat_bgwriter_history_record NOT NULL,
    maxs_in_range powa_stat_bgwriter_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX powa_stat_bgwriter_history_ts ON powa_stat_bgwriter_history USING gist (srvid, coalesce_range);

CREATE TABLE powa_stat_bgwriter_history_current(
    srvid integer NOT NULL,
    record powa_stat_bgwriter_history_record NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_stat_bgwriter_history_current(srvid);


CREATE TABLE public.powa_extensions (
    srvid integer,
    extname text,
    version text,
    PRIMARY KEY (srvid, extname),
    FOREIGN KEY (srvid) REFERENCES public.powa_servers (id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO public.powa_extensions(srvid, extname) VALUES
    (0, 'pg_stat_statements'),
    (0, 'powa');

CREATE TABLE public.powa_functions (
    srvid integer NOT NULL,
    module text NOT NULL,
    operation text NOT NULL,
    function_name text NOT NULL,
    query_source text default NULL,
    query_cleanup text default NULL,
    added_manually boolean NOT NULL default true,
    enabled boolean NOT NULL default true,
    priority numeric NOT NULL default 10,
    extname text,
    CHECK (operation IN ('snapshot','aggregate','purge','unregister','reset')),
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (srvid, extname)
      REFERENCES public.powa_extensions (srvid, extname)
      ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO public.powa_functions (srvid, extname, module, operation, function_name, query_source, added_manually, enabled, priority) VALUES
    (0, 'pg_stat_statements', 'pg_stat_statements',       'snapshot',  'powa_databases_snapshot',       'powa_databases_src',      false, true, -3),
    (0, 'pg_stat_statements', 'pg_stat_statements',       'snapshot',  'powa_statements_snapshot',      'powa_statements_src',     false, true, -2),
    (0, 'powa', 'powa_stat_user_functions', 'snapshot',  'powa_user_functions_snapshot',  'powa_user_functions_src', false, true, default),
    (0, 'powa', 'powa_stat_all_relations',  'snapshot',  'powa_all_relations_snapshot',   'powa_all_relations_src',  false, true, default),
    (0, NULL, 'pg_stat_bgwriter',         'snapshot',  'powa_stat_bgwriter_snapshot',   'powa_stat_bgwriter_src',  false, true, default),
    (0, 'pg_stat_statements', 'pg_stat_statements',       'aggregate', 'powa_statements_aggregate',     NULL,                      false, true, default),
    (0, 'powa', 'powa_stat_user_functions', 'aggregate', 'powa_user_functions_aggregate', NULL,                      false, true, default),
    (0, 'powa', 'powa_stat_all_relations',  'aggregate', 'powa_all_relations_aggregate',  NULL,                      false, true, default),
    (0, NULL, 'pg_stat_bgwriter',         'aggregate', 'powa_stat_bgwriter_aggregate',  NULL,                      false, true, default),
    (0, 'pg_stat_statements', 'pg_stat_statements',       'purge',     'powa_statements_purge',         NULL,                      false, true, default),
    (0, 'pg_stat_statements', 'pg_stat_statements',       'purge',     'powa_databases_purge',          NULL,                      false, true, default),
    (0, 'powa', 'powa_stat_user_functions', 'purge',     'powa_user_functions_purge',     NULL,                      false, true, default),
    (0, 'powa', 'powa_stat_all_relations',  'purge',     'powa_all_relations_purge',      NULL,                      false, true, default),
    (0, NULL, 'pg_stat_bgwriter',         'purge',     'powa_stat_bgwriter_purge',      NULL,                      false, true, default),
    (0, 'pg_stat_statements', 'pg_stat_statements',       'reset',     'powa_statements_reset',         NULL,                      false, true, default),
    (0, 'powa', 'powa_stat_user_functions', 'reset',     'powa_user_functions_reset',     NULL,                      false, true, default),
    (0, 'powa', 'powa_stat_all_relations',  'reset',     'powa_all_relations_reset',      NULL,                      false, true, default),
    (0, NULL, 'pg_stat_bgwriter',         'reset',     'powa_stat_bgwriter_reset',      NULL,                      false, true, default);

-- Register the module if needed, and set the enabled flag to on.  This
-- function should only be callsed by powa_register_server.
CREATE FUNCTION public.powa_activate_extension(_srvid integer, _module text) RETURNS boolean
AS $_$
DECLARE
    v_ext_registered boolean;
    v_manually boolean;
    v_found boolean;
    v_extname text;
BEGIN
    SELECT COUNT(*) > 0 INTO v_ext_registered
    FROM public.powa_functions
    WHERE module = _module
    AND srvid = _srvid;

    IF (_module LIKE 'powa%') THEN
        v_extname = 'powa';
    ELSIF (_module = 'pg_stat_bgwriter') THEN
        v_extname = NULL;
    ELSE
        v_extname = _module;
    END IF;

    -- the rows may already be present, but the enabled flag could be off,
    -- so enabled it everywhere it's disabled.  We don't check for other cases,
    -- for instance if part of the needed rows were deleted.
    IF (v_ext_registered) THEN
        UPDATE public.powa_functions
        SET enabled = true
        WHERE enabled = false
        AND srvid = _srvid
        AND module = _module;

        RETURN true;
    END IF;

    -- Add the row in powa_extensions if needed.  Note that since we add the
    -- row before knowing if it's a supported extension, we may have to remove
    -- it later.
    IF (v_extname IS NOT NULL) THEN
        SELECT COUNT(*) = 1 INTO v_found
        FROM public.powa_extensions
        WHERE srvid = _srvid
        AND extname = v_extname;

        IF NOT v_found THEN
            INSERT INTO public.powa_extensions (srvid, extname)
            VALUES (_srvid, v_extname);
        END IF;
    END IF;

    -- default extensions for non-local server have to be dumped
    SELECT _srvid != 0 INTO v_manually;

    IF (_module = 'pg_stat_statements') THEN
        INSERT INTO public.powa_functions(srvid, extname, module, operation, function_name,
            query_source, added_manually, enabled, priority)
        VALUES
        (_srvid, 'pg_stat_statements', 'pg_stat_statements', 'snapshot',  'powa_databases_snapshot',   'powa_databases_src',  v_manually, true, -1),
        (_srvid, 'pg_stat_statements', 'pg_stat_statements', 'snapshot',  'powa_statements_snapshot',  'powa_statements_src', v_manually, true, default),
        (_srvid, 'pg_stat_statements', 'pg_stat_statements', 'aggregate', 'powa_statements_aggregate', NULL,                  v_manually, true, default),
        (_srvid, 'pg_stat_statements', 'pg_stat_statements', 'purge',     'powa_statements_purge',     NULL,                  v_manually, true, default),
        (_srvid, 'pg_stat_statements', 'pg_stat_statements', 'purge',     'powa_databases_purge',      NULL,                  v_manually, true, default),
        (_srvid, 'pg_stat_statements', 'pg_stat_statements', 'reset',     'powa_statements_reset',     NULL,                  v_manually, true, default);
    ELSIF (_module = 'powa_stat_user_functions') THEN
        INSERT INTO public.powa_functions(srvid, extname, module, operation, function_name,
            query_source, added_manually, enabled, priority)
        VALUES
         (_srvid, 'powa', 'powa_stat_user_functions', 'snapshot',  'powa_user_functions_snapshot',  'powa_user_functions_src', v_manually, true, default),
         (_srvid, 'powa', 'powa_stat_user_functions', 'aggregate', 'powa_user_functions_aggregate', NULL,                      v_manually, true, default),
         (_srvid, 'powa', 'powa_stat_user_functions', 'purge',     'powa_user_functions_purge',     NULL,                      v_manually, true, default),
         (_srvid, 'powa', 'powa_stat_user_functions', 'reset',     'powa_user_functions_reset',     NULL,                      v_manually, true, default);
    ELSIF (_module = 'powa_stat_all_relations') THEN
        INSERT INTO public.powa_functions(srvid, extname, module, operation, function_name,
            query_source, added_manually, enabled, priority)
        VALUES
        (_srvid, 'powa', 'powa_stat_all_relations',  'snapshot',  'powa_all_relations_snapshot',   'powa_all_relations_src',  v_manually, true, default),
        (_srvid, 'powa', 'powa_stat_all_relations',  'aggregate', 'powa_all_relations_aggregate',  NULL,                      v_manually, true, default),
        (_srvid, 'powa', 'powa_stat_all_relations',  'purge',     'powa_all_relations_purge',      NULL,                      v_manually, true, default),
        (_srvid, 'powa', 'powa_stat_all_relations',  'reset',     'powa_all_relations_reset',      NULL,                      v_manually, true, default);
    ELSIF (_module = 'pg_stat_bgwriter') THEN
        INSERT INTO public.powa_functions(srvid, extname, module, operation, function_name,
            query_source, added_manually, enabled, priority)
        VALUES
        (_srvid, NULL, 'pg_stat_bgwriter',  'snapshot',  'powa_stat_bgwriter_snapshot',   'powa_stat_bgwriter_src',  v_manually, true, default),
        (_srvid, NULL, 'pg_stat_bgwriter',  'aggregate', 'powa_stat_bgwriter_aggregate',  NULL,                      v_manually, true, default),
        (_srvid, NULL, 'pg_stat_bgwriter',  'purge',     'powa_stat_bgwriter_purge',      NULL,                      v_manually, true, default),
        (_srvid, NULL, 'pg_stat_bgwriter',  'reset',     'powa_stat_bgwriter_reset',      NULL,                      v_manually, true, default);
    ELSIF (_module = 'pg_stat_kcache') THEN
        RETURN public.powa_kcache_register(_srvid);
    ELSIF (_module = 'pg_qualstats') THEN
        RETURN public.powa_qualstats_register(_srvid);
    ELSIF (_module = 'pg_wait_sampling') THEN
        RETURN public.powa_wait_sampling_register(_srvid);
    ELSIF (_module = 'pg_track_settings') THEN
        RETURN public.powa_track_settings_register(_srvid);
    ELSE
        -- remove the previously added row in powa_extensions
        IF (v_extname IS NOT NULL) THEN
            DELETE FROM public.powa_extensions
                WHERE srvid = _srvid AND extname = v_extname;
        END IF;

        RETURN false;
    END IF;

    return true;
END;
$_$ LANGUAGE plpgsql; /* end of powa_activate_extension */

CREATE FUNCTION powa_deactivate_extension(_srvid integer, _module text) RETURNS boolean
AS $_$
BEGIN
    UPDATE public.powa_functions
    SET enabled = false
    WHERE module = _module
    AND srvid = _srvid;

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

    INSERT INTO public.powa_servers
        (alias, hostname, port, username, password, dbname, frequency, powa_coalesce, retention, allow_ui_connection)
    VALUES
        (alias, hostname, port, username, password, dbname, frequency, powa_coalesce, retention, allow_ui_connection)
    RETURNING id INTO v_srvid;

    INSERT INTO public.powa_snapshot_metas(srvid) VALUES (v_srvid);

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
    SELECT powa_activate_extension(v_srvid, 'pg_stat_bgwriter') INTO v_ok;
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

CREATE FUNCTION powa_configure_server(_srvid integer, data json) RETURNS boolean
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

    v_query := 'UPDATE public.powa_servers SET '
        || v_query
        || format(' WHERE id = %s', _srvid);

    EXECUTE v_query;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    RETURN v_rowcount = 1;
END;
$_$ LANGUAGE plpgsql; /* powa_config_server */

CREATE FUNCTION powa_deactivate_server(_srvid integer) RETURNS boolean
AS $_$
DECLARE
    v_rowcount bigint;
BEGIN
    IF (_srvid = 0) THEN
        RAISE EXCEPTION 'Local server cannot be updated';
    END IF;

    UPDATE public.powa_servers SET frequency = -1 WHERE id = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    RETURN v_rowcount = 1;
END;
$_$ LANGUAGE plpgsql; /* powa_deactivate_server */

CREATE FUNCTION powa_delete_and_purge_server(_srvid integer) RETURNS boolean
AS $_$
DECLARE
    v_rowcount bigint;
BEGIN
    IF (_srvid = 0) THEN
        RAISE EXCEPTION 'Local server cannot be deleted';
    END IF;

    DELETE FROM public.powa_servers WHERE id = _srvid;

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
        CREATE FUNCTION public.powa_get_guc (guc text, def text DEFAULT NULL) RETURNS text
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
        CREATE FUNCTION public.powa_get_guc (guc text, def text DEFAULT NULL) RETURNS text
        LANGUAGE plpgsql
        AS $_$
        BEGIN
            RETURN COALESCE(current_setting(guc, true), def);
        END;
        $_$;
    END IF;
END;
$anon$;

CREATE FUNCTION public.powa_log (msg text) RETURNS void
LANGUAGE plpgsql
AS $_$
BEGIN
    IF powa_get_guc('powa.debug', 'false')::bool THEN
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
    srvid            integer NOT NULL,
    ts               timestamp with time zone NOT NULL,
    queryid          bigint NOT NULL,
    top              bool NOT NULL,
    userid           oid NOT NULL,
    dbid             oid NOT NULL,
    plan_reads       bigint NOT NULL,
    plan_writes      bigint NOT NULL,
    plan_user_time   double precision NOT NULL,
    plan_system_time double precision NOT NULL,
    plan_minflts     bigint NOT NULL,
    plan_majflts     bigint NOT NULL,
    plan_nswaps      bigint NOT NULL,
    plan_msgsnds     bigint NOT NULL,
    plan_msgrcvs     bigint NOT NULL,
    plan_nsignals    bigint NOT NULL,
    plan_nvcsws      bigint NOT NULL,
    plan_nivcsws     bigint NOT NULL,
    exec_reads       bigint NOT NULL,
    exec_writes      bigint NOT NULL,
    exec_user_time   double precision NOT NULL,
    exec_system_time double precision NOT NULL,
    exec_minflts     bigint NOT NULL,
    exec_majflts     bigint NOT NULL,
    exec_nswaps      bigint NOT NULL,
    exec_msgsnds     bigint NOT NULL,
    exec_msgrcvs     bigint NOT NULL,
    exec_nsignals    bigint NOT NULL,
    exec_nvcsws      bigint NOT NULL,
    exec_nivcsws     bigint NOT NULL
);

CREATE TYPE public.powa_kcache_type AS (
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
CREATE TYPE powa_kcache_diff AS (
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

CREATE OR REPLACE FUNCTION public.powa_kcache_mi(
    a powa_kcache_type,
    b powa_kcache_type)
RETURNS powa_kcache_diff AS
$_$
DECLARE
    res powa_kcache_diff;
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

CREATE OPERATOR - (
    PROCEDURE = powa_kcache_mi,
    LEFTARG = powa_kcache_type,
    RIGHTARG = powa_kcache_type
);

CREATE TYPE powa_kcache_rate AS (
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

CREATE OR REPLACE FUNCTION public.powa_kcache_div(
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
    res.plan_reads_per_sec = (a.plan_reads - b.plan_reads)::double precision / sec;
    res.plan_writes_per_sec = (a.plan_writes - b.plan_writes)::double precision / sec;
    res.plan_user_time_per_sec = (a.plan_user_time - b.plan_user_time)::double precision / sec;
    res.plan_system_time_per_sec = (a.plan_system_time - b.plan_system_time)::double precision / sec;
    res.plan_minflts_per_sec = (a.plan_minflts_per_sec - b.plan_minflts_per_sec)::double precision / sec;
    res.plan_majflts_per_sec = (a.plan_majflts_per_sec - b.plan_majflts_per_sec)::double precision / sec;
    res.plan_nswaps_per_sec = (a.plan_nswaps_per_sec - b.plan_nswaps_per_sec)::double precision / sec;
    res.plan_msgsnds_per_sec = (a.plan_msgsnds_per_sec - b.plan_msgsnds_per_sec)::double precision / sec;
    res.plan_msgrcvs_per_sec = (a.plan_msgrcvs_per_sec - b.plan_msgrcvs_per_sec)::double precision / sec;
    res.plan_nsignals_per_sec = (a.plan_nsignals_per_sec - b.plan_nsignals_per_sec)::double precision / sec;
    res.plan_nvcsws_per_sec = (a.plan_nvcsws_per_sec - b.plan_nvcsws_per_sec)::double precision / sec;
    res.plan_nivcsws_per_sec = (a.plan_nivcsws_per_sec - b.plan_nivcsws_per_sec)::double precision / sec;
    res.exec_reads_per_sec = (a.exec_reads - b.exec_reads)::double precision / sec;
    res.exec_writes_per_sec = (a.exec_writes - b.exec_writes)::double precision / sec;
    res.exec_user_time_per_sec = (a.exec_user_time - b.exec_user_time)::double precision / sec;
    res.exec_system_time_per_sec = (a.exec_system_time - b.exec_system_time)::double precision / sec;
    res.exec_minflts_per_sec = (a.exec_minflts_per_sec - b.exec_minflts_per_sec)::double precision / sec;
    res.exec_majflts_per_sec = (a.exec_majflts_per_sec - b.exec_majflts_per_sec)::double precision / sec;
    res.exec_nswaps_per_sec = (a.exec_nswaps_per_sec - b.exec_nswaps_per_sec)::double precision / sec;
    res.exec_msgsnds_per_sec = (a.exec_msgsnds_per_sec - b.exec_msgsnds_per_sec)::double precision / sec;
    res.exec_msgrcvs_per_sec = (a.exec_msgrcvs_per_sec - b.exec_msgrcvs_per_sec)::double precision / sec;
    res.exec_nsignals_per_sec = (a.exec_nsignals_per_sec - b.exec_nsignals_per_sec)::double precision / sec;
    res.exec_nvcsws_per_sec = (a.exec_nvcsws_per_sec - b.exec_nvcsws_per_sec)::double precision / sec;
    res.exec_nivcsws_per_sec = (a.exec_nivcsws_per_sec - b.exec_nivcsws_per_sec)::double precision / sec;

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
    top boolean NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, queryid, dbid, userid, top),
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
    top boolean NOT NULL,
    PRIMARY KEY (srvid, coalesce_range, dbid, top),
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE public.powa_kcache_metrics_current ( srvid integer NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    metrics powa_kcache_type NULL NULL,
    top boolean NOT NULL,
    FOREIGN KEY (srvid) REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON powa_kcache_metrics_current(srvid);

CREATE TABLE public.powa_kcache_metrics_current_db (
    srvid integer NOT NULL,
    dbid oid NOT NULL,
    metrics powa_kcache_type NULL NULL,
    top boolean NOT NULL,
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
    nbfiltered bigint,
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision
);

CREATE TYPE powa_qualstats_history_item AS (
  ts timestamptz,
  occurences bigint,
  execution_count bigint,
  nbfiltered bigint,
  mean_err_estimate_ratio double precision,
  mean_err_estimate_num double precision
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
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision,
    queryid bigint NOT NULL,
    constvalues varchar[] NOT NULL,
    quals qual_type[] NOT NULL
);

/* pg_qualstats operator support */
CREATE TYPE powa_qualstats_history_diff AS (
    intvl interval,
    occurences bigint,
    execution_count bigint,
    nbfiltered bigint,
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision
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
    res.mean_err_estimate_ratio = a.mean_err_estimate_ratio - b.mean_err_estimate_ratio;
    res.mean_err_estimate_num = a.mean_err_estimate_num - b.mean_err_estimate_num;

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
    nbfiltered_per_sec double precision,
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision
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
    res.mean_err_estimate_ratio = (a.mean_err_estimate_ratio - b.mean_err_estimate_ratio)::double precision / sec;
    res.mean_err_estimate_num = (a.mean_err_estimate_num - b.mean_err_estimate_num)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR / (
    PROCEDURE = powa_qualstats_history_div,
    LEFTARG = powa_qualstats_history_item,
    RIGHTARG = powa_qualstats_history_item
);
/* end of pg_qualstats operator support */

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
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision,
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
    most_errestim_ratio qual_values[],
    most_errestim_num qual_values[],
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
    mean_err_estimate_ratio double precision,
    mean_err_estimate_num double precision,
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
    srvid integer NOT NULL REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
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
    srvid integer NOT NULL REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
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
    srvid integer NOT NULL REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    record wait_sampling_type NOT NULL
);
CREATE INDEX ON powa_wait_sampling_history_current(srvid);

CREATE TABLE public.powa_wait_sampling_history_current_db (
    srvid integer NOT NULL REFERENCES powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    record wait_sampling_type NOT NULL
);
CREATE INDEX ON powa_wait_sampling_history_current_db(srvid);

/* end of pg_wait_sampling integration - part 1 */

-- Mark all of powa's tables as "to be dumped"
SELECT pg_catalog.pg_extension_config_dump('powa_servers','WHERE id > 0');
SELECT pg_catalog.pg_extension_config_dump('powa_snapshot_metas','WHERE srvid > 0');
SELECT pg_catalog.pg_extension_config_dump('powa_databases','');
SELECT pg_catalog.pg_extension_config_dump('powa_statements','');
SELECT pg_catalog.pg_extension_config_dump('powa_statements_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_statements_history_db','');
SELECT pg_catalog.pg_extension_config_dump('powa_statements_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_statements_history_current_db','');
SELECT pg_catalog.pg_extension_config_dump('powa_user_functions_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_user_functions_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_all_relations_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_all_relations_history_db','');
SELECT pg_catalog.pg_extension_config_dump('powa_all_relations_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_all_relations_history_current_db','');
SELECT pg_catalog.pg_extension_config_dump('powa_stat_bgwriter_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_stat_bgwriter_history_current','');
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

-- automatically configure powa for local snapshot if supported extension are
-- created locally
CREATE OR REPLACE FUNCTION public.powa_check_created_extensions()
RETURNS event_trigger
LANGUAGE plpgsql
AS $_$
DECLARE
BEGIN

    /* We have for now no way for a proper handling of this event,
     * as we don't have a table with the list of supported extensions.
     * So just call every powa_*_register() function we know each time an
     * extension is created. Powa should be in a dedicated database and the
     * register function handle to be called several time, so it's not critical
     */
    PERFORM public.powa_activate_extension(0, 'pg_stat_kcache');
    PERFORM public.powa_activate_extension(0, 'pg_qualstats');
    PERFORM public.powa_activate_extension(0, 'pg_track_settings');
    PERFORM public.powa_activate_extension(0, 'pg_wait_sampling');
END;
$_$; /* end of powa_check_created_extensions */

CREATE EVENT TRIGGER powa_check_created_extensions
    ON ddl_command_end
    WHEN tag IN ('CREATE EXTENSION')
    EXECUTE PROCEDURE public.powa_check_created_extensions() ;

-- automatically remove extensions from local snapshot if supported extension
-- is removed locally
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
    -- We unregister extensions regardless the "enabled" field
    WITH ext AS (
        SELECT object_name
        FROM pg_event_trigger_dropped_objects() d
        WHERE d.object_type = 'extension'
    )
    SELECT function_name INTO funcname
    FROM public.powa_functions f
    JOIN ext ON f.module = ext.object_name
    WHERE operation = 'unregister'
    ORDER BY module;

    IF ( funcname IS NOT NULL ) THEN
        BEGIN
            PERFORM public.powa_log(format('running %I', funcname));
            EXECUTE 'SELECT ' || quote_ident(funcname) || '(0)';
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
DECLARE
    v_state   text;
    v_msg     text;
    v_detail  text;
    v_hint    text;
    v_context text;
BEGIN
    BEGIN
        PERFORM 1
        FROM powa_snapshot_metas
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
  v_coalesce bigint;
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

    IF (_srvid = 0) THEN
        SELECT current_setting('powa.coalesce') INTO v_coalesce;
    ELSE
        SELECT powa_coalesce
        FROM public.powa_servers
        WHERE id = _srvid
        INTO v_coalesce;
    END IF;

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
    IF ( (purge_seq % v_coalesce ) = 0 )
    THEN
      PERFORM powa_log(
        format('coalesce needed, srvid: %s - seq: %s - coalesce seq: %s',
        _srvid, purge_seq, v_coalesce ));

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
    IF ( (purge_seq % v_coalesce) = 1 )
    THEN
      PERFORM powa_log(
        format('purge needed, srvid: %s - seq: %s coalesce seq: %s',
        _srvid, purge_seq, v_coalesce));

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
RETURNS SETOF record
STABLE
AS $PROC$
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
    INSERT INTO public.powa_databases
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
BEGIN
    IF (_srvid = 0) THEN
        SELECT regexp_split_to_array(extversion, '\.') INTO STRICT v_pgss
        FROM pg_extension
        WHERE extname = 'pg_stat_statements';

        IF (v_pgss[1] = 1 AND v_pgss[2] < 8) THEN
            RETURN QUERY SELECT now(),
                pgss.userid, pgss.dbid, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written, pgss.blk_read_time,pgss.blk_write_time,
                0::bigint, 0::double precision,
                0::bigint, 0::bigint, 0::numeric

            FROM pg_stat_statements pgss
            JOIN pg_database d ON d.oid = pgss.dbid
            JOIN pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        powa_get_guc('powa.ignored_users', ''),
                        ',')));
        ELSE
            RETURN QUERY SELECT now(),
                pgss.userid, pgss.dbid, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_exec_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time,
                pgss.plans, pgss.total_plan_time,
                pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes
            FROM pg_stat_statements pgss
            JOIN pg_database d ON d.oid = pgss.dbid
            JOIN pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        powa_get_guc('powa.ignored_users', ''),
                        ',')));
        END IF;
    ELSE
        RETURN QUERY SELECT pgss.ts,
            pgss.userid, pgss.dbid, pgss.queryid, pgss.query,
            pgss.calls, pgss.total_exec_time,
            pgss.rows, pgss.shared_blks_hit,
            pgss.shared_blks_read, pgss.shared_blks_dirtied,
            pgss.shared_blks_written, pgss.local_blks_hit,
            pgss.local_blks_read, pgss.local_blks_dirtied,
            pgss.local_blks_written, pgss.temp_blks_read,
            pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time,
            pgss.plans, pgss.total_plan_time,
            pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes
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
    mru as (UPDATE powa_statements set last_present_ts = now()
            FROM capture
            WHERE powa_statements.queryid = capture.queryid
              AND powa_statements.dbid = capture.dbid
              AND powa_statements.userid = capture.userid
              AND powa_statements.srvid = _srvid
    ),
    missing_statements AS(
        INSERT INTO public.powa_statements (srvid, queryid, dbid, userid, query)
            SELECT _srvid, queryid, dbid, userid, min(query)
            FROM capture c
            WHERE NOT EXISTS (SELECT 1
                              FROM powa_statements ps
                              WHERE ps.queryid = c.queryid
                              AND ps.dbid = c.dbid
                              AND ps.userid = c.userid
                              AND ps.srvid = _srvid
            )
            GROUP BY queryid, dbid, userid
    ),

    by_query AS (
        INSERT INTO public.powa_statements_history_current
            SELECT _srvid, queryid, dbid, userid,
            ROW(
                ts, calls, total_exec_time, rows,
                shared_blks_hit, shared_blks_read, shared_blks_dirtied,
                shared_blks_written, local_blks_hit, local_blks_read,
                local_blks_dirtied, local_blks_written, temp_blks_read,
                temp_blks_written, blk_read_time, blk_write_time,
                plans, total_plan_time,
                wal_records, wal_fpi, wal_bytes
            )::powa_statements_history_record AS record
            FROM capture
    ),

    by_database AS (
        INSERT INTO public.powa_statements_history_current_db
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
) RETURNS SETOF record
STABLE
AS $PROC$
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
    INSERT INTO public.powa_user_functions_history_current
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
) RETURNS SETOF record STABLE AS $PROC$
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
    ),

    by_relation AS (
        INSERT INTO public.powa_all_relations_history_current
            SELECT _srvid, dbid, relid,
            ROW(ts,numscan, tup_returned, tup_fetched,
                n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
                n_liv_tup, n_dead_tup, n_mod_since_analyze,
                blks_read, blks_hit, last_vacuum, vacuum_count,
                last_autovacuum, autovacuum_count, last_analyze,
                analyze_count, last_autoanalyze,
                autoanalyze_count)::powa_all_relations_history_record AS record
            FROM rel
    ),

    by_database AS (
        INSERT INTO public.powa_all_relations_history_current_db (srvid, dbid, record)
            SELECT _srvid AS srvid, dbid,
            ROW(ts,
                sum(CASE WHEN
                        (n_tup_ins + n_tup_upd + n_tup_del + n_tup_hot_upd +
                         n_liv_tup + n_dead_tup + n_mod_since_analyze + vacuum_count +
                         autovacuum_count + analyze_count + autoanalyze_count) = 0
                    THEN 0 ELSE numscan END),
                sum(CASE WHEN
                        (n_tup_ins + n_tup_upd + n_tup_del + n_tup_hot_upd +
                         n_liv_tup + n_dead_tup + n_mod_since_analyze + vacuum_count +
                         autovacuum_count + analyze_count + autoanalyze_count) = 0
                    THEN numscan ELSE 0 END),
                sum(rel.tup_returned), sum(rel.tup_fetched),
                sum(n_tup_ins), sum(n_tup_upd), sum(n_tup_del), sum(n_tup_hot_upd),
                sum(n_liv_tup), sum(n_dead_tup), sum(n_mod_since_analyze),
                sum(rel.blks_read), sum(rel.blks_hit),
                sum(vacuum_count), sum(autovacuum_count),
                sum(analyze_count), sum(autoanalyze_count)
            )::powa_all_relations_history_db_record
            FROM rel
            GROUP BY srvid, dbid, ts
    )
 
    SELECT COUNT(*) into v_rowcount
    FROM rel;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_all_relations_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_all_relations_snapshot */

CREATE OR REPLACE FUNCTION powa_stat_bgwriter_src(IN _srvid integer,
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
        FROM powa_stat_bgwriter_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of pg_stat_bgwriter_src */

CREATE OR REPLACE FUNCTION powa_stat_bgwriter_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_stat_bgwriter_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- Insert background writer statistics
    WITH rel AS (
        SELECT *
        FROM powa_stat_bgwriter_src(_srvid)
    )
    INSERT INTO public.powa_stat_bgwriter_history_current
        SELECT _srvid,
        ROW(ts, checkpoints_timed, checkpoints_req, checkpoint_write_time,
            checkpoint_sync_time, buffers_checkpoint, buffers_clean,
            maxwritten_clean, buffers_backend, buffers_backend_fsync,
            buffers_alloc)::powa_stat_bgwriter_history_record AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_stat_bgwriter_src_tmp WHERE srvid = _srvid;
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

    -- Delete obsolete data. We only bother with already coalesced data
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

    DELETE FROM powa_statements
    WHERE last_present_ts < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements) - rowcount: %s',
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
    perform powa_log(format('%I - (powa_all_relations_history) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_all_relations_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - (powa_all_relations_history_db) rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_relations_purge */

CREATE OR REPLACE FUNCTION powa_stat_bgwriter_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_stat_bgwriter_purge(' || _srvid || ')';
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    SELECT powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_stat_bgwriter_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_bgwriter_purge */


CREATE OR REPLACE FUNCTION powa_statements_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_statements_aggregate(' || _srvid || ')';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate statements table
    INSERT INTO public.powa_statements_history
        SELECT srvid, queryid, dbid, userid,
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
            )::powa_statements_history_record,
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
            )::powa_statements_history_record
        FROM powa_statements_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, queryid, dbid, userid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_statements_history_current WHERE srvid = _srvid;

    -- aggregate db table
    INSERT INTO public.powa_statements_history_db
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
            )::powa_statements_history_record,
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
            )::powa_statements_history_record
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
    INSERT INTO public.powa_user_functions_history
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
    INSERT INTO public.powa_all_relations_history
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
    perform powa_log(format('%I - (powa_all_relations_history_current) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_all_relations_history_current WHERE srvid = _srvid;

    -- aggregate all_relations_db table
    INSERT INTO public.powa_all_relations_history_db
        SELECT srvid, dbid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                min((record).seq_scan),min((record).idx_scan),
                min((record).tup_returned),min((record).tup_fetched),
                min((record).n_tup_ins),min((record).n_tup_upd),
                min((record).n_tup_del),min((record).n_tup_hot_upd),
                min((record).n_liv_tup),min((record).n_dead_tup),
                min((record).n_mod_since_analyze),
                min((record).blks_read),min((record).blks_hit),
                min((record).vacuum_count),min((record).autovacuum_count),
                min((record).analyze_count),min((record).autoanalyze_count)
            )::powa_all_relations_history_db_record,
            ROW(max((record).ts),
                max((record).seq_scan),max((record).idx_scan),
                max((record).tup_returned),max((record).tup_fetched),
                max((record).n_tup_ins),max((record).n_tup_upd),
                max((record).n_tup_del),max((record).n_tup_hot_upd),
                max((record).n_liv_tup),max((record).n_dead_tup),
                max((record).n_mod_since_analyze),
                max((record).blks_read),max((record).blks_hit),
                max((record).vacuum_count),max((record).autovacuum_count),
                max((record).analyze_count),max((record).autoanalyze_count)
            )::powa_all_relations_history_db_record
        FROM powa_all_relations_history_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_all_relations_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_all_relations_history_current_db WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_relations_aggregate */

CREATE OR REPLACE FUNCTION powa_stat_bgwriter_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_stat_bgwriter_aggregate(' || _srvid || ')';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate bgwriter table
    INSERT INTO public.powa_stat_bgwriter_history
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
                min((record).buffers_alloc))::powa_stat_bgwriter_history_record,
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
                max((record).buffers_alloc))::powa_stat_bgwriter_history_record
        FROM powa_stat_bgwriter_history_current
        WHERE srvid = _srvid
        GROUP BY srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_stat_bgwriter_history_current WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_bgwriter_aggregate */

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
    PERFORM public.powa_log('Resetting powa_statements_history(' || _srvid || ')');
    DELETE FROM public.powa_statements_history WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_statements_history_current(' || _srvid || ')');
    DELETE FROM public.powa_statements_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_statements_history_db(' || _srvid || ')');
    DELETE FROM public.powa_statements_history_db WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_statements_history_current_db(' || _srvid || ')');
    DELETE FROM public.powa_statements_history_current_db WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_statements_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_statements_src_tmp WHERE srvid = _srvid;

    -- if 3rd part datasource has FK on it, throw everything away
    DELETE FROM public.powa_statements WHERE srvid = _srvid;
    PERFORM public.powa_log('Resetting powa_statements(' || _srvid || ')');

    RETURN true;
END;
$function$; /* end of powa_statements_reset */

CREATE OR REPLACE FUNCTION public.powa_user_functions_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM public.powa_log('Resetting powa_user_functions_history(' || _srvid || ')');
    DELETE FROM public.powa_user_functions_history WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_user_functions_history_current(' || _srvid || ')');
    DELETE FROM public.powa_user_functions_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_user_functions_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_user_functions_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_user_functions_reset */

CREATE OR REPLACE FUNCTION public.powa_all_relations_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM public.powa_log('Resetting powa_all_relations_history(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_history WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_all_relations_history_db(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_history_db WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_all_relations_history_current(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_all_relations_history_current_db(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_history_current_db WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_all_relations_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_all_relations_reset */

CREATE OR REPLACE FUNCTION public.powa_stat_bgwriter_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM public.powa_log('Resetting powa_stat_bgwriter_history(' || _srvid || ')');
    DELETE FROM public.powa_stat_bgwriter_history WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_stat_bgwriter_history_current(' || _srvid || ')');
    DELETE FROM public.powa_stat_bgwriter_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_stat_bgwriter_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_stat_bgwriter_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_stat_bgwriter_reset */

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

            INSERT INTO public.powa_functions (srvid, extname, module, operation, function_name, query_source, added_manually, enabled, priority)
            VALUES (_srvid, 'pg_stat_kcache', 'pg_stat_kcache', 'snapshot',   'powa_kcache_snapshot',   'powa_kcache_src', true, true, -1),
                   (_srvid, 'pg_stat_kcache', 'pg_stat_kcache', 'aggregate',  'powa_kcache_aggregate',  NULL,              true, true, default),
                   (_srvid, 'pg_stat_kcache', 'pg_stat_kcache', 'unregister', 'powa_kcache_unregister', NULL,              true, true, default),
                   (_srvid, 'pg_stat_kcache', 'pg_stat_kcache', 'purge',      'powa_kcache_purge',      NULL,              true, true, default),
                   (_srvid, 'pg_stat_kcache', 'pg_stat_kcache', 'reset',      'powa_kcache_reset',      NULL,              true, true, default);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql; /* end of powa_kcache_register */

/*
 * unregister pg_stat_kcache extension
 */
CREATE OR REPLACE function public.powa_kcache_unregister(_srvid integer = 0) RETURNS bool AS
$_$
BEGIN
    PERFORM powa_log('unregistering pg_stat_kcache');
    DELETE FROM public.powa_functions
    WHERE module = 'pg_stat_kcache'
    AND srvid = _srvid;
    RETURN true;
END;
$_$
language plpgsql; /* end of powa_kcache_unregister */

CREATE OR REPLACE FUNCTION powa_kcache_src(IN _srvid integer,
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
BEGIN
    IF (_srvid = 0) THEN
        SELECT (
            (regexp_split_to_array(extversion, '\.')::int[])[1] >= 2 AND
            (regexp_split_to_array(extversion, '\.')::int[])[2] >= 2
        ) INTO is_v2_2
          FROM pg_extension
          WHERE extname = 'pg_stat_kcache';

        IF (is_v2_2 IS NOT DISTINCT FROM 'true'::bool) THEN
            RETURN QUERY SELECT now(),
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
            FROM pg_stat_kcache() k
            JOIN pg_roles r ON r.oid = k.userid
            WHERE NOT (r.rolname = ANY (string_to_array(
                        powa_get_guc('powa.ignored_users', ''),
                        ',')))
            AND k.dbid NOT IN (SELECT oid FROM powa_databases WHERE dropped IS NOT NULL);
        ELSE
            RETURN QUERY SELECT now(),
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
            FROM pg_stat_kcache() k
            JOIN pg_roles r ON r.oid = k.userid
            WHERE NOT (r.rolname = ANY (string_to_array(
                        powa_get_guc('powa.ignored_users', ''),
                        ',')))
            AND k.dbid NOT IN (SELECT oid FROM powa_databases WHERE dropped IS NOT NULL);
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
        INSERT INTO public.powa_kcache_metrics_current (srvid, queryid, top, dbid, userid, metrics)
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
        )::powa_kcache_type
            FROM capture
    ),

    by_database AS (
        INSERT INTO public.powa_kcache_metrics_current_db (srvid, top, dbid, metrics)
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
              )::powa_kcache_type
            FROM capture
            GROUP BY ts, srvid, top, dbid
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
    INSERT INTO public.powa_kcache_metrics (coalesce_range, srvid, queryid,
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
        )::powa_kcache_type,
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
        )::powa_kcache_type
        FROM powa_kcache_metrics_current
        WHERE srvid = _srvid
        GROUP BY srvid, queryid, top, dbid, userid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_kcache_metrics) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_kcache_metrics_current WHERE srvid = _srvid;

    -- aggregate metrics_db table
    INSERT INTO public.powa_kcache_metrics_db (srvid, coalesce_range, dbid,
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
        )::powa_kcache_type,
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
        )::powa_kcache_type
        FROM powa_kcache_metrics_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, top;

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
    PERFORM public.powa_log(format('running %I', v_funcname));

    PERFORM public.powa_log('resetting powa_kcache_metrics(' || _srvid || ')');
    DELETE FROM public.powa_kcache_metrics WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_kcache_metrics_db(' || _srvid || ')');
    DELETE FROM public.powa_kcache_metrics_db WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_kcache_metrics_current(' || _srvid || ')');
    DELETE FROM public.powa_kcache_metrics_current WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_kcache_metrics_current_db(' || _srvid || ')');
    DELETE FROM public.powa_kcache_metrics_current_db WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_kcache_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_kcache_src_tmp WHERE srvid = _srvid;
END;
$PROC$ language plpgsql; /* end of powa_kcache_reset */

-- By default, try to register pg_stat_kcache, in case it's alreay here
SELECT * FROM public.powa_activate_extension(0, 'pg_stat_kcache');

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

            INSERT INTO public.powa_functions (srvid, extname, module, operation, function_name, query_source, query_cleanup, added_manually, enabled)
            VALUES (_srvid, 'pg_qualstats', 'pg_qualstats', 'snapshot',   'powa_qualstats_snapshot',   'powa_qualstats_src', 'SELECT pg_qualstats_reset()', true, true),
                   (_srvid, 'pg_qualstats', 'pg_qualstats', 'aggregate',  'powa_qualstats_aggregate',  NULL,                 NULL,                          true, true),
                   (_srvid, 'pg_qualstats', 'pg_qualstats', 'unregister', 'powa_qualstats_unregister', NULL,                 NULL,                          true, true),
                   (_srvid, 'pg_qualstats', 'pg_qualstats', 'purge',      'powa_qualstats_purge',      NULL,                 NULL,                          true, true),
                   (_srvid, 'pg_qualstats', 'pg_qualstats', 'reset',      'powa_qualstats_reset',      NULL,                 NULL,                          true, true);
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
    OUT me qual_values[],
    OUT mer qual_values[],
    OUT men qual_values[])
RETURNS SETOF record STABLE AS $_$
SELECT
    -- Ordered aggregate of top 20 metrics for each kind of stats (most executed, most filetered, least filtered...)
    srvid, qualid, queryid, dbid, userid,
    tstzrange(min(min_constvalues_ts) , max(max_constvalues_ts) ,'[]') ,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::qual_values ORDER BY occurences_rank ASC) FILTER (WHERE occurences_rank <=20)  mu,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::qual_values ORDER BY filtered_rank ASC) FILTER (WHERE filtered_rank <=20)  mf,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::qual_values ORDER BY filtered_rank DESC) FILTER (WHERE filtered_rank >= nb_lines - 20)  lf, -- Keep last 20 lines from the same window function
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::qual_values ORDER BY execution_rank ASC) FILTER (WHERE execution_rank <=20)  me,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::qual_values ORDER BY err_estimate_ratio_rank ASC) FILTER (WHERE err_estimate_ratio_rank <=20)  mer,
    array_agg((constvalues, sum_occurences, sum_execution_count, sum_nbfiltered, avg_mean_err_estimate_ratio, avg_mean_err_estimate_num)::qual_values ORDER BY err_estimate_num_rank ASC) FILTER (WHERE err_estimate_num_rank <=20)  men
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
        FROM powa_qualstats_constvalues_history_current
        WHERE srvid = _srvid
          AND ts >= _ts_from AND ts <= _ts_to
        GROUP BY srvid, qualid, queryid, dbid, userid,constvalues
        ) distinct_constvalues
    WINDOW W AS (PARTITION BY srvid, qualid, queryid, dbid, userid)
    ) ranked_constvalues
GROUP BY srvid, qualid, queryid, dbid, userid
;
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
    OUT mean_err_estimate_ratio double precision,
    OUT mean_err_estimate_num double precision,
    OUT queryid bigint,
    OUT constvalues varchar[],
    OUT quals qual_type[]
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
  is_v2 bool;
  ratio_col text := 'qs.mean_err_estimate_ratio';
  num_col text := 'qs.mean_err_estimate_num';
  sql text;
BEGIN
    IF (_srvid = 0) THEN
        SELECT substr(extversion, 1, 1)::int >=2 INTO is_v2
          FROM pg_extension
          WHERE extname = 'pg_qualstats';

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
                    %s AS mean_err_estimate_ratio,
                    %s AS mean_err_estimate_num,
                    qs.eval_type,
                    qs.constant_position
                    FROM pg_qualstats() qs
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
                    FROM pg_stat_statements s1
                UNION
                SELECT s2.queryid, s2.dbid, s2.userid
                    FROM powa_statements s2 WHERE s2.srvid = 0
            ) s USING(queryid, dbid, userid)
        -- we don't gather quals for databases that have been dropped
        JOIN pg_database d ON d.oid = s.dbid
        JOIN pg_roles r ON s.userid = r.oid
          AND NOT (r.rolname = ANY (string_to_array(
                    powa_get_guc('powa.ignored_users', ''),
                    ',')))
        WHERE pgqs.dbid NOT IN (SELECT oid FROM powa_databases WHERE dropped IS NOT NULL)
        $sql$, ratio_col, num_col);
        RETURN QUERY EXECUTE sql;
    ELSE
        RETURN QUERY
            SELECT pgqs.ts, pgqs.uniquequalnodeid, pgqs.dbid, pgqs.userid,
                pgqs.qualnodeid, pgqs.occurences, pgqs.execution_count,
                pgqs.nbfiltered, pgqs.mean_err_estimate_ratio,
                pgqs.mean_err_estimate_num, pgqs.queryid, pgqs.constvalues,
                pgqs.quals
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
    FROM powa_qualstats_src(_srvid) q
    WHERE EXISTS (SELECT 1
      FROM powa_statements s
      WHERE s.srvid = _srvid
      AND q.queryid = s.queryid
      AND q.dbid = s.dbid
      AND q.userid = s.userid)
  ),
  missing_quals AS (
      INSERT INTO public.powa_qualstats_quals (srvid, qualid, queryid, dbid, userid, quals)
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
      INSERT INTO public.powa_qualstats_quals_history_current (srvid, qualid, queryid,
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
      INSERT INTO public.powa_qualstats_constvalues_history_current(srvid, qualid,
        queryid, dbid, userid, ts, occurences, execution_count, nbfiltered,
        mean_err_estimate_ratio, mean_err_estimate_num, constvalues)
      SELECT _srvid, qualnodeid, qs.queryid, qs.dbid, qs.userid, ts,
        occurences, execution_count, nbfiltered, mean_err_estimate_ratio,
        mean_err_estimate_num, constvalues
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

  -- pg_qualstats metrics are not accumulated, so we force a reset after every
  -- snapshot.  For local snapshot this is done here, remote snapshots will
  -- rely on the collector doing it through query_cleanup.
  IF (_srvid = 0) THEN
    PERFORM pg_qualstats_reset();
  END IF;
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

  INSERT INTO public.powa_qualstats_constvalues_history (
      srvid, qualid, queryid, dbid, userid, coalesce_range, most_used,
      most_filtering, least_filtering, most_executed, most_errestim_ratio,
    most_errestim_num)
    SELECT * FROM powa_qualstats_aggregate_constvalues_current(_srvid)
    WHERE srvid = _srvid;

  INSERT INTO public.powa_qualstats_quals_history (srvid, qualid, queryid, dbid,
      userid, coalesce_range, records, mins_in_range, maxs_in_range)
    SELECT srvid, qualid, queryid, dbid, userid, tstzrange(min(ts),
      max(ts),'[]'),
      array_agg((ts, occurences, execution_count, nbfiltered,
            mean_err_estimate_ratio,
            mean_err_estimate_num)::powa_qualstats_history_item),
    ROW(min(ts), min(occurences), min(execution_count), min(nbfiltered),
        min(mean_err_estimate_ratio), min(mean_err_estimate_num)
    )::powa_qualstats_history_item,
    ROW(max(ts), max(occurences), max(execution_count), max(nbfiltered),
        max(mean_err_estimate_ratio), max(mean_err_estimate_num)
    )::powa_qualstats_history_item
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
  PERFORM public.powa_log('running powa_qualstats_reset(' || _srvid || ')');

  PERFORM public.powa_log('resetting powa_qualstats_quals(' || _srvid || ')');
  DELETE FROM public.powa_qualstats_quals WHERE srvid = _srvid;
  -- cascaded :
  -- powa_qualstats_quals_history
  -- powa_qualstats_quals_history_current
  -- powa_qualstats_constvalues_history
  -- powa_qualstats_constvalues_history_current

  PERFORM public.powa_log('resetting powa_qualstats_src_tmp(' || _srvid || ')');
  DELETE FROM public.powa_qualstats_src_tmp WHERE srvid = _srvid;
END;
$PROC$ language plpgsql; /* end of powa_qualstats_reset */

/*
 * powa_qualstats_unregister
 */
CREATE OR REPLACE function public.powa_qualstats_unregister(_srvid integer = 0) RETURNS bool AS
$_$
BEGIN
    PERFORM powa_log('unregistering pg_qualstats');
    DELETE FROM public.powa_functions
    WHERE module = 'pg_qualstats'
    AND srvid = _srvid;
    RETURN true;
END;
$_$
language plpgsql; /* end of powa_qualstats_unregister */

-- By default, try to register pg_qualstats, in case it's alreay here
SELECT * FROM public.powa_activate_extension(0, 'pg_qualstats');

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

            -- This extension handles its own storage, just add its snapshot,
            -- reset and an unregister function.
            INSERT INTO public.powa_functions (srvid, extname, module, operation, function_name, query_source, added_manually, enabled)
            VALUES (_srvid, 'pg_track_settings', 'pg_track_settings', 'snapshot',   'pg_track_settings_snapshot_settings', 'pg_track_settings_settings_src', true, true),
                   (_srvid, 'pg_track_settings', 'pg_track_settings', 'snapshot',   'pg_track_settings_snapshot_rds',      'pg_track_settings_rds_src',      true, true),
                   (_srvid, 'pg_track_settings', 'pg_track_settings', 'snapshot',   'pg_track_settings_snapshot_reboot',   'pg_track_settings_reboot_src',   true, true),
                   (_srvid, 'pg_track_settings', 'pg_track_settings', 'reset',      'pg_track_settings_reset',             NULL,                             true, true),
                   (_srvid, 'pg_track_settings', 'pg_track_settings', 'unregister', 'powa_track_settings_unregister',      NULL,                             true, true);
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
SELECT * FROM public.powa_activate_extension(0, 'pg_track_settings');

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

            INSERT INTO public.powa_functions (srvid, extname, module, operation, function_name, query_source, added_manually, enabled)
            VALUES (_srvid, 'pg_wait_sampling', 'pg_wait_sampling', 'snapshot',   'powa_wait_sampling_snapshot',   'powa_wait_sampling_src', true, true),
                   (_srvid, 'pg_wait_sampling', 'pg_wait_sampling', 'aggregate',  'powa_wait_sampling_aggregate',  NULL,                     true, true),
                   (_srvid, 'pg_wait_sampling', 'pg_wait_sampling', 'unregister', 'powa_wait_sampling_unregister', NULL,                     true, true),
                   (_srvid, 'pg_wait_sampling', 'pg_wait_sampling', 'purge',      'powa_wait_sampling_purge',      NULL,                     true, true),
                   (_srvid, 'pg_wait_sampling', 'pg_wait_sampling', 'reset',      'powa_wait_sampling_reset',      NULL,                     true, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql; /* end of powa_wait_sampling_register */

/*
 * unregister pg_wait_sampling extension
 */
CREATE OR REPLACE FUNCTION public.powa_wait_sampling_unregister(_srvid integer = 0)
RETURNS bool AS $_$
BEGIN
    PERFORM powa_log('unregistering pg_wait_sampling');
    DELETE FROM public.powa_functions
    WHERE module = 'pg_wait_sampling'
    AND srvid = _srvid;
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
) RETURNS SETOF RECORD STABLE AS $PROC$
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
            -- Note that the same queryid can exists for multiple entries if
            -- multiple users execute the query, so it's critical to retrieve a
            -- single row from pg_stat_statements per (dbid, queryid)
            LEFT JOIN (SELECT DISTINCT s2.dbid, s2.queryid
                FROM pg_stat_statements(false) s2
            ) pgss ON pgss.queryid = s.queryid
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
        INSERT INTO public.powa_wait_sampling_history_current (srvid, queryid, dbid,
                event_type, event, record)
            SELECT _srvid, queryid, dbid, event_type, event, (ts, count)::wait_sampling_type
            FROM capture
    ),

    by_database AS (
        INSERT INTO public.powa_wait_sampling_history_current_db (srvid, dbid,
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
    INSERT INTO public.powa_wait_sampling_history (coalesce_range, srvid, queryid,
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
    INSERT INTO public.powa_wait_sampling_history_db (coalesce_range, srvid, dbid,
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
    PERFORM public.powa_log(format('running %I', v_funcname));

    PERFORM public.powa_log('resetting powa_wait_sampling_history(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_history WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_wait_sampling_history_db(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_history_db WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_wait_sampling_history_current(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_wait_sampling_history_current_db(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_history_current_db WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_wait_sampling_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_src_tmp WHERE srvid = _srvid;
END;
$PROC$ language plpgsql; /* end of powa_wait_sampling_reset */

-- By default, try to register pg_wait_sampling, in case it's alreay here
SELECT * FROM public.powa_activate_extension(0, 'pg_wait_sampling');

/* end of pg_wait_sampling integration - part 2 */
