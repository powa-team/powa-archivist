-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "CREATE EXTENSION powa" to load this file. \quit

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;
SET search_path = public, pg_catalog;

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

CREATE TYPE powa_user_functions_history_record AS (
    ts timestamp with time zone,
    calls bigint,
    total_time double precision,
    self_time double precision
);

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

CREATE TABLE powa_last_aggregation (
    aggts timestamp with time zone
);

INSERT INTO powa_last_aggregation(aggts) VALUES (current_timestamp);

CREATE TABLE powa_last_purge (
    purgets timestamp with time zone
);

INSERT INTO powa_last_purge (purgets) VALUES (current_timestamp);

CREATE TABLE powa_statements (
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    query text NOT NULL
);

ALTER TABLE ONLY powa_statements
    ADD CONSTRAINT powa_statements_pkey PRIMARY KEY (queryid, dbid, userid);

CREATE INDEX powa_statements_dbid_idx ON powa_statements(dbid);
CREATE INDEX powa_statements_userid_idx ON powa_statements(userid);


CREATE TABLE powa_statements_history (
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_statements_history_record[] NOT NULL,
    mins_in_range powa_statements_history_record NOT NULL,
    maxs_in_range powa_statements_history_record NOT NULL
);

CREATE INDEX powa_statements_history_query_ts ON powa_statements_history USING gist (queryid, coalesce_range);

CREATE TABLE powa_statements_history_db (
    dbid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_statements_history_record[] NOT NULL,
    mins_in_range powa_statements_history_record NOT NULL,
    maxs_in_range powa_statements_history_record NOT NULL
);

CREATE INDEX powa_statements_history_db_ts ON powa_statements_history_db USING gist (dbid, coalesce_range);

CREATE TABLE powa_statements_history_current (
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    record powa_statements_history_record NOT NULL
);

CREATE TABLE powa_statements_history_current_db (
    dbid oid NOT NULL,
    record powa_statements_history_record NOT NULL
);

CREATE TABLE powa_user_functions_history (
    dbid oid NOT NULL,
    funcid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_user_functions_history_record[] NOT NULL,
    mins_in_range powa_user_functions_history_record NOT NULL,
    maxs_in_range powa_user_functions_history_record NOT NULL
);

CREATE INDEX powa_user_functions_history_funcid_ts ON powa_user_functions_history USING gist (funcid, coalesce_range);

CREATE TABLE powa_user_functions_history_current (
    dbid oid NOT NULL,
    funcid oid NOT NULL,
    record powa_user_functions_history_record NOT NULL
);

CREATE TABLE powa_all_relations_history (
    dbid oid NOT NULL,
    relid oid NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records powa_all_relations_history_record[] NOT NULL,
    mins_in_range powa_all_relations_history_record NOT NULL,
    maxs_in_range powa_all_relations_history_record NOT NULL
);

CREATE INDEX powa_all_relations_history_relid_ts ON powa_all_relations_history USING gist (relid, coalesce_range);

CREATE TABLE powa_all_relations_history_current (
    dbid oid NOT NULL,
    relid oid NOT NULL,
    record powa_all_relations_history_record NOT NULL
);

CREATE SEQUENCE powa_coalesce_sequence INCREMENT BY 1
  START WITH 1
  CYCLE;


CREATE TABLE powa_functions (
    module text NOT NULL,
    operation text NOT NULL,
    function_name text NOT NULL,
    added_manually boolean NOT NULL default true,
    enabled boolean NOT NULL default true,
    CHECK (operation IN ('snapshot','aggregate','purge','unregister','reset'))
);

INSERT INTO powa_functions (module, operation, function_name, added_manually, enabled) VALUES
    ('pg_stat_statements', 'snapshot', 'powa_statements_snapshot', false, true),
    ('powa_stat_user_functions', 'snapshot', 'powa_user_functions_snapshot', false, true),
    ('powa_stat_all_relations', 'snapshot', 'powa_all_relations_snapshot', false, true),
    ('pg_stat_statements', 'aggregate','powa_statements_aggregate', false, true),
    ('powa_stat_user_functions', 'aggregate','powa_user_functions_aggregate', false, true),
    ('powa_stat_all_relations', 'aggregate','powa_all_relations_aggregate', false, true),
    ('pg_stat_statements', 'purge', 'powa_statements_purge', false, true),
    ('powa_stat_user_functions', 'purge', 'powa_user_functions_purge', false, true),
    ('powa_stat_all_relations', 'purge', 'powa_all_relations_purge', false, true),
    ('pg_stat_statements', 'reset', 'powa_statements_reset', false, true),
    ('powa_stat_user_functions', 'reset', 'powa_user_functions_reset', false, true),
    ('powa_stat_all_relations', 'reset', 'powa_all_relations_reset', false, true);

/* pg_stat_kcache integration - part 1 */

CREATE TYPE public.kcache_type AS (
    ts timestamptz,
    reads bigint,
    writes bigint,
    user_time double precision,
    system_time double precision
);

CREATE TABLE public.powa_kcache_metrics (
    coalesce_range tstzrange NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    metrics public.kcache_type[] NOT NULL,
    mins_in_range public.kcache_type NOT NULL,
    maxs_in_range public.kcache_type NOT NULL,
    PRIMARY KEY (coalesce_range, queryid, dbid, userid)
);

CREATE INDEX ON public.powa_kcache_metrics (queryid);

CREATE TABLE public.powa_kcache_metrics_db (
    coalesce_range tstzrange NOT NULL,
    dbid oid NOT NULL,
    metrics public.kcache_type[] NOT NULL,
    mins_in_range public.kcache_type NOT NULL,
    maxs_in_range public.kcache_type NOT NULL,
    PRIMARY KEY (coalesce_range, dbid)
);

CREATE TABLE public.powa_kcache_metrics_current (
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    userid oid NOT NULL,
    metrics kcache_type NULL NULL
);

CREATE TABLE public.powa_kcache_metrics_current_db (
    dbid oid NOT NULL,
    metrics kcache_type NULL NULL
);

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

CREATE TABLE public.powa_qualstats_quals (
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    quals public.qual_type[],
    PRIMARY KEY (qualid, queryid, dbid, userid),
    FOREIGN KEY (queryid, dbid, userid) REFERENCES powa_statements(queryid, dbid, userid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE public.powa_qualstats_quals_history (
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    coalesce_range tstzrange,
    records powa_qualstats_history_item[],
    mins_in_range powa_qualstats_history_item,
    maxs_in_range powa_qualstats_history_item,
    FOREIGN KEY (qualid, queryid, dbid, userid) REFERENCES public.powa_qualstats_quals (qualid, queryid, dbid, userid) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE public.powa_qualstats_quals_history_current (
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    ts timestamptz,
    occurences bigint,
    execution_count   bigint,
    nbfiltered bigint,
    FOREIGN KEY (qualid, queryid, dbid, userid) REFERENCES powa_qualstats_quals(qualid, queryid, dbid, userid)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE public.powa_qualstats_constvalues_history (
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    coalesce_range tstzrange,
    most_used qual_values[],
    most_filtering qual_values[],
    least_filtering qual_values[],
    most_executed qual_values[],
    FOREIGN KEY (qualid, queryid, dbid, userid) REFERENCES public.powa_qualstats_quals (qualid, queryid, dbid, userid) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE public.powa_qualstats_constvalues_history_current (
    qualid bigint,
    queryid bigint,
    dbid oid,
    userid oid,
    ts timestamptz,
    constvalues text[],
    occurences bigint,
    execution_count bigint,
    nbfiltered bigint,
    FOREIGN KEY (qualid, queryid, dbid, userid) REFERENCES public.powa_qualstats_quals (qualid, queryid, dbid, userid) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX ON powa_qualstats_constvalues_history USING gist (queryid, qualid, coalesce_range);
CREATE INDEX ON powa_qualstats_constvalues_history (qualid, queryid);
CREATE INDEX ON powa_qualstats_quals(queryid);


/* end of pg_qualstats_integration - part 1 */

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
    PERFORM public.powa_kcache_register();
    PERFORM public.powa_qualstats_register();
    PERFORM public.powa_track_settings_register();
END;
$_$;

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
    -- We unregister extensions regardless the "enabled" field
    WITH ext AS (
        SELECT object_name
        FROM pg_event_trigger_dropped_objects() d
        WHERE d.object_type = 'extension'
    )
    SELECT function_name INTO funcname
    FROM powa_functions f
    JOIN ext ON f.module = ext.object_name
    WHERE operation = 'unregister';

    IF ( funcname IS NOT NULL ) THEN
        BEGIN
            RAISE DEBUG 'running %', funcname;
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
$_$;

CREATE EVENT TRIGGER powa_check_dropped_extensions
    ON sql_drop
    WHEN tag IN ('DROP EXTENSION')
    EXECUTE PROCEDURE public.powa_check_dropped_extensions() ;

CREATE OR REPLACE FUNCTION powa_take_snapshot() RETURNS void AS $PROC$
DECLARE
  purgets timestamp with time zone;
  purge_seq bigint;
  funcname text;
  v_state   text;
  v_msg     text;
  v_detail  text;
  v_hint    text;
  v_context text;

BEGIN
    -- For all enabled snapshot functions in the powa_functions table, execute
    FOR funcname IN SELECT function_name
                 FROM powa_functions
                 WHERE operation='snapshot' AND enabled LOOP
      -- Call all of them, with no parameter
      RAISE debug 'fonction: %',funcname;
      BEGIN
        EXECUTE 'SELECT ' || quote_ident(funcname)||'()';
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS
              v_state   = RETURNED_SQLSTATE,
              v_msg     = MESSAGE_TEXT,
              v_detail  = PG_EXCEPTION_DETAIL,
              v_hint    = PG_EXCEPTION_HINT,
              v_context = PG_EXCEPTION_CONTEXT;
          RAISE warning 'powa_take_snapshot(): function "%" failed:
              state  : %
              message: %
              detail : %
              hint   : %
              context: %', funcname, v_state, v_msg, v_detail, v_hint, v_context;

      END;
    END LOOP;

    -- Coalesce datas if needed
    SELECT nextval('powa_coalesce_sequence'::regclass) INTO purge_seq;

    IF (  purge_seq
            % current_setting('powa.coalesce')::bigint ) = 0
    THEN
      FOR funcname IN SELECT function_name
                   FROM powa_functions
                   WHERE operation='aggregate' AND enabled LOOP
        -- Call all of them, with no parameter
        BEGIN
          EXECUTE 'SELECT ' || quote_ident(funcname)||'()';
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;
            RAISE warning 'powa_take_snapshot(): function "%" failed:
                state  : %
                message: %
                detail : %
                hint   : %
                context: %', funcname, v_state, v_msg, v_detail, v_hint, v_context;

        END;
      END LOOP;
      UPDATE powa_last_aggregation SET aggts = now();
    END IF;
    -- Once every 10 packs, we also purge
    IF (  purge_seq
            % (current_setting('powa.coalesce')::bigint *10) ) = 0
    THEN
      FOR funcname IN SELECT function_name
                   FROM powa_functions
                   WHERE operation='purge' AND enabled LOOP
        -- Call all of them, with no parameter
        BEGIN
          EXECUTE 'SELECT ' || quote_ident(funcname)||'()';
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;
            RAISE warning 'powa_take_snapshot(): function "%" failed:
                state  : %
                message: %
                detail : %
                hint   : %
                context: %', funcname, v_state, v_msg, v_detail, v_hint, v_context;

        END;
      END LOOP;
      UPDATE powa_last_purge SET purgets = now();
    END IF;
END;
$PROC$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION powa_statements_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    ignore_regexp text:='^[[:space:]]*(BEGIN)'; -- Ignore begin at beginning of statement
BEGIN
    -- In this function, we capture statements, and also aggregate counters by database
    -- so that the first screens of powa stay reactive even though there may be thousands
    -- of different statements
    RAISE DEBUG 'running powa_statements_snapshot';
    WITH capture AS(
        SELECT pgss.*
        FROM pg_stat_statements pgss
        JOIN pg_roles r ON pgss.userid = r.oid
        WHERE pgss.query !~* ignore_regexp
        AND NOT (r.rolname = ANY (string_to_array(current_setting('powa.ignored_users'),',')))
    ),

    missing_statements AS(
        INSERT INTO powa_statements (queryid, dbid, userid, query)
            SELECT queryid, dbid, userid, query
            FROM capture c
            WHERE NOT EXISTS (SELECT 1
                              FROM powa_statements ps
                              WHERE ps.queryid = c.queryid
                              AND ps.dbid = c.dbid
                              AND ps.userid = c.userid
            )
    ),

    by_query AS (
        INSERT INTO powa_statements_history_current
            SELECT queryid, dbid, userid,
            ROW(
                now(), calls, total_time, rows, shared_blks_hit, shared_blks_read,
                shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read,
                local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
                blk_read_time, blk_write_time
            )::powa_statements_history_record AS record
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_statements_history_current_db
            SELECT dbid,
            ROW(
                now(), sum(calls), sum(total_time), sum(rows), sum(shared_blks_hit), sum(shared_blks_read),
                sum(shared_blks_dirtied), sum(shared_blks_written), sum(local_blks_hit), sum(local_blks_read),
                sum(local_blks_dirtied), sum(local_blks_written), sum(temp_blks_read), sum(temp_blks_written),
                sum(blk_read_time), sum(blk_write_time)
            )::powa_statements_history_record AS record
            FROM capture
            GROUP BY dbid
    )

    SELECT true::boolean INTO result; -- For now we don't care. What could we do on error except crash anyway?
END;
$PROC$ language plpgsql;

CREATE OR REPLACE FUNCTION powa_user_functions_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
BEGIN
    RAISE DEBUG 'running powa_user_functions_snapshot';
    -- Insert cluster-wide user function statistics
    WITH func(dbid,funcid, r) AS (
        SELECT oid,
            (powa_stat_user_functions(oid)).funcid,
            powa_stat_user_functions(oid)
        FROM pg_database
    )
    INSERT INTO powa_user_functions_history_current
        SELECT dbid, funcid,
        ROW(now(), (r).calls,
            (r).total_time,
            (r).self_time)::powa_user_functions_history_record AS record
        FROM func;

    result := true;
END;
$PROC$ language plpgsql;

CREATE OR REPLACE FUNCTION powa_all_relations_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
BEGIN
    RAISE DEBUG 'running powa_all_relations_snapshot';
    -- Insert cluster-wide relation statistics
    WITH rel(dbid, relid, r) AS (
        SELECT oid,
            (powa_stat_all_rel(oid)).relid,
            powa_stat_all_rel(oid)
        FROM pg_database
    )
    INSERT INTO powa_all_relations_history_current
        SELECT dbid, relid,
        ROW(now(),(r).numscan, (r).tup_returned, (r).tup_fetched,
            (r).n_tup_ins, (r).n_tup_upd, (r).n_tup_del, (r).n_tup_hot_upd,
            (r).n_liv_tup, (r).n_dead_tup, (r).n_mod_since_analyze,
            (r).blks_read, (r).blks_hit, (r).last_vacuum, (r).vacuum_count,
        (r).last_autovacuum, (r).autovacuum_count, (r).last_analyze,
            (r).analyze_count, (r).last_autoanalyze,
            (r).autoanalyze_count)::powa_all_relations_history_record AS record
        FROM rel;

    result := true;
END;
$PROC$ language plpgsql;

CREATE OR REPLACE FUNCTION powa_statements_purge() RETURNS void AS $PROC$
BEGIN
    RAISE DEBUG 'running powa_statements_purge';
    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_statements_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);
    DELETE FROM powa_statements_history_db WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);
    -- FIXME maybe we should cleanup the powa_*_history tables ? But it will take a while: unnest all records...
END;
$PROC$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION powa_user_functions_purge() RETURNS void AS $PROC$
BEGIN
    RAISE DEBUG 'running powa_user_functions_purge';
    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_user_functions_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);
    -- FIXME maybe we should cleanup the powa_*_history tables ? But it will take a while: unnest all records...
END;
$PROC$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION powa_all_relations_purge() RETURNS void AS $PROC$
BEGIN
    RAISE DEBUG 'running powa_all_relations_purge';
    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_all_relations_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);
    -- FIXME maybe we should cleanup the powa_*_history tables ? But it will take a while: unnest all records...
END;
$PROC$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION powa_statements_aggregate() RETURNS void AS $PROC$
BEGIN
    RAISE DEBUG 'running powa_statements_aggregate';

    -- aggregate statements table
    LOCK TABLE powa_statements_history_current IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_statements_history
        SELECT queryid, dbid, userid,
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
        GROUP BY queryid, dbid, userid;

    TRUNCATE powa_statements_history_current;

    -- aggregate db table
    LOCK TABLE powa_statements_history_current_db IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_statements_history_db
        SELECT dbid,
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
        GROUP BY dbid;

    TRUNCATE powa_statements_history_current_db;
 END;
$PROC$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION powa_user_functions_aggregate() RETURNS void AS $PROC$
BEGIN
    RAISE DEBUG 'running powa_user_functions_aggregate';

    -- aggregate user_functions table
    LOCK TABLE powa_user_functions_history_current IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_user_functions_history
        SELECT dbid, funcid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts), min((record).calls),min((record).total_time),
                min((record).self_time))::powa_user_functions_history_record,
            ROW(max((record).ts), max((record).calls),max((record).total_time),
                max((record).self_time))::powa_user_functions_history_record
        FROM powa_user_functions_history_current
        GROUP BY dbid, funcid;

    TRUNCATE powa_user_functions_history_current;
 END;
$PROC$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION powa_all_relations_aggregate() RETURNS void AS $PROC$
BEGIN
    RAISE DEBUG 'running powa_all_relations_aggregate';

    -- aggregate all_relations table
    LOCK TABLE powa_all_relations_history_current IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_all_relations_history
        SELECT dbid, relid,
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
        GROUP BY dbid, relid;

    TRUNCATE powa_all_relations_history_current;
 END;
$PROC$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.powa_reset()
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
                 WHERE operation='reset' LOOP
      -- Call all of them, with no parameter
      BEGIN
        EXECUTE 'SELECT ' || quote_ident(funcname)||'()';
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS
              v_state   = RETURNED_SQLSTATE,
              v_msg     = MESSAGE_TEXT,
              v_detail  = PG_EXCEPTION_DETAIL,
              v_hint    = PG_EXCEPTION_HINT,
              v_context = PG_EXCEPTION_CONTEXT;
          RAISE warning 'powa_reset(): function "%" failed:
              state  : %
              message: %
              detail : %
              hint   : %
              context: %', funcname, v_state, v_msg, v_detail, v_hint, v_context;

      END;
    END LOOP;
    RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION public.powa_statements_reset()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    TRUNCATE TABLE powa_statements_history;
    TRUNCATE TABLE powa_statements_history_current;
    TRUNCATE TABLE powa_statements_history_db;
    TRUNCATE TABLE powa_statements_history_current_db;
    -- if 3rd part datasource has FK on it, throw everything away
    TRUNCATE TABLE powa_statements CASCADE;
    RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION public.powa_user_functions_reset()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    TRUNCATE TABLE powa_user_functions_history;
    TRUNCATE TABLE powa_user_functions_history_current;
    RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION public.powa_all_relations_reset()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    TRUNCATE TABLE powa_all_relations_history;
    TRUNCATE TABLE powa_all_relations_history_current;
    RETURN true;
END;
$function$;

/* pg_stat_kcache integration - part 2 */

/*
 * register pg_stat_kcache extension
 */
CREATE OR REPLACE function public.powa_kcache_register() RETURNS bool AS
$_$
DECLARE
    v_func_present bool;
    v_ext_present bool;
BEGIN
    SELECT COUNT(*) = 1 INTO v_ext_present FROM pg_extension WHERE extname = 'pg_stat_kcache';

    IF ( v_ext_present ) THEN
        SELECT COUNT(*) > 0 INTO v_func_present FROM public.powa_functions WHERE module = 'pg_stat_kcache';
        IF ( NOT v_func_present) THEN
            INSERT INTO powa_functions (module, operation, function_name, added_manually, enabled)
            VALUES ('pg_stat_kcache', 'snapshot',   'powa_kcache_snapshot',   false, true),
                   ('pg_stat_kcache', 'aggregate',  'powa_kcache_aggregate',  false, true),
                   ('pg_stat_kcache', 'unregister', 'powa_kcache_unregister', false, true),
                   ('pg_stat_kcache', 'purge',      'powa_kcache_purge',      false, true),
                   ('pg_stat_kcache', 'reset',      'powa_kcache_reset',      false, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql;

/*
 * unregister pg_stat_kcache extension
 */
CREATE OR REPLACE function public.powa_kcache_unregister() RETURNS bool AS
$_$
BEGIN
    DELETE FROM public.powa_functions WHERE module = 'pg_stat_kcache';
    RETURN true;
END;
$_$
language plpgsql;

/*
 * powa_kcache snapshot collection.
 */
CREATE OR REPLACE FUNCTION powa_kcache_snapshot() RETURNS void as $PROC$
DECLARE
  result bool;
BEGIN
    RAISE DEBUG 'running powa_kcache_snapshot';

    WITH capture AS (
        SELECT *
        FROM pg_stat_kcache() k
        JOIN pg_roles r ON r.oid = k.userid
        WHERE NOT (r.rolname = ANY (string_to_array(current_setting('powa.ignored_users'),',')))
    ),

    by_query AS (
        INSERT INTO powa_kcache_metrics_current (queryid, dbid, userid, metrics)
            SELECT queryid, dbid, userid, (now(), reads, writes, user_time, system_time)::kcache_type
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_kcache_metrics_current_db (dbid, metrics)
            SELECT dbid, (now(), sum(reads), sum(writes), sum(user_time), sum(system_time))::kcache_type
            FROM capture
            GROUP BY dbid
    )

    SELECT true into result;
END
$PROC$ language plpgsql;

/*
 * powa_kcache aggregation
 */
CREATE OR REPLACE FUNCTION powa_kcache_aggregate() RETURNS void AS $PROC$
DECLARE
  result bool;
BEGIN
    RAISE DEBUG 'running powa_kcache_aggregate';

    -- aggregate metrics table
    LOCK TABLE powa_kcache_metrics_current IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_kcache_metrics (coalesce_range, queryid, dbid, userid, metrics, mins_in_range, maxs_in_range)
        SELECT tstzrange(min((metrics).ts), max((metrics).ts),'[]'),
        queryid, dbid, userid, array_agg(metrics),
        ROW(min((metrics).ts),
            min((metrics).reads),min((metrics).writes),min((metrics).user_time),
            min((metrics).system_time))::kcache_type,
        ROW(max((metrics).ts),
            max((metrics).reads),max((metrics).writes),max((metrics).user_time),
            max((metrics).system_time))::kcache_type
        FROM powa_kcache_metrics_current
        GROUP BY queryid, dbid, userid;

    TRUNCATE powa_kcache_metrics_current;

    -- aggregate metrics_db table
    LOCK TABLE powa_kcache_metrics_current_db IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_kcache_metrics_db (coalesce_range, dbid, metrics, mins_in_range, maxs_in_range)
        SELECT tstzrange(min((metrics).ts), max((metrics).ts),'[]'),
        dbid, array_agg(metrics),
        ROW(min((metrics).ts),
            min((metrics).reads),min((metrics).writes),min((metrics).user_time),
            min((metrics).system_time))::kcache_type,
        ROW(max((metrics).ts),
            max((metrics).reads),max((metrics).writes),max((metrics).user_time),
            max((metrics).system_time))::kcache_type
        FROM powa_kcache_metrics_current_db
        GROUP BY dbid;

    TRUNCATE powa_kcache_metrics_current_db;
END
$PROC$ language plpgsql;

/*
 * powa_kcache purge
 */
CREATE OR REPLACE FUNCTION powa_kcache_purge() RETURNS void as $PROC$
BEGIN
    RAISE DEBUG 'running powa_kcache_purge';

    DELETE FROM powa_kcache_metrics WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
    DELETE FROM powa_kcache_metrics_db WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
END;
$PROC$ language plpgsql;

/*
 * powa_kcache reset
 */
CREATE OR REPLACE FUNCTION powa_kcache_reset() RETURNS void as $PROC$
BEGIN
    RAISE DEBUG 'running powa_kcache_reset';

    TRUNCATE TABLE powa_kcache_metrics;
    TRUNCATE TABLE powa_kcache_metrics_db;
    TRUNCATE TABLE powa_kcache_metrics_current;
    TRUNCATE TABLE powa_kcache_metrics_current_db;
END;
$PROC$ language plpgsql;

-- By default, try to register pg_stat_kcache, in case it's alreay here
SELECT * FROM public.powa_kcache_register();

/* end of pg_stat_kcache integration - part 2 */

/* pg_qualstats integration - part 2 */

/*
 * powa_qualstats_register
 */
CREATE OR REPLACE function public.powa_qualstats_register() RETURNS bool AS
$_$
DECLARE
    v_func_present bool;
    v_ext_present bool;
BEGIN
    SELECT COUNT(*) = 1 INTO v_ext_present FROM pg_extension WHERE extname = 'pg_qualstats';

    IF ( v_ext_present) THEN
        SELECT COUNT(*) > 0 INTO v_func_present FROM public.powa_functions WHERE function_name IN ('powa_qualstats_snapshot', 'powa_qualstats_aggregate', 'powa_qualstats_purge');
        IF ( NOT v_func_present) THEN
            INSERT INTO powa_functions (module, operation, function_name, added_manually, enabled)
            VALUES ('pg_qualstats', 'snapshot',   'powa_qualstats_snapshot',   false, true),
                   ('pg_qualstats', 'aggregate',  'powa_qualstats_aggregate',  false, true),
                   ('pg_qualstats', 'unregister', 'powa_qualstats_unregister', false, true),
                   ('pg_qualstats', 'purge',      'powa_qualstats_purge',      false, true),
                   ('pg_qualstats', 'reset',      'powa_qualstats_reset',      false, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql;

/*
 * powa_qualstats utility view for aggregating constvalues
 */
CREATE OR REPLACE VIEW powa_qualstats_aggregate_constvalues_current AS
WITH consts AS (
  SELECT qualid, queryid, dbid, userid, min(ts) as mints, max(ts) as maxts,
  sum(occurences) as occurences,
  sum(nbfiltered) as nbfiltered,
  sum(execution_count) as execution_count, constvalues
  FROM powa_qualstats_constvalues_history_current
  GROUP BY qualid, queryid, dbid, userid, constvalues
),
groups AS (
  SELECT qualid, queryid, dbid, userid, tstzrange(min(mints), max(maxts),'[]')
  FROM consts
  GROUP BY qualid, queryid, dbid, userid
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


CREATE OR REPLACE FUNCTION powa_qualstats_snapshot() RETURNS void as $PROC$
DECLARE
  result bool;
BEGIN
  RAISE DEBUG 'running powa_qualstats_snaphot';
  WITH capture AS (
    SELECT pgqs.*, s.query
    FROM pg_qualstats_by_query pgqs
    JOIN powa_statements s USING(queryid, dbid, userid)
    JOIN pg_roles r ON s.userid = r.oid
    AND NOT (r.rolname = ANY (string_to_array(current_setting('powa.ignored_users'),',')))
  ),
  missing_quals AS (
      INSERT INTO powa_qualstats_quals (qualid, queryid, dbid, userid, quals)
        SELECT DISTINCT qs.qualnodeid, qs.queryid, qs.dbid, qs.userid, array_agg(DISTINCT q::qual_type)
        FROM capture qs,
        LATERAL (SELECT (unnest(quals)).*) as q
        WHERE NOT EXISTS (
          SELECT 1
          FROM powa_qualstats_quals nh
          WHERE nh.qualid = qs.qualnodeid AND nh.queryid = qs.queryid
            AND nh.dbid = qs.dbid AND nh.userid = qs.userid
        )
        GROUP BY qualnodeid, queryid, dbid, userid
      RETURNING *
  ),
  by_qual AS (
      INSERT INTO powa_qualstats_quals_history_current (qualid, queryid, dbid, userid, ts, occurences, execution_count, nbfiltered)
      SELECT qs.qualnodeid, qs.queryid, qs.dbid, qs.userid, now(), sum(occurences), sum(execution_count), sum(nbfiltered)
        FROM capture as qs
        GROUP BY qualnodeid, qs.queryid, qs.dbid, qs.userid
      RETURNING *
  ),
  by_qual_with_const AS (
      INSERT INTO powa_qualstats_constvalues_history_current(qualid, queryid, dbid, userid, ts, occurences, execution_count, nbfiltered, constvalues)
      SELECT qualnodeid, qs.queryid, qs.dbid, qs.userid, now(), occurences, execution_count, nbfiltered, constvalues
      FROM capture as qs
  )
  SELECT true into result;
  PERFORM pg_qualstats_reset();
END
$PROC$ language plpgsql;

/*
 * powa_qualstats aggregate
 */
CREATE OR REPLACE FUNCTION powa_qualstats_aggregate() RETURNS void AS $PROC$
DECLARE
  result bool;
BEGIN
  RAISE DEBUG 'running powa_qualstats_aggregate';
  LOCK TABLE powa_qualstats_constvalues_history_current IN SHARE MODE;
  LOCK TABLE powa_qualstats_quals_history_current IN SHARE MODE;
  INSERT INTO powa_qualstats_constvalues_history (
    qualid, queryid, dbid, userid, coalesce_range, most_used, most_filtering, least_filtering, most_executed)
    SELECT * FROM powa_qualstats_aggregate_constvalues_current;
  INSERT INTO powa_qualstats_quals_history (qualid, queryid, dbid, userid, coalesce_range, records, mins_in_range, maxs_in_range)
    SELECT qualid, queryid, dbid, userid, tstzrange(min(ts), max(ts),'[]'), array_agg((ts, occurences, execution_count, nbfiltered)::powa_qualstats_history_item),
    ROW(min(ts), min(occurences), min(execution_count), min(nbfiltered))::powa_qualstats_history_item,
    ROW(max(ts), max(occurences), max(execution_count), max(nbfiltered))::powa_qualstats_history_item
    FROM powa_qualstats_quals_history_current
    GROUP BY qualid, queryid, dbid, userid;
  TRUNCATE powa_qualstats_constvalues_history_current;
  TRUNCATE powa_qualstats_quals_history_current;
END
$PROC$ language plpgsql;

/*
 * powa_qualstats_purge
 */
CREATE OR REPLACE FUNCTION powa_qualstats_purge() RETURNS void as $PROC$
BEGIN
  RAISE DEBUG 'running powa_qualstats_purge';
  DELETE FROM powa_qualstats_constvalues_history WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
  DELETE FROM powa_qualstats_quals_history WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
END;
$PROC$ language plpgsql;

/*
 * powa_qualstats_reset
 */
CREATE OR REPLACE FUNCTION powa_qualstats_reset() RETURNS void as $PROC$
BEGIN
  RAISE DEBUG 'running powa_qualstats_reset';
  TRUNCATE TABLE powa_qualstats_quals CASCADE;
  -- cascaded :
  -- powa_qualstats_quals_history
  -- powa_qualstats_quals_history_current
  -- powa_qualstats_constvalues_history
  -- powa_qualstats_constvalues_history_current
END;
$PROC$ language plpgsql;

/*
 * powa_qualstats_unregister
 */
CREATE OR REPLACE function public.powa_qualstats_unregister() RETURNS bool AS
$_$
BEGIN
    DELETE FROM public.powa_functions WHERE module = 'pg_qualstats';
    RETURN true;
END;
$_$
language plpgsql;

SELECT * FROM public.powa_qualstats_register();

/* end of pg_qualstats_integration - part 2 */

/* pg_track_settings integration */

CREATE OR REPLACE FUNCTION powa_track_settings_register() RETURNS bool AS $_$
DECLARE
    v_func_present bool;
    v_ext_present bool;
BEGIN
    SELECT COUNT(*) = 1 INTO v_ext_present FROM pg_extension WHERE extname = 'pg_track_settings';

    IF ( v_ext_present ) THEN
        SELECT COUNT(*) > 0 INTO v_func_present FROM public.powa_functions WHERE module = 'pg_track_settings';
        IF ( NOT v_func_present) THEN
            -- This extension handles its own storage, just its snapshot
            -- function and an unregister function.
            INSERT INTO powa_functions (module, operation, function_name, added_manually, enabled)
            VALUES ('pg_track_settings', 'snapshot',   'pg_track_settings_snapshot',   false, true),
                   ('pg_track_settings', 'unregister', 'powa_track_settings_unregister',   false, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$ language plpgsql;

CREATE OR REPLACE function public.powa_track_settings_unregister() RETURNS bool AS
$_$
BEGIN
    DELETE FROM public.powa_functions WHERE module = 'pg_track_settings';
    RETURN true;
END;
$_$
language plpgsql;

-- By default, try to register pg_track_settings, in case it's alreay here
SELECT * FROM public.powa_track_settings_register();
/* end pg_track_settings integration */
