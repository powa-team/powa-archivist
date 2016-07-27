-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET client_min_messages = warning;
SET escape_string_warning = off;
SET search_path = public, pg_catalog;

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


CREATE OR REPLACE FUNCTION powa_take_snapshot() RETURNS void AS $PROC$
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

BEGIN
    PERFORM set_config('application_name',
        v_title || ' snapshot database list',
        false);
    PERFORM powa_log('start of powa_take_snapshot');

    -- Keep track of existing databases
    PERFORM powa_log('Maintaining database list...');

    WITH missing AS (
        SELECT d.oid, d.datname
        FROM pg_database d
        LEFT JOIN powa_databases p ON d.oid = p.oid
        WHERE p.oid IS NULL
    )
    INSERT INTO powa_databases
    SELECT * FROM missing;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('missing db: %s', v_rowcount));

    -- Keep track of renamed databases
    WITH renamed AS (
        SELECT d.oid, d.datname
        FROM pg_database AS d
        JOIN powa_databases AS p ON d.oid = p.oid
        WHERE d.datname != p.datname
    )
    UPDATE powa_databases AS p
    SET datname = r.datname
    FROM renamed AS r
    WHERE p.oid = r.oid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('renamed db: %s', v_rowcount));

    -- Keep track of when databases are dropped
    WITH dropped AS (
        SELECT p.oid
        FROM powa_databases p
        LEFT JOIN pg_database d ON p.oid = d.oid
        WHERE d.oid IS NULL
        AND p.dropped IS NULL)
    UPDATE powa_databases p
    SET dropped = now()
    FROM dropped d
    WHERE p.oid = d.oid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('dropped db: %s', v_rowcount));

    -- For all enabled snapshot functions in the powa_functions table, execute
    FOR funcname IN SELECT function_name
                 FROM powa_functions
                 WHERE operation='snapshot' AND enabled LOOP
      -- Call all of them, with no parameter
      BEGIN
        PERFORM powa_log(format('calling snapshot function: %I', funcname));
        PERFORM set_config('application_name',
            v_title || quote_ident(funcname) || '()',
            false);

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
    PERFORM powa_log(format('powa_coalesce_sequence: %s', purge_seq));

    IF (  purge_seq
            % current_setting('powa.coalesce')::bigint ) = 0
    THEN
      PERFORM powa_log(format('coalesce needed, seq: %s coalesce seq: %s',
            purge_seq, current_setting('powa.coalesce')::bigint ));

      FOR funcname IN SELECT function_name
                   FROM powa_functions
                   WHERE operation='aggregate' AND enabled LOOP
        -- Call all of them, with no parameter
        BEGIN
          PERFORM powa_log(format('calling aggregate function: %I', funcname));

          PERFORM set_config('application_name',
              v_title || quote_ident(funcname) || '()',
              false);
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
    -- We also purge, at next pass
    IF (  purge_seq
            % (current_setting('powa.coalesce')::bigint ) ) = 1
    THEN
      PERFORM powa_log(format('purge needed, seq: %s coalesce seq: %s',
        purge_seq, current_setting('powa.coalesce')));

      FOR funcname IN SELECT function_name
                   FROM powa_functions
                   WHERE operation='purge' AND enabled LOOP
        -- Call all of them, with no parameter
        BEGIN
          PERFORM powa_log(format('calling purge function: %I', funcname));
          PERFORM set_config('application_name',
              v_title || quote_ident(funcname) || '()',
              false);

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
      PERFORM set_config('application_name',
          v_title || 'UPDATE powa_last_purge',
          false);
      UPDATE powa_last_purge SET purgets = now();
    END IF;
    PERFORM powa_log('end of powa_take_snapshot');
    PERFORM set_config('application_name',
        v_title || 'snapshot finished',
        false);
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_take_snapshot */


CREATE OR REPLACE FUNCTION powa_statements_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    ignore_regexp text :='^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)';
    v_funcname    text := 'powa_statements_snapshot';
    v_rowcount    bigint;
BEGIN
    -- In this function, we capture statements, and also aggregate counters by database
    -- so that the first screens of powa stay reactive even though there may be thousands
    -- of different statements
    PERFORM powa_log(format('running %I', v_funcname));

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

    SELECT count(*) INTO v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true; -- For now we don't care. What could we do on error except crash anyway?
END;
$PROC$ language plpgsql; /* end of powa_statements_snapshot */


CREATE OR REPLACE FUNCTION powa_user_functions_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_user_functions_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

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

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_user_functions_snapshot */


CREATE OR REPLACE FUNCTION powa_all_relations_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_all_relations_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

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

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_all_relations_snapshot */


CREATE OR REPLACE FUNCTION powa_statements_purge() RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_statements_purge';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_statements_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_hitory) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_statements_history_db WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    -- FIXME maybe we should cleanup the powa_*_history tables ? But it will take a while: unnest all records...
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_purge */


CREATE OR REPLACE FUNCTION powa_user_functions_purge() RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_user_functions_purge';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_user_functions_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    -- FIXME maybe we should cleanup the powa_*_history tables ? But it will take a while: unnest all records...
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_user_functions_purge */


CREATE OR REPLACE FUNCTION powa_all_relations_purge() RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_all_relations_purge';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_all_relations_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    -- FIXME maybe we should cleanup the powa_*_history tables ? But it will take a while: unnest all records...
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_relations_purge */


CREATE OR REPLACE FUNCTION powa_statements_aggregate() RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_statements_aggregate';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

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

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history) - rowcount: %s',
            v_funcname, v_rowcount));

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

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    TRUNCATE powa_statements_history_current_db;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_aggregate */


CREATE OR REPLACE FUNCTION powa_user_functions_aggregate() RETURNS void AS $PROC$
BEGIN
    PERFORM powa_log('running powa_user_functions_aggregate');

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
$PROC$ LANGUAGE plpgsql; /* end of powa_user_functions_aggregate */


CREATE OR REPLACE FUNCTION powa_all_relations_aggregate() RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_all_relations_aggregate';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

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

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    TRUNCATE powa_all_relations_history_current;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_relations_aggregate */


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
$function$; /* end of powa_reset */


CREATE OR REPLACE FUNCTION public.powa_statements_reset()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM powa_log('truncating powa_statements_history');
    TRUNCATE TABLE powa_statements_history;

    PERFORM powa_log('truncating powa_statements_history_current');
    TRUNCATE TABLE powa_statements_history_current;

    PERFORM powa_log('truncating powa_statements_history_db');
    TRUNCATE TABLE powa_statements_history_db;

    PERFORM powa_log('truncating powa_statements_history_current_db');
    TRUNCATE TABLE powa_statements_history_current_db;

    PERFORM powa_log('truncating powa_statements');
    -- if 3rd part datasource has FK on it, throw everything away
    TRUNCATE TABLE powa_statements CASCADE;
    RETURN true;
END;
$function$; /* end of powa_statements_reset */


CREATE OR REPLACE FUNCTION public.powa_user_functions_reset()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM powa_log('truncating powa_user_functions_history');
    TRUNCATE TABLE powa_user_functions_history;

    PERFORM powa_log('truncating powa_user_functions_history_current');
    TRUNCATE TABLE powa_user_functions_history_current;
    RETURN true;
END;
$function$; /* end of powa_user_functions_reset */


CREATE OR REPLACE FUNCTION public.powa_all_relations_reset()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM powa_log('truncating powa_all_relations_history');
    TRUNCATE TABLE powa_all_relations_history;

    PERFORM powa_log('truncating powa_all_relations_history_current');
    TRUNCATE TABLE powa_all_relations_history_current;
    RETURN true;
END;
$function$; /* end of powa_all_relations_reset */


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
            PERFORM powa_log('registering pg_stat_kcache');

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
language plpgsql;

/*
 * powa_kcache snapshot collection.
 */
CREATE OR REPLACE FUNCTION powa_kcache_snapshot() RETURNS void as $PROC$
DECLARE
  result bool;
    v_funcname    text := 'powa_kcache_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

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

    SELECT COUNT(*) into v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true;
END
$PROC$ language plpgsql; /* end of powa_kcache_unregister */


/*
 * powa_kcache aggregation
 */
CREATE OR REPLACE FUNCTION powa_kcache_aggregate() RETURNS void AS $PROC$
DECLARE
    result     bool;
    v_funcname text := 'powa_kcache_aggregate';
    v_rowcount bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

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

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_kcache_metrics) - rowcount: %s',
            v_funcname, v_rowcount));

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

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_kcache_metrics_db) - rowcount: %s',
            v_funcname, v_rowcount));

    TRUNCATE powa_kcache_metrics_current_db;
END
$PROC$ language plpgsql; /* end of powa_kcache_aggregate */


/*
 * powa_kcache purge
 */
CREATE OR REPLACE FUNCTION powa_kcache_purge() RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_kcache_purge';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    DELETE FROM powa_kcache_metrics WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_kcache_metrics) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_kcache_metrics_db WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_kcache_metrics_db) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ language plpgsql; /* end of powa_kcache_purge */


/*
 * powa_kcache reset
 */
CREATE OR REPLACE FUNCTION powa_kcache_reset() RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_kcache_reset';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log('running powa_kcache_reset');

    PERFORM powa_log('truncating powa_kcache_metrics');
    TRUNCATE TABLE powa_kcache_metrics;

    PERFORM powa_log('truncating powa_kcache_metrics_db');
    TRUNCATE TABLE powa_kcache_metrics_db;

    PERFORM powa_log('truncating powa_kcache_metrics_current');
    TRUNCATE TABLE powa_kcache_metrics_current;

    PERFORM powa_log('truncating powa_kcache_metrics_current_db');
    TRUNCATE TABLE powa_kcache_metrics_current_db;
END;
$PROC$ language plpgsql; /* end of powa_kcache_reset */


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
            PERFORM powa_log('registering pg_qualstats');

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
language plpgsql; /* end of powa_qualstats_register */


CREATE OR REPLACE FUNCTION powa_qualstats_snapshot() RETURNS void as $PROC$
DECLARE
    result     bool;
    v_funcname text := 'powa_qualstats_snapshot';
    v_rowcount bigint;
BEGIN
  PERFORM powa_log(format('running %I', v_funcname));

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
  SELECT COUNT(*) into v_rowcount
  FROM capture;

  perform powa_log(format('%I - rowcount: %s',
        v_funcname, v_rowcount));

  result := true;
  PERFORM pg_qualstats_reset();
END
$PROC$ language plpgsql; /* end of powa_qualstats_snapshot */


/*
 * powa_qualstats aggregate
 */
CREATE OR REPLACE FUNCTION powa_qualstats_aggregate() RETURNS void AS $PROC$
DECLARE
  result bool;
BEGIN
  PERFORM powa_log('running powa_qualstats_aggregate');

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
$PROC$ language plpgsql; /* end of powa_qualstats_aggregate */


/*
 * powa_qualstats_purge
 */
CREATE OR REPLACE FUNCTION powa_qualstats_purge() RETURNS void as $PROC$
BEGIN
  PERFORM powa_log('running powa_qualstats_purge');
  DELETE FROM powa_qualstats_constvalues_history WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
  DELETE FROM powa_qualstats_quals_history WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
END;
$PROC$ language plpgsql; /* end of powa_qualstats_purge */


/*
 * powa_qualstats_reset
 */
CREATE OR REPLACE FUNCTION powa_qualstats_reset() RETURNS void as $PROC$
BEGIN
  PERFORM powa_log('running powa_qualstats_reset');

  PERFORM powa_log('truncating powa_qualstats_quals');
  TRUNCATE TABLE powa_qualstats_quals CASCADE;
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
            PERFORM powa_log('registering pg_track_settings');

            -- This extension handles its own storage, just its snapshot
            -- function and an unregister function.
            INSERT INTO powa_functions (module, operation, function_name, added_manually, enabled)
            VALUES ('pg_track_settings', 'snapshot',   'pg_track_settings_snapshot',   false, true),
                   ('pg_track_settings', 'unregister', 'powa_track_settings_unregister',   false, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$ language plpgsql; /* end of powa_qualstats_unregister */


CREATE OR REPLACE function public.powa_track_settings_unregister() RETURNS bool AS
$_$
BEGIN
    PERFORM powa_log('unregistering pg_track_settings');
    DELETE FROM public.powa_functions WHERE module = 'pg_track_settings';
    RETURN true;
END;
$_$
language plpgsql; /* end of powa_track_settings_unregister */

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

/* pg_stat_all_relations operator support */
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

/* pg_stat_kcache operator support */

CREATE TYPE powa_kcache_diff AS (
    intvl interval,
    reads bigint,
    writes bigint,
    user_time double precision,
    system_time double precision
);

CREATE OR REPLACE FUNCTION powa_kcache_mi(
    a kcache_type,
    b kcache_type)
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

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR - (
    PROCEDURE = powa_kcache_mi,
    LEFTARG = kcache_type,
    RIGHTARG = kcache_type
);

CREATE TYPE powa_kcache_rate AS (
    sec integer,
    reads_per_sec double precision,
    writes_per_sec double precision,
    user_time_per_sec double precision,
    system_time_per_sec double precision
);

CREATE OR REPLACE FUNCTION powa_kcache_div(
    a kcache_type,
    b kcache_type)
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

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR / (
    PROCEDURE = powa_kcache_div,
    LEFTARG = kcache_type,
    RIGHTARG = kcache_type
);

/* end of pg_stat_kcache operator support */

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
