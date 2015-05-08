-- complain if script is sourced in psql, rather than via ALTER EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

-- powa_functions now have an "unregister" operation
ALTER TABLE public.powa_functions DROP CONSTRAINT powa_functions_operation_check;
ALTER TABLE public.powa_functions ADD CHECK (operation IN ('snapshot','aggregate','purge','unregister'));

-- Handle automatic extensions registering
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

-- Handle automatic extensions unregistering
CREATE EVENT TRIGGER powa_check_dropped_extensions
    ON sql_drop
    WHEN tag IN ('DROP EXTENSION')
    EXECUTE PROCEDURE public.powa_check_dropped_extensions() ;

-- New powa_kcache_register function
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
            INSERT INTO powa_functions (module, operation, function_name, added_manually)
            VALUES ('pg_stat_kcache', 'snapshot',   'powa_kcache_snapshot',   false),
                   ('pg_stat_kcache', 'aggregate',  'powa_kcache_aggregate',  false),
                   ('pg_stat_kcache', 'unregister', 'powa_kcache_unregister', false),
                   ('pg_stat_kcache', 'purge',      'powa_kcache_purge',      false);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql;

-- New powa_qualstats_unregister function
CREATE OR REPLACE function public.powa_qualstats_unregister() RETURNS bool AS
$_$
BEGIN
    DELETE FROM public.powa_functions WHERE module = 'pg_qualstats';
    RETURN true;
END;
$_$
language plpgsql;

-- New powa_qualstats_register function
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
            INSERT INTO powa_functions (module, operation, function_name, added_manually)
            VALUES ('pg_qualstats', 'snapshot',   'powa_qualstats_snapshot',   false),
                   ('pg_qualstats', 'aggregate',  'powa_qualstats_aggregate',  false),
                   ('pg_qualstats', 'unregister', 'powa_qualstats_unregister', false),
                   ('pg_qualstats', 'purge',      'powa_qualstats_purge',      false);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql;

-- Add the _unregister() function in powa_functions if the related extension exists
WITH ext_exists AS (
    SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_kcache'
)
INSERT INTO public.powa_functions (module, operation, function_name, added_manually)
SELECT 'pg_stat_kcache', 'unregister', 'powa_kcache_unregister', false
FROM ext_exists;

WITH ext_exists AS (
    SELECT 1 FROM pg_extension WHERE extname = 'pg_qualstats'
)
INSERT INTO public.powa_functions (module, operation, function_name, added_manually)
SELECT 'pg_qualstats', 'unregister', 'powa_qualstats_unregister', false
FROM ext_exists;

-- Fix the "added_manually" value for pg_stat_kcache extension
UPDATE public.powa_functions
    SET added_manually = false WHERE module = 'pg_stat_kcache';


-----------------------------------------------------------
-- Fix the tstzrange inclusive upper bounds for
-- * powa_kcache_aggregate() function
-- * powa_qualstats_aggregate_constvalues_current view
-- * powa_qualstats_aggregate() function
-----------------------------------------------------------
CREATE OR REPLACE FUNCTION powa_kcache_aggregate() RETURNS void AS $PROC$
DECLARE
  result bool;
BEGIN
    RAISE DEBUG 'running powa_kcache_aggregate';

    -- aggregate metrics table
    LOCK TABLE powa_kcache_metrics_current IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_kcache_metrics (coalesce_range, queryid, dbid, userid, metrics)
        SELECT tstzrange(min((metrics).ts), max((metrics).ts),'[]'),
        queryid, dbid, userid, array_agg(metrics)
        FROM powa_kcache_metrics_current
        GROUP BY queryid, dbid, userid;

    TRUNCATE powa_kcache_metrics_current;

    -- aggregate metrics_db table
    LOCK TABLE powa_kcache_metrics_current_db IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_kcache_metrics_db (coalesce_range, dbid, metrics)
        SELECT tstzrange(min((metrics).ts), max((metrics).ts),'[]'),
        dbid, array_agg(metrics)
        FROM powa_kcache_metrics_current_db
        GROUP BY dbid;

    TRUNCATE powa_kcache_metrics_current_db;
END
$PROC$ language plpgsql;

CREATE OR REPLACE VIEW powa_qualstats_aggregate_constvalues_current AS
WITH consts AS (
  SELECT qualid, queryid, dbid, userid, min(ts) as mints, max(ts) as maxts, sum(nbfiltered) as nbfiltered,
  sum(count) as count, constvalues
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
  SELECT array_agg(constvalues) as mf
  FROM (
    SELECT (constvalues, nbfiltered, count)::qual_values as constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY CASE WHEN count = 0 THEN 0 ELSE nbfiltered / count::numeric END DESC
    LIMIT 20
  ) s
) as mf,
LATERAL (
  SELECT array_agg(constvalues) as lf
  FROM (
    SELECT (constvalues, nbfiltered, count)::qual_values as constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY CASE WHEN count = 0 THEN 0 ELSE nbfiltered / count::numeric END DESC
    LIMIT 20
  ) s
) as lf,
LATERAL (
  SELECT array_agg(constvalues) as me
  FROM (
    SELECT (constvalues, nbfiltered, count)::qual_values as constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY count desc
    LIMIT 20
  ) s
) as me;

CREATE OR REPLACE FUNCTION powa_qualstats_aggregate() RETURNS void AS $PROC$
DECLARE
  result bool;
BEGIN
  RAISE DEBUG 'running powa_qualstats_aggregate';
  LOCK TABLE powa_qualstats_constvalues_history_current IN SHARE MODE;
  LOCK TABLE powa_qualstats_quals_history_current IN SHARE MODE;
  INSERT INTO powa_qualstats_constvalues_history (
    qualid, queryid, dbid, userid, coalesce_range, most_filtering, least_filtering, most_executed)
    SELECT * FROM powa_qualstats_aggregate_constvalues_current;
  INSERT INTO powa_qualstats_quals_history (qualid, queryid, dbid, userid, coalesce_range, records)
    SELECT qualid, queryid, dbid, userid, tstzrange(min(ts), max(ts),'[]'), array_agg((ts, count, nbfiltered)::powa_qualstats_history_item)
    FROM powa_qualstats_quals_history_current
    GROUP BY qualid, queryid, dbid, userid;
  TRUNCATE powa_qualstats_constvalues_history_current;
  TRUNCATE powa_qualstats_quals_history_current;
END
$PROC$ language plpgsql;

-- Try to register handled extensions
SELECT * FROM public.powa_qualstats_register();
