-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

/* capture functions */
/* part1: powa_functions table */
ALTER TABLE powa_functions ADD COLUMN query_source text DEFAULT NULL;

UPDATE powa_functions
   SET query_source = 'get_statements_capture_data'
 WHERE module = 'pg_stat_statements'
   AND operation = 'snapshot';

UPDATE powa_functions
   SET query_source = 'get_user_functions_capture_data'
 WHERE module = 'powa_stat_user_functions'
   AND operation = 'snapshot';

UPDATE powa_functions
   SET query_source = 'get_all_relations_capture_data'
 WHERE module = 'powa_stat_all_relations'
   AND operation = 'snapshot';

UPDATE powa_functions
   SET query_source = 'get_qualstats_capture_data'
 WHERE module = 'pg_qualstats'
   AND operation = 'snapshot';

UPDATE powa_functions
   SET query_source = 'get_kcache_capture_data'
 WHERE module = 'pg_stat_kcache'
   AND operation = 'snapshot';

/* capture functions */
/* part2: capture and snapshot functions */
CREATE OR REPLACE FUNCTION get_statements_capture_data() RETURNS SETOF pg_stat_statements AS $PROC$
    SELECT pgss.*
    FROM pg_stat_statements pgss
    JOIN pg_roles r ON pgss.userid = r.oid
    WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
    AND NOT (r.rolname = ANY (string_to_array(current_setting('powa.ignored_users'),',')));
$PROC$ LANGUAGE sql; /* end of get_statements_capture_data() */

CREATE OR REPLACE FUNCTION powa_statements_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_statements_snapshot';
    v_rowcount    bigint;
BEGIN
    -- In this function, we capture statements, and also aggregate counters by database
    -- so that the first screens of powa stay reactive even though there may be thousands
    -- of different statements
    PERFORM powa_log(format('running %I', v_funcname));

    WITH capture AS(
        SELECT * FROM get_statements_capture_data()
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


CREATE OR REPLACE FUNCTION get_user_functions_capture_data(
    OUT dbid oid,
    OUT funcid oid,
    OUT calls bigint,
    OUT total_time double precision,
    OUT self_time double precision
) RETURNS SETOF record AS $PROC$
        SELECT oid, r.funcid, r.calls, r.total_time, r.self_time
        FROM pg_database d, powa_stat_user_functions(oid) r
$PROC$ LANGUAGE sql; /* end of get_user_functions_capture_data */

CREATE OR REPLACE FUNCTION powa_user_functions_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_user_functions_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Insert cluster-wide user function statistics
    WITH func AS (
        SELECT * FROM get_user_functions_capture_data()
    )
    INSERT INTO powa_user_functions_history_current
        SELECT dbid, funcid,
        ROW(now(), calls, total_time,
            self_time)::powa_user_functions_history_record AS record
        FROM func;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_user_functions_snapshot */

CREATE OR REPLACE FUNCTION get_all_relations_capture_data(
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
    OUT autoanalyze_count bigint)
RETURNS SETOF record AS $PROC$
       SELECT d.oid AS dbid, r.relid, r.numscan, r.tup_returned, r.tup_fetched,
       r.n_tup_ins, r.n_tup_upd, r.n_tup_del, r.n_tup_hot_upd, r.n_liv_tup,
       r.n_dead_tup, r.n_mod_since_analyze, r.blks_read, r.blks_hit, r.last_vacuum,
       r.vacuum_count, r.last_autovacuum, r.autovacuum_count, r.last_analyze,
       r.analyze_count, r.last_autoanalyze, r.autoanalyze_count
       FROM pg_database d, powa_stat_all_rel(d.oid) as r
$PROC$ LANGUAGE sql; /* end of get_all_relations_capture_data */

CREATE OR REPLACE FUNCTION powa_all_relations_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_all_relations_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Insert cluster-wide relation statistics
    WITH rel AS (
        SELECT * FROM get_all_relations_capture_data()
    )
    INSERT INTO powa_all_relations_history_current
        SELECT dbid, relid,
        ROW(now(), numscan, tup_returned, tup_fetched,
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

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_all_relations_snapshot */


CREATE OR REPLACE FUNCTION get_kcache_capture_data(OUT queryid bigint, OUT userid oid, OUT dbid oid,
OUT reads bigint, OUT writes bigint, OUT user_time double precision, OUT system_time double precision)
RETURNS SETOF record AS $PROC$
BEGIN
        RETURN QUERY
        SELECT k.queryid, k.userid, k.dbid, k.reads, k.writes, k.user_time, k.system_time
        FROM pg_stat_kcache() k
        JOIN pg_roles r ON r.oid = k.userid
        WHERE NOT (r.rolname = ANY (string_to_array(current_setting('powa.ignored_users'),',')));
END;
$PROC$ LANGUAGE plpgsql; /* end of get_kcache_capture_data() */

CREATE OR REPLACE FUNCTION powa_kcache_snapshot() RETURNS void as $PROC$
DECLARE
  result bool;
    v_funcname    text := 'powa_kcache_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    WITH capture AS (
        SELECT * FROM get_kcache_capture_data()
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
$PROC$ language plpgsql; /* end of powa_kcache_snapshot */


CREATE OR REPLACE FUNCTION get_qualstats_capture_data(
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
    RETURN QUERY
    SELECT pgqs.uniquequalnodeid, pgqs.dbid, pgqs.userid, pgqs.qualnodeid,
           pgqs.occurences, pgqs.execution_count, pgqs.nbfiltered,
           pgqs.queryid, pgqs.constvalues, pgqs.quals
    FROM pg_qualstats_by_query pgqs
    JOIN powa_statements s USING(queryid, dbid, userid)
    JOIN pg_roles r ON s.userid = r.oid
    AND NOT (r.rolname = ANY (string_to_array(current_setting('powa.ignored_users'),',')));
END;
$PROC$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION powa_qualstats_snapshot() RETURNS void as $PROC$
DECLARE
    result     bool;
    v_funcname text := 'powa_qualstats_snapshot';
    v_rowcount bigint;
BEGIN
  PERFORM powa_log(format('running %I', v_funcname));

  WITH capture AS (
    SELECT * FROM get_qualstats_capture_data()
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

/* capture functions */
/* part3: register functions */
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

            INSERT INTO powa_functions (module, operation, function_name, query_source, added_manually, enabled)
            VALUES ('pg_stat_kcache', 'snapshot',   'powa_kcache_snapshot',    'get_kcache_capture_data', false, true),
                   ('pg_stat_kcache', 'aggregate',  'powa_kcache_aggregate',   NULL,                      false, true),
                   ('pg_stat_kcache', 'unregister', 'powa_kcache_unregister',  NULL,                      false, true),
                   ('pg_stat_kcache', 'purge',      'powa_kcache_purge',       NULL,                      false, true),
                   ('pg_stat_kcache', 'reset',      'powa_kcache_reset',       NULL,                      false, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql; /* end of powa_kcache_register */

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

            INSERT INTO powa_functions (module, operation, function_name, query_source, added_manually, enabled)
            VALUES ('pg_qualstats', 'snapshot',   'powa_qualstats_snapshot',    'get_qualstats_capture_data', false, true),
                   ('pg_qualstats', 'aggregate',  'powa_qualstats_aggregate',   NULL,                         false, true),
                   ('pg_qualstats', 'unregister', 'powa_qualstats_unregister',  NULL,                         false, true),
                   ('pg_qualstats', 'purge',      'powa_qualstats_purge',       NULL,                         false, true),
                   ('pg_qualstats', 'reset',      'powa_qualstats_reset',       NULL,                         false, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$
language plpgsql; /* end of powa_qualstats_register */


/* pg_wait_sampling integration - part 1 */
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
    coalesce_range tstzrange NOT NULL,
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    records public.wait_sampling_type[] NOT NULL,
    mins_in_range public.wait_sampling_type NOT NULL,
    maxs_in_range public.wait_sampling_type NOT NULL,
    PRIMARY KEY (coalesce_range, queryid, dbid, event_type, event)
);

CREATE INDEX ON public.powa_wait_sampling_history (queryid);

CREATE TABLE public.powa_wait_sampling_history_db (
    coalesce_range tstzrange NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    records public.wait_sampling_type[] NOT NULL,
    mins_in_range public.wait_sampling_type NOT NULL,
    maxs_in_range public.wait_sampling_type NOT NULL,
    PRIMARY KEY (coalesce_range, dbid, event_type, event)
);

CREATE TABLE public.powa_wait_sampling_history_current (
    queryid bigint NOT NULL,
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    record wait_sampling_type NOT NULL
);

CREATE TABLE public.powa_wait_sampling_history_current_db (
    dbid oid NOT NULL,
    event_type text NOT NULL,
    event text NOT NULL,
    record wait_sampling_type NOT NULL
);

/* end of pg_wait_sampling integration - part 1 */

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
            INSERT INTO powa_functions (module, operation, function_name, query_source, added_manually, enabled)
            VALUES ('pg_track_settings', 'snapshot',   'pg_track_settings_snapshot',     NULL, false, true),
                   ('pg_track_settings', 'unregister', 'powa_track_settings_unregister', NULL, false, true);
        END IF;
    END IF;

    RETURN true;
END;
$_$ language plpgsql; /* end of pg_track_settings_register */

/* end pg_track_settings integration */

/* pg_wait_sampling integration - part 2 */

/*
 * register pg_wait_sampling extension
 */
CREATE OR REPLACE function public.powa_wait_sampling_register() RETURNS bool AS
$_$
DECLARE
    v_func_present bool;
    v_ext_present bool;
BEGIN
    SELECT COUNT(*) = 1 INTO v_ext_present FROM pg_extension WHERE extname = 'pg_wait_sampling';

    IF ( v_ext_present ) THEN
        SELECT COUNT(*) > 0 INTO v_func_present FROM public.powa_functions WHERE module = 'pg_wait_sampling';
        IF ( NOT v_func_present) THEN
            PERFORM powa_log('registering pg_wait_sampling');

            INSERT INTO powa_functions (module, operation, function_name, query_source, added_manually, enabled)
            VALUES ('pg_wait_sampling', 'snapshot',   'powa_wait_sampling_snapshot',   'get_wait_sampling_capture_data', false, true),
                   ('pg_wait_sampling', 'aggregate',  'powa_wait_sampling_aggregate',  NULL,                             false, true),
                   ('pg_wait_sampling', 'unregister', 'powa_wait_sampling_unregister', NULL,                             false, true),
                   ('pg_wait_sampling', 'purge',      'powa_wait_sampling_purge',      NULL,                             false, true),
                   ('pg_wait_sampling', 'reset',      'powa_wait_sampling_reset',      NULL,                             false, true);
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

CREATE OR REPLACE FUNCTION get_wait_sampling_capture_data(
    OUT dbid oid,
    OUT event_type text,
    OUT event text,
    OUT queryid bigint,
    OUT count numeric)
RETURNS SETOF record AS $PROC$
BEGIN
        RETURN QUERY
        -- the various background processes report wait events but don't have
        -- associated queryid.  Gather them all under a fake 0 dbid
        SELECT COALESCE(pgss.dbid, 0) AS dbid, s.event_type, s.event, s.queryid,
            sum(s.count) as count
        FROM pg_wait_sampling_profile s
        -- pg_wait_sampling doesn't offer a per (userid, dbid, queryid) view,
        -- only per pid, but pid can be reused for different databases or users
        -- so we cannot deduce db or user from it.  However, queryid should be
        -- unique across differet databases, so we retrieve the dbid this way.
        LEFT JOIN pg_stat_statements(false) pgss ON pgss.queryid = s.queryid
        WHERE event_type IS NOT NULL AND event IS NOT NULL
        GROUP BY pgss.dbid, s.event_type, s.event, s.queryid;
END;
$PROC$ LANGUAGE plpgsql;

/*
 * powa_wait_sampling snapshot collection.
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_snapshot() RETURNS void as $PROC$
DECLARE
  result bool;
    v_funcname    text := 'powa_wait_sampling_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    WITH capture AS (
        SELECT * FROM get_wait_sampling_capture_data()
    ),

    by_query AS (
        INSERT INTO powa_wait_sampling_history_current (queryid, dbid,
                event_type, event, record)
            SELECT queryid, dbid, event_type, event, (now(), count)::wait_sampling_type
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_wait_sampling_history_current_db (dbid,
                event_type, event, record)
            SELECT dbid, event_type, event, (now(), sum(count))::wait_sampling_type
            FROM capture
            GROUP BY dbid, event_type, event
    )

    SELECT COUNT(*) into v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true;
END
$PROC$ language plpgsql; /* end of powa_wait_sampling_snapshot */

/*
 * powa_wait_sampling aggregation
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_aggregate() RETURNS void AS $PROC$
DECLARE
    result     bool;
    v_funcname text := 'powa_wait_sampling_aggregate';
    v_rowcount bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- aggregate history table
    LOCK TABLE powa_wait_sampling_history_current IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_wait_sampling_history (coalesce_range, queryid, dbid,
            event_type, event, records, mins_in_range, maxs_in_range)
        SELECT tstzrange(min((record).ts), max((record).ts),'[]'),
            queryid, dbid, event_type, event, array_agg(record),
        ROW(min((record).ts),
            min((record).count))::wait_sampling_type,
        ROW(max((record).ts),
            max((record).count))::wait_sampling_type
        FROM powa_wait_sampling_history_current
        GROUP BY queryid, dbid, event_type, event;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_wait_sampling_history) - rowcount: %s',
            v_funcname, v_rowcount));

    TRUNCATE powa_wait_sampling_history_current;

    -- aggregate history_db table
    LOCK TABLE powa_wait_sampling_history_current_db IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_wait_sampling_history_db (coalesce_range, dbid,
            event_type, event, records, mins_in_range, maxs_in_range)
        SELECT tstzrange(min((record).ts), max((record).ts),'[]'), dbid,
            event_type, event, array_agg(record),
        ROW(min((record).ts),
            min((record).count))::wait_sampling_type,
        ROW(max((record).ts),
            max((record).count))::wait_sampling_type
        FROM powa_wait_sampling_history_current_db
        GROUP BY dbid, event_type, event;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_wait_sampling_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    TRUNCATE powa_wait_sampling_history_current_db;
END
$PROC$ language plpgsql; /* end of powa_wait_sampling_aggregate */

/*
 * powa_wait_sampling purge
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_purge() RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_wait_sampling_purge';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    DELETE FROM powa_wait_sampling_history WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_wait_sampling_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_wait_sampling_history_db WHERE upper(coalesce_range) < (now() - current_setting('powa.retention')::interval);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_wait_sampling_history_db) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ language plpgsql; /* end of powa_wait_sampling_purge */

/*
 * powa_wait_sampling reset
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_reset() RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_wait_sampling_reset';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log('running powa_wait_sampling_reset');

    PERFORM powa_log('truncating powa_wait_sampling_history');
    TRUNCATE TABLE powa_wait_sampling_history;

    PERFORM powa_log('truncating powa_wait_sampling_history_db');
    TRUNCATE TABLE powa_wait_sampling_history_db;

    PERFORM powa_log('truncating powa_wait_sampling_history_current');
    TRUNCATE TABLE powa_wait_sampling_history_current;

    PERFORM powa_log('truncating powa_wait_sampling_history_current_db');
    TRUNCATE TABLE powa_wait_sampling_history_current_db;
END;
$PROC$ language plpgsql; /* end of powa_wait_sampling_reset */

-- By default, try to register pg_wait_sampling, in case it's alreay here
SELECT * FROM public.powa_wait_sampling_register();

/* end of pg_wait_sampling integration - part 2 */
