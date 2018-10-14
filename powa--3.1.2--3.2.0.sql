-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

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

            INSERT INTO powa_functions (module, operation, function_name, added_manually, enabled)
            VALUES ('pg_wait_sampling', 'snapshot',   'powa_wait_sampling_snapshot',   false, true),
                   ('pg_wait_sampling', 'aggregate',  'powa_wait_sampling_aggregate',  false, true),
                   ('pg_wait_sampling', 'unregister', 'powa_wait_sampling_unregister', false, true),
                   ('pg_wait_sampling', 'purge',      'powa_wait_sampling_purge',      false, true),
                   ('pg_wait_sampling', 'reset',      'powa_wait_sampling_reset',      false, true);
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
        GROUP BY pgss.dbid, s.event_type, s.event, s.queryid
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
