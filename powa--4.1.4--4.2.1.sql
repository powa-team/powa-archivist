-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

SET LOCAL statement_timeout = 0;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL standard_conforming_strings = on;
SET LOCAL client_min_messages = warning;
SET LOCAL search_path = public, pg_catalog;

ALTER TABLE public.powa_statements_src_tmp ADD toplevel boolean;
UPDATE public.powa_statements_src_tmp SET toplevel = true;
ALTER TABLE public.powa_statements_src_tmp ALTER COLUMN toplevel SET NOT NULL;

ALTER TABLE public.powa_statements_history ADD toplevel boolean;
UPDATE public.powa_statements_history SET toplevel = true;
ALTER TABLE public.powa_statements_history ALTER COLUMN toplevel SET NOT NULL;

ALTER TABLE public.powa_statements_history_current ADD toplevel boolean;
UPDATE public.powa_statements_history_current SET toplevel = true;
ALTER TABLE public.powa_statements_history_current ALTER COLUMN toplevel SET NOT NULL;

DROP FUNCTION powa_statements_src(integer);
CREATE OR REPLACE FUNCTION powa_statements_src(IN _srvid integer,
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
BEGIN
    IF (_srvid = 0) THEN
        SELECT regexp_split_to_array(extversion, E'\\.') INTO STRICT v_pgss
        FROM pg_extension
        WHERE extname = 'pg_stat_statements';

        IF (v_pgss[1] = 1 AND v_pgss[2] >= 10) THEN
            RETURN QUERY SELECT now(),
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
            FROM pg_stat_statements pgss
            JOIN pg_database d ON d.oid = pgss.dbid
            JOIN pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        powa_get_guc('powa.ignored_users', ''),
                        ',')));
        ELSIF (v_pgss[1] = 1 AND v_pgss[2] >= 8) THEN
            RETURN QUERY SELECT now(),
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
            FROM pg_stat_statements pgss
            JOIN pg_database d ON d.oid = pgss.dbid
            JOIN pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        powa_get_guc('powa.ignored_users', ''),
                        ',')));
        ELSE
            RETURN QUERY SELECT now(),
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
        INSERT INTO public.powa_statements_history_current (srvid, queryid,
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
            )::powa_statements_history_record AS record
            FROM capture
    ),

    by_database AS (
        INSERT INTO public.powa_statements_history_current_db (srvid, dbid, record)
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

CREATE OR REPLACE FUNCTION powa_statements_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_statements_aggregate(' || _srvid || ')';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate statements table
    INSERT INTO public.powa_statements_history (srvid, queryid, dbid, toplevel,
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
        GROUP BY srvid, queryid, dbid, toplevel, userid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_statements_history_current WHERE srvid = _srvid;

    -- aggregate db table
    INSERT INTO public.powa_statements_history_db (srvid, dbid, coalesce_range,
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

ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_reads DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_writes DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_user_time DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_system_time DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_minflts DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_majflts DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_nswaps DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_msgsnds DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_msgrcvs DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_nsignals DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_nvcsws DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN plan_nivcsws DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_reads DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_writes DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_minflts DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_majflts DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_nswaps DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_msgsnds DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_msgrcvs DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_nsignals DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_nvcsws DROP NOT NULL;
ALTER TABLE public.powa_kcache_src_tmp ALTER COLUMN exec_nivcsws DROP NOT NULL;
