-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit
SET LOCAL statement_timeout = 0;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL standard_conforming_strings = on;
SET LOCAL client_min_messages = warning;
SET LOCAL escape_string_warning = off;
SET LOCAL search_path = public, pg_catalog;

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

-- previous powa version created an incorrect version of that function
DROP FUNCTION IF EXISTS powa_wait_sampling_unregister();
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

CREATE OR REPLACE FUNCTION public.powa_activate_extension(_srvid integer, _module text) RETURNS boolean
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

CREATE OR REPLACE FUNCTION powa_deactivate_extension(_srvid integer, _module text) RETURNS boolean
AS $_$
BEGIN
    UPDATE public.powa_functions
    SET enabled = false
    WHERE module = _module
    AND srvid = _srvid;

    return true;
END;
$_$ LANGUAGE plpgsql; /* end of powa_deactivate_extension */

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
