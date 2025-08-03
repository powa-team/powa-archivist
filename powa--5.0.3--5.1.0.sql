-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

-- Replace a bunch of indexes on the *_current(srvid) to include the columns
-- used in the aggregate functions
CREATE INDEX ON @extschema@.powa_statements_history_current(srvid, queryid, dbid);
DROP INDEX @extschema@.powa_statements_history_current_srvid_idx;

CREATE INDEX ON @extschema@.powa_kcache_metrics_current(srvid, queryid, top, dbid);
DROP INDEX @extschema@.powa_kcache_metrics_current_srvid_idx;

CREATE INDEX ON @extschema@.powa_all_indexes_history_current(srvid, dbid, relid);
DROP INDEX @extschema@.powa_all_indexes_history_current_srvid_idx;

CREATE INDEX ON @extschema@.powa_wait_sampling_history_current(srvid, queryid, dbid);
DROP INDEX @extschema@.powa_wait_sampling_history_current_srvid_idx;

ALTER TABLE @extschema@.powa_stat_activity_src_tmp
    ADD COLUMN clock_ts timestamp with time zone NOT NULL;

ALTER TYPE @extschema@.powa_stat_activity_history_record
    ADD ATTRIBUTE clock_ts timestamp with time zone;

ALTER TYPE @extschema@.powa_stat_activity_history_record_minmax
    ADD ATTRIBUTE clock_ts timestamp with time zone;

DROP FUNCTION @extschema@.powa_stat_activity_src(_srvid integer);
CREATE OR REPLACE FUNCTION @extschema@.powa_stat_activity_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT cur_txid xid,
    OUT datid oid,
    OUT pid integer,
    OUT leader_pid integer,
    OUT usesysid oid,
    OUT application_name text,
    OUT client_addr inet,
    OUT backend_start timestamp with time zone,
    OUT xact_start timestamp with time zone,
    OUT query_start timestamp with time zone,
    OUT state_change timestamp with time zone,
    OUT state text,
    OUT backend_xid xid,
    OUT backend_xmin xid,
    OUT query_id bigint,
    OUT backend_type text,
    OUT clock_ts timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    txid xid;
    v_server_version int;
BEGIN
    IF (_srvid = 0) THEN
        v_server_version := current_setting('server_version_num')::int;

        IF pg_catalog.pg_is_in_recovery() THEN
            txid = NULL;
        ELSE
            -- xid() was introduced in pg13
            IF v_server_version >= 130000 THEN
                txid = pg_catalog.xid(pg_catalog.pg_current_xact_id());
            ELSE
                txid = (txid_current()::bigint - (txid_current()::bigint >> 32 << 32))::text::xid;
            END IF;
        END IF;

        -- query_id added in pg14
        IF v_server_version >= 140000 THEN
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, s.leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, s.query_id, s.backend_type,
                clock_timestamp() AS clock_ts
            FROM pg_catalog.pg_stat_activity AS s;
        -- leader_pid added in pg13+
        ELSIF v_server_version >= 130000 THEN
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, s.leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, NULL::bigint AS query_id, s.backend_type,
                clock_timestamp() AS clock_ts
            FROM pg_catalog.pg_stat_activity AS s;
        -- backend_type added in pg10+
        ELSIF v_server_version >= 100000 THEN
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, NULL::integer AS leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, NULL::bigint AS query_id, s.backend_type,
                clock_timestamp() AS clock_ts
            FROM pg_catalog.pg_stat_activity AS s;
        ELSE
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, NULL::integer AS leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, NULL::bigint AS query_id,
                NULL::text AS backend_type,
                clock_timestamp() AS clock_ts
            FROM pg_catalog.pg_stat_activity AS s;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.cur_txid,
            s.datid, s.pid, s.leader_pid, s.usesysid,
            s.application_name, s.client_addr, s.backend_start,
            s.xact_start,
            s.query_start, s.state_change, s.state, s.backend_xid,
            s.backend_xmin, s.query_id, s.backend_type,
            s.clock_ts
        FROM @extschema@.powa_stat_activity_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_activity_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_activity_snapshot(_srvid integer)
 RETURNS void
AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('public.%I(%s)',
                                 'powa_stat_activity_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM public.powa_log(format('running %s', v_funcname));

    PERFORM public.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert background writer statistics
    WITH rel AS (
        SELECT *
        FROM public.powa_stat_activity_src(_srvid)
    )
    INSERT INTO public.powa_stat_activity_history_current
        SELECT _srvid,
        ROW(ts, cur_txid, datid, pid,
            leader_pid, usesysid, application_name,
            client_addr, backend_start, xact_start,
            query_start, state_change, state,
            backend_xid, backend_xmin, query_id,
            backend_type, clock_ts)::public.powa_stat_activity_history_record AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM public.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM public.powa_stat_activity_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_activity_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_activity_aggregate(_srvid integer)
 RETURNS void
AS $PROC$
DECLARE
    v_funcname    text := format('public.%I(%s)',
                                 'powa_stat_activity_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM public.powa_log(format('running %s', v_funcname));

    PERFORM public.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate powa_stat_activity history table
    INSERT INTO public.powa_stat_activity_history
        SELECT srvid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                    min((record).datid),
                    min((record).pid),
                    min((record).leader_pid),
                    min((record).usesysid),
                    min((record).application_name),
                    min((record).client_addr),
                    min((record).backend_start),
                    min((record).xact_start),
                    min((record).query_start),
                    min((record).state_change),
                    min((record).state),
                    min((record).query_id),
                    min((record).backend_type),
                    min((record).clock_ts))::public.powa_stat_activity_history_record_minmax,
            ROW(max((record).ts),
                    max((record).datid),
                    max((record).pid),
                    max((record).leader_pid),
                    max((record).usesysid),
                    max((record).application_name),
                    max((record).client_addr),
                    max((record).backend_start),
                    max((record).xact_start),
                    max((record).query_start),
                    max((record).state_change),
                    max((record).state),
                    max((record).query_id),
                    max((record).backend_type),
                    max((record).clock_ts))::public.powa_stat_activity_history_record_minmax
        FROM public.powa_stat_activity_history_current
        WHERE srvid = _srvid
        GROUP BY srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM public.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM public.powa_stat_activity_history_current WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_activity_aggregate */
