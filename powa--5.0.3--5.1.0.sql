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
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_activity_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert background writer statistics
    WITH rel AS (
        SELECT *
        FROM @extschema@.powa_stat_activity_src(_srvid)
    )
    INSERT INTO @extschema@.powa_stat_activity_history_current
        SELECT _srvid,
        ROW(ts, cur_txid, datid, pid,
            leader_pid, usesysid, application_name,
            client_addr, backend_start, xact_start,
            query_start, state_change, state,
            backend_xid, backend_xmin, query_id,
            backend_type, clock_ts)::@extschema@.powa_stat_activity_history_record AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_stat_activity_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_activity_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_activity_aggregate(_srvid integer)
 RETURNS void
AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_activity_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate powa_stat_activity history table
    INSERT INTO @extschema@.powa_stat_activity_history
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
                    min((record).clock_ts))::@extschema@.powa_stat_activity_history_record_minmax,
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
                    max((record).clock_ts))::@extschema@.powa_stat_activity_history_record_minmax
        FROM @extschema@.powa_stat_activity_history_current
        WHERE srvid = _srvid
        GROUP BY srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_stat_activity_history_current WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_activity_aggregate */

ALTER TABLE @extschema@.powa_module_functions
    ADD COLUMN added_manually boolean NOT NULL default true;

UPDATE @extschema@.powa_module_functions
    SET added_manually = false
    WHERE module in (
        'pg_database',
        'pg_role',
        'pg_replication_slots',
        'pg_stat_activity',
        'pg_stat_archiver',
        'pg_stat_bgwriter',
        'pg_stat_checkpointer',
        'pg_stat_database',
        'pg_stat_database_conflicts',
        'pg_stat_io',
        'pg_stat_replication',
        'pg_stat_slru',
        'pg_stat_subscription',
        'pg_stat_subscription_stats',
        'pg_stat_wal',
        'pg_stat_wal_receiver'
    );
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_module_functions','WHERE added_manually');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_module_config','WHERE srvid != 0');

ALTER TYPE @extschema@.powa_stat_io_history_db_record
    ADD ATTRIBUTE read_bytes numeric,
    ADD ATTRIBUTE write_bytes numeric,
    ADD ATTRIBUTE extend_bytes numeric;

ALTER TYPE @extschema@.powa_stat_io_history_diff
    ADD ATTRIBUTE read_bytes numeric,
    ADD ATTRIBUTE write_bytes numeric,
    ADD ATTRIBUTE extend_bytes numeric;

ALTER TYPE @extschema@.powa_stat_io_history_rate
    ADD ATTRIBUTE read_bytes_per_sec numeric,
    ADD ATTRIBUTE write_bytes_per_sec numeric,
    ADD ATTRIBUTE extend_bytes_per_sec numeric;

ALTER TYPE @extschema@.powa_stat_io_history_record
    ADD ATTRIBUTE read_bytes numeric,
    ADD ATTRIBUTE write_bytes numeric,
    ADD ATTRIBUTE extend_bytes numeric;

ALTER TABLE @extschema@.powa_stat_io_src_tmp
    ADD COLUMN read_bytes numeric NOT NULL,
    ADD COLUMN write_bytes numeric NOT NULL,
    ADD COLUMN extend_bytes numeric NOT NULL;

DROP FUNCTION @extschema@.powa_stat_io_src(_srvid integer);
CREATE OR REPLACE FUNCTION @extschema@.powa_stat_io_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT backend_type text,
    OUT object text,
    OUT context text,
    OUT reads bigint,
    OUT read_time double precision,
    OUT writes bigint,
    OUT write_time double precision,
    OUT writebacks bigint,
    OUT writeback_time double precision,
    OUT extends bigint,
    OUT extend_time double precision,
    OUT op_bytes bigint,
    OUT hits bigint,
    OUT evictions bigint,
    OUT reuses bigint,
    OUT fsyncs bigint,
    OUT fsync_time double precision,
    OUT stats_reset timestamp with time zone,
    OUT read_bytes numeric,
    OUT write_bytes numeric,
    OUT extend_bytes numeric
) RETURNS SETOF record STABLE AS $PROC$
BEGIN
    IF (_srvid = 0) THEN
        -- pg18, op_bytes split into read_bytes, write_bytes and extend_bytes
        IF current_setting('server_version_num')::int >= 180000 THEN
            RETURN QUERY SELECT now(),
            s.backend_type, s.object, s.context,
            s.reads, s.read_time,
            s.writes, s.write_time,
            s.writebacks, s.writeback_time,
            s.extends, s.extend_time,
            0::bigint AS op_bytes, s.hits,
            s.evictions, s.reuses,
            s.fsyncs, s.fsync_time,
            s.stats_reset,
            s.read_bytes, s.write_bytes, s.extend_bytes
            FROM pg_catalog.pg_stat_io AS s;
        -- pg16+, the view is introduced
        ELSIF current_setting('server_version_num')::int >= 160000 THEN
            RETURN QUERY SELECT now(),
            s.backend_type, s.object, s.context,
            s.reads, s.read_time,
            s.writes, s.write_time,
            s.writebacks, s.writeback_time,
            s.extends, s.extend_time,
            s.op_bytes, s.hits,
            s.evictions, s.reuses,
            s.fsyncs, s.fsync_time,
            s.stats_reset,
            0::numeric AS read_bytes, 0::numeric AS write_bytes,
            0::numeric AS extend_bytes
            FROM pg_catalog.pg_stat_io AS s;
        ELSE -- return an empty dataset for pg15- servers
            RETURN QUERY SELECT now(),
            NULL::text AS backend_type, NULL::text AS object,
            NULL::text AS context,
            0::bigint AS reads, 0::double precision AS read_time,
            0::bigint AS writes, 0::double precision AS write_time,
            0::bigint AS writebacks, 0::double precision AS writeback_time,
            0::bigint AS extends, 0::double precision AS extend_time,
            NULL::bigint AS op_bytes, 0::bigint AS hits,
            0::bigint AS evictions, 0::bigint AS reuses,
            0::bigint AS fsyncs, 0::double precision AS fsync_time,
            NULL::timestamp with time zone AS stats_reset,
            NULL::numeric AS read_bytes, NULL::numeric AS write_bytes,
            NULL::numeric AS extend_bytes
            WHERE false;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.backend_type, s.object, s.context,
            s.reads, s.read_time,
            s.writes, s.write_time,
            s.writebacks, s.writeback_time,
            s.extends, s.extend_time,
            s.op_bytes, s.hits,
            s.evictions, s.reuses,
            s.fsyncs, s.fsync_time,
            s.stats_reset
        FROM @extschema@.powa_stat_io_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_io_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_io_snapshot(_srvid integer)
RETURNS void
AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_io_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert background writer statistics
    WITH rel AS (
        SELECT *
        FROM @extschema@.powa_stat_io_src(_srvid)
    )
    INSERT INTO @extschema@.powa_stat_io_history_current
        SELECT _srvid, backend_type, object, context,
        ROW(ts, reads, read_time, writes,
            write_time, writebacks, writeback_time,
            extends, extend_time, op_bytes,
            hits, evictions, reuses,
            fsyncs, fsync_time, stats_reset,
            read_bytes, write_bytes, extend_bytes)::@extschema@.powa_stat_io_history_record AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_stat_io_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$
LANGUAGE plpgsql;   /* end of powa_stat_io_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_io_aggregate(_srvid integer)
RETURNS void
AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_io_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate powa_stat_io history table
    INSERT INTO @extschema@.powa_stat_io_history
        SELECT srvid, backend_type, object, context,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                    min((record).reads),
                    min((record).read_time),
                    min((record).writes),
                    min((record).write_time),
                    min((record).writebacks),
                    min((record).writeback_time),
                    min((record).extends),
                    min((record).extend_time),
                    min((record).op_bytes),
                    min((record).hits),
                    min((record).evictions),
                    min((record).reuses),
                    min((record).fsyncs),
                    min((record).fsync_time),
                    min((record).stats_reset),
                    min((record).read_bytes),
                    min((record).write_bytes),
                    min((record).extend_bytes))::@extschema@.powa_stat_io_history_record,
            ROW(max((record).ts),
                    max((record).reads),
                    max((record).read_time),
                    max((record).writes),
                    max((record).write_time),
                    max((record).writebacks),
                    max((record).writeback_time),
                    max((record).extends),
                    max((record).extend_time),
                    max((record).op_bytes),
                    max((record).hits),
                    max((record).evictions),
                    max((record).reuses),
                    max((record).fsyncs),
                    max((record).fsync_time),
                    max((record).stats_reset),
                    max((record).read_bytes),
                    max((record).write_bytes),
                    max((record).extend_bytes))::@extschema@.powa_stat_io_history_record
        FROM @extschema@.powa_stat_io_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, backend_type, object, context;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_stat_io_history_current WHERE srvid = _srvid;
 END;
$PROC$
LANGUAGE plpgsql;   /* end of powa_stat_io_aggregate */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_io_history_db_div(
    a @extschema@.powa_stat_io_history_db_record,
    b @extschema@.powa_stat_io_history_db_record)
RETURNS @extschema@.powa_stat_io_history_rate
AS $PROC$
DECLARE
    res @extschema@.powa_stat_io_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.reads_per_sec = (a.reads - b.reads)::double precision / sec;
    res.read_time_per_sec = (a.read_time - b.read_time)::double precision / sec;
    res.writes_per_sec = (a.writes - b.writes)::double precision / sec;
    res.write_time_per_sec = (a.write_time - b.write_time)::double precision / sec;
    res.writebacks_per_sec = (a.writebacks - b.writebacks)::double precision / sec;
    res.writeback_time_per_sec = (a.writeback_time - b.writeback_time)::double precision / sec;
    res.extends_per_sec = (a.extends - b.extends)::double precision / sec;
    res.extend_time_per_sec = (a.extend_time - b.extend_time)::double precision / sec;
    res.op_bytes_per_sec = (a.op_bytes - b.op_bytes)::double precision / sec;
    res.hits_per_sec = (a.hits - b.hits)::double precision / sec;
    res.evictions_per_sec = (a.evictions - b.evictions)::double precision / sec;
    res.reuses_per_sec = (a.reuses - b.reuses)::double precision / sec;
    res.fsyncs_per_sec = (a.fsyncs - b.fsyncs)::double precision / sec;
    res.fsync_time_per_sec = (a.fsync_time - b.fsync_time)::double precision / sec;
    res.read_bytes_per_sec = (a.read_bytes - b.read_bytes)::double precision / sec;
    res.write_bytes_per_sec = (a.write_bytes - b.write_bytes)::double precision / sec;
    res.extend_bytes_per_sec = (a.extend_bytes - b.extend_bytes)::double precision / sec;

    return res;
END;
$PROC$
LANGUAGE plpgsql
IMMUTABLE STRICT;   /* end of powa_stat_io_history_db_div */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_io_history_db_mi(
    a @extschema@.powa_stat_io_history_db_record,
    b @extschema@.powa_stat_io_history_db_record)
RETURNS @extschema@.powa_stat_io_history_diff
AS $PROC$
DECLARE
    res @extschema@.powa_stat_io_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.reads = a.reads - b.reads;
    res.read_time = a.read_time - b.read_time;
    res.writes = a.writes - b.writes;
    res.write_time = a.write_time - b.write_time;
    res.writebacks = a.writebacks - b.writebacks;
    res.writeback_time = a.writeback_time - b.writeback_time;
    res.extends = a.extends - b.extends;
    res.extend_time = a.extend_time - b.extend_time;
    res.op_bytes = a.op_bytes - b.op_bytes;
    res.hits = a.hits - b.hits;
    res.evictions = a.evictions - b.evictions;
    res.reuses = a.reuses - b.reuses;
    res.fsyncs = a.fsyncs - b.fsyncs;
    res.fsync_time = a.fsync_time - b.fsync_time;
    res.read_bytes = a.read_bytes - b.read_bytes;
    res.write_bytes = a.write_bytes - b.write_bytes;
    res.extend_bytes = a.extend_bytes - b.extend_bytes;

    return res;
END;
$PROC$
LANGUAGE plpgsql
IMMUTABLE STRICT;   /* end of powa_stat_io_history_db_mi */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_io_history_div(
    a @extschema@.powa_stat_io_history_record,
    b @extschema@.powa_stat_io_history_record)
RETURNS @extschema@.powa_stat_io_history_rate
AS $PROC$
DECLARE
    res @extschema@.powa_stat_io_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.reads_per_sec = (a.reads - b.reads)::double precision / sec;
    res.read_time_per_sec = (a.read_time - b.read_time)::double precision / sec;
    res.writes_per_sec = (a.writes - b.writes)::double precision / sec;
    res.write_time_per_sec = (a.write_time - b.write_time)::double precision / sec;
    res.writebacks_per_sec = (a.writebacks - b.writebacks)::double precision / sec;
    res.writeback_time_per_sec = (a.writeback_time - b.writeback_time)::double precision / sec;
    res.extends_per_sec = (a.extends - b.extends)::double precision / sec;
    res.extend_time_per_sec = (a.extend_time - b.extend_time)::double precision / sec;
    res.op_bytes_per_sec = (a.op_bytes - b.op_bytes)::double precision / sec;
    res.hits_per_sec = (a.hits - b.hits)::double precision / sec;
    res.evictions_per_sec = (a.evictions - b.evictions)::double precision / sec;
    res.reuses_per_sec = (a.reuses - b.reuses)::double precision / sec;
    res.fsyncs_per_sec = (a.fsyncs - b.fsyncs)::double precision / sec;
    res.fsync_time_per_sec = (a.fsync_time - b.fsync_time)::double precision / sec;
    res.read_bytes_per_sec = (a.read_bytes - b.read_bytes)::double precision / sec;
    res.write_bytes_per_sec = (a.write_bytes - b.write_bytes)::double precision / sec;
    res.extend_bytes_per_sec = (a.extend_bytes - b.extend_bytes)::double precision / sec;

    return res;
END;
$PROC$
LANGUAGE plpgsql
IMMUTABLE STRICT;   /* end of powa_stat_io_history_div */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_io_history_mi(
    a @extschema@.powa_stat_io_history_record,
    b @extschema@.powa_stat_io_history_record)
RETURNS @extschema@.powa_stat_io_history_diff
AS $PROC$
DECLARE
    res @extschema@.powa_stat_io_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.reads = a.reads - b.reads;
    res.read_time = a.read_time - b.read_time;
    res.writes = a.writes - b.writes;
    res.write_time = a.write_time - b.write_time;
    res.writebacks = a.writebacks - b.writebacks;
    res.writeback_time = a.writeback_time - b.writeback_time;
    res.extends = a.extends - b.extends;
    res.extend_time = a.extend_time - b.extend_time;
    res.op_bytes = a.op_bytes - b.op_bytes;
    res.hits = a.hits - b.hits;
    res.evictions = a.evictions - b.evictions;
    res.reuses = a.reuses - b.reuses;
    res.fsyncs = a.fsyncs - b.fsyncs;
    res.fsync_time = a.fsync_time - b.fsync_time;
    res.read_bytes = a.read_bytes - b.read_bytes;
    res.write_bytes = a.write_bytes - b.write_bytes;
    res.extend_bytes = a.extend_bytes - b.extend_bytes;

    return res;
END;
$PROC$
LANGUAGE plpgsql
IMMUTABLE STRICT;   /* end of powa_stat_io_history_mi */
