-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

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
            s.stats_reset,
            s.read_bytes, s.write_bytes, s.extend_bytes
        FROM @extschema@.powa_stat_io_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_io_src */

ALTER TABLE @extschema@.powa_stat_io_src_tmp
    ALTER COLUMN read_bytes DROP NOT NULL,
    ALTER COLUMN extend_bytes DROP NOT NULL;
