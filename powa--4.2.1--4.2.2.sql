-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

DROP FUNCTION powa_statements_src(integer);
CREATE OR REPLACE FUNCTION powa_statements_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT userid oid,
    OUT dbid oid,
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
    OUT wal_bytes numeric,
    OUT toplevel boolean
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
                pgss.userid, pgss.dbid, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_exec_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time,
                pgss.plans, pgss.total_plan_time,
                pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes, pgss.toplevel
            FROM pg_stat_statements pgss
            JOIN pg_database d ON d.oid = pgss.dbid
            JOIN pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        powa_get_guc('powa.ignored_users', ''),
                        ',')));
        ELSIF (v_pgss[1] = 1 AND v_pgss[2] >= 8) THEN
            RETURN QUERY SELECT now(),
                pgss.userid, pgss.dbid, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_exec_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time,
                pgss.plans, pgss.total_plan_time,
                pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes, true::boolean
            FROM pg_stat_statements pgss
            JOIN pg_database d ON d.oid = pgss.dbid
            JOIN pg_roles r ON pgss.userid = r.oid
            WHERE pgss.query !~* '^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)'
            AND NOT (r.rolname = ANY (string_to_array(
                        powa_get_guc('powa.ignored_users', ''),
                        ',')));
        ELSE
            RETURN QUERY SELECT now(),
                pgss.userid, pgss.dbid, pgss.queryid, pgss.query,
                pgss.calls, pgss.total_time,
                pgss.rows, pgss.shared_blks_hit,
                pgss.shared_blks_read, pgss.shared_blks_dirtied,
                pgss.shared_blks_written, pgss.local_blks_hit,
                pgss.local_blks_read, pgss.local_blks_dirtied,
                pgss.local_blks_written, pgss.temp_blks_read,
                pgss.temp_blks_written, pgss.blk_read_time,pgss.blk_write_time,
                0::bigint, 0::double precision,
                0::bigint, 0::bigint, 0::numeric, true::boolean

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
            pgss.userid, pgss.dbid, pgss.queryid, pgss.query,
            pgss.calls, pgss.total_exec_time,
            pgss.rows, pgss.shared_blks_hit,
            pgss.shared_blks_read, pgss.shared_blks_dirtied,
            pgss.shared_blks_written, pgss.local_blks_hit,
            pgss.local_blks_read, pgss.local_blks_dirtied,
            pgss.local_blks_written, pgss.temp_blks_read,
            pgss.temp_blks_written, pgss.blk_read_time, pgss.blk_write_time,
            pgss.plans, pgss.total_plan_time,
            pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes, pgss.toplevel
        FROM powa_statements_src_tmp pgss WHERE srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_src */
