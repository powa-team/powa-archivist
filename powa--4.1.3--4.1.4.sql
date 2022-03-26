-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit
SET LOCAL statement_timeout = 0;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL standard_conforming_strings = on;
SET LOCAL client_min_messages = warning;
SET LOCAL escape_string_warning = off;
SET LOCAL search_path = public, pg_catalog;


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

        IF (v_pgss[1] = 1 AND v_pgss[2] < 8) THEN
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
                0::bigint, 0::bigint, 0::numeric

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
            pgss.wal_records, pgss.wal_fpi, pgss.wal_bytes
        FROM powa_statements_src_tmp pgss WHERE srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_src */

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
            (regexp_split_to_array(extversion, E'\\.')::int[])[1] >= 2 AND
            (regexp_split_to_array(extversion, E'\\.')::int[])[2] >= 2
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
