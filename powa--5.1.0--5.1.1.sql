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


DELETE FROM @extschema@.powa_db_module_src_queries
WHERE db_module = 'pg_stat_all_tables';

INSERT INTO @extschema@.powa_db_module_src_queries
    (db_module, min_version, added_manually, query_source) VALUES
    -- pg_stat_all_tables
    ('pg_stat_all_tables', 0, false,
     'SELECT relid, pg_table_size(relid) AS tbl_size,
        seq_scan, NULL AS last_seq_scan, seq_tup_read,
        idx_scan, NULL AS last_idx_scan, idx_tup_fetch,
        n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, 0 AS n_tup_newpage_upd,
        n_live_tup, n_dead_tup, n_mod_since_analyze, 0 AS n_ins_since_vacuum,
        last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
        vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
        heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit,
        toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit
     FROM pg_catalog.pg_stat_all_tables st
     JOIN pg_catalog.pg_statio_all_tables sit USING (relid)
     WHERE st.schemaname NOT LIKE ''pg_toast%'''),
    -- pg_stat_all_tables pg13+, n_ins_since_vacuum added
    ('pg_stat_all_tables', 130000, false,
     'SELECT relid, pg_table_size(relid) AS tbl_size,
        seq_scan, NULL AS last_seq_scan, seq_tup_read,
        idx_scan, NULL AS last_idx_scan, idx_tup_fetch,
        n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, 0 AS n_tup_newpage_upd,
        n_live_tup, n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum,
        last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
        vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
        heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit,
        toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit
     FROM pg_catalog.pg_stat_all_tables st
     JOIN pg_catalog.pg_statio_all_tables sit USING (relid)
     WHERE st.schemaname NOT LIKE ''pg_toast%'''),
    -- pg_stat_all_tables pg16+, last_seq_scan, last_idx_scan and
    -- n_tup_newpage_upd added
    ('pg_stat_all_tables', 160000, false,
     'SELECT relid, pg_table_size(relid) AS tbl_size,
        seq_scan,  last_seq_scan, seq_tup_read,
        idx_scan,  last_idx_scan, idx_tup_fetch,
        n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, n_tup_newpage_upd,
        n_live_tup, n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum,
        last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
        vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
        heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit,
        toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit
     FROM pg_catalog.pg_stat_all_tables st
     JOIN pg_catalog.pg_statio_all_tables sit USING (relid)
     WHERE st.schemaname NOT LIKE ''pg_toast%''');

