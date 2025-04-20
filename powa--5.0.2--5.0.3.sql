-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

CREATE OR REPLACE FUNCTION @extschema@.powa_replication_slots_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT slot_name text,
    OUT plugin text,
    OUT slot_type text,
    OUT datoid oid,
    OUT temporary boolean,
    OUT cur_txid xid,
    OUT current_lsn pg_lsn,
    OUT active bool,
    OUT active_pid int,
    OUT slot_xmin xid,
    OUT catalog_xmin xid,
    OUT restart_lsn pg_lsn,
    OUT confirmed_flush_lsn pg_lsn,
    OUT wal_status text,
    OUT safe_wal_size bigint,
    OUT two_phase boolean,
    OUT conflicting boolean
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_txid xid;
    v_current_lsn pg_lsn;
    v_server_version int;
BEGIN
    IF (_srvid = 0) THEN
        v_server_version := current_setting('server_version_num')::int;

        IF pg_catalog.pg_is_in_recovery() THEN
            v_txid = NULL;
        ELSE
            -- xid() was introduced in pg13
            IF v_server_version >= 130000 THEN
                v_txid = pg_catalog.xid(pg_catalog.pg_current_xact_id());
            ELSE
                v_txid = (txid_current()::bigint - (txid_current()::bigint >> 32 << 32))::text::xid;
            END IF;
        END IF;

        IF v_server_version < 100000 THEN
            IF pg_is_in_recovery() THEN
                v_current_lsn := pg_last_xlog_receive_location();
            ELSE
                v_current_lsn := pg_current_xlog_location();
            END IF;
        ELSE
            IF pg_is_in_recovery() THEN
                v_current_lsn := pg_last_wal_receive_lsn();
            ELSE
                v_current_lsn := pg_current_wal_lsn();
            END IF;
        END IF;

        -- We want to always return a row, even if no replication slots is
        -- found, so the UI can properly graph that no slot exists.

        -- conflicting added in pg16
        IF v_server_version >= 160000 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, s.temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn, s.wal_status,
                s.safe_wal_size, s.two_phase, s.conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- two_phase added in pg14
        ELSIF v_server_version >= 140000 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, s.temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn, s.wal_status,
                s.safe_wal_size, s.two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- wal_status and safe_wal_size added in pg13
        ELSIF v_server_version >= 130000 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, s.temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn, s.wal_status,
                s.safe_wal_size, false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- temporary added in pg10
        ELSIF v_server_version >= 100000 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, s.temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn,
                NULL::text as wal_status,
                NULL::bigint as safe_wal_size,
                false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- confirmed_flush_lsn added in pg9.6
        ELSIF v_server_version >= 90600 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, false AS temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, s.confirmed_flush_lsn,
                NULL::text as wal_status,
                NULL::bigint as safe_wal_size,
                false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        -- active_pid added in pg9.5
        ELSIF v_server_version >= 90500 THEN
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, false AS temporary,
                v_txid, v_current_lsn,
                s.active,
                s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, NULL::pg_lsn AS confirmed_flush_lsn,
                NULL::text as wal_status,
                NULL::bigint as safe_wal_size,
                false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        ELSE
            RETURN QUERY SELECT n.now,
                s.slot_name::text AS slot_name, s.plugin::text AS plugin,
                s.slot_type, s.datoid, false AS temporary,
                v_txid, v_current_lsn,
                s.active,
                NULL::int AS active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
                s.restart_lsn, NULL::pg_lsn AS confirmed_flush_lsn,
                NULL::text as wal_status,
                NULL::bigint as safe_wal_size,
                false AS two_phase, false AS conflicting
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_replication_slots AS s ON true;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.slot_name, s.plugin,
            s.slot_type, s.datoid, s.temporary,
            s.cur_txid, s.current_lsn,
            s.active,
            s.active_pid, s.xmin AS slot_xmin, s.catalog_xmin,
            s.restart_lsn, s.confirmed_flush_lsn, s.wal_status,
            s.safe_wal_size, s.two_phase, s.conflicting
        FROM @extschema@.powa_replication_slots_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_replication_slots_src */

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
    OUT backend_type text
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
                s.backend_xmin, s.query_id, s.backend_type
            FROM pg_catalog.pg_stat_activity AS s;
        -- leader_pid added in pg13+
        ELSIF v_server_version >= 130000 THEN
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, s.leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, NULL::bigint AS query_id, s.backend_type
            FROM pg_catalog.pg_stat_activity AS s;
        -- backend_type added in pg10+
        ELSIF v_server_version >= 100000 THEN
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, NULL::integer AS leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, NULL::bigint AS query_id, s.backend_type
            FROM pg_catalog.pg_stat_activity AS s;
        ELSE
            RETURN QUERY SELECT now(),
                txid,
                s.datid, s.pid, NULL::integer AS leader_pid, s.usesysid,
                s.application_name, s.client_addr, s.backend_start,
                s.xact_start,
                s.query_start, s.state_change, s.state, s.backend_xid,
                s.backend_xmin, NULL::bigint AS query_id,
                NULL::text AS backend_type
            FROM pg_catalog.pg_stat_activity AS s;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.cur_txid,
            s.datid, s.pid, s.leader_pid, s.usesysid,
            s.application_name, s.client_addr, s.backend_start,
            s.xact_start,
            s.query_start, s.state_change, s.state, s.backend_xid,
            s.backend_xmin, s.query_id, s.backend_type
        FROM @extschema@.powa_stat_activity_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_stat_activity_src */
