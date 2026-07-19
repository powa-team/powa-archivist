-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

-- in pg19b2 pg_stat_lock.wait_time is now double precision.
DROP FUNCTION @extschema@.powa_stat_lock_src(integer);
CREATE FUNCTION @extschema@.powa_stat_lock_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT locktype text,
    OUT waits bigint,
    OUT wait_time double precision,
    OUT fastpath_exceeded bigint,
    OUT stats_reset timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_current_lsn pg_lsn;
    v_pg_version_num int;
BEGIN
    IF (_srvid = 0) THEN
        v_pg_version_num := current_setting('server_version_num')::int;

        -- pg19+, view is added
        IF v_pg_version_num >= 190000 THEN
            RETURN QUERY SELECT now,
                s.locktype,
                s.waits, s.wait_time,
                s.fastpath_exceeded,
                s.stats_reset
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_stat_lock AS s ON true;
        ELSE
            RETURN QUERY SELECT now(),
                NULL::text AS locktype,
                0::bigint AS waits,
                0::bigint AS wait_time,
                0::bigint AS fastpath_exceeded,
                NULL::timestamp with time zone AS stats_reset
            WHERE false;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
                s.locktype,
                s.waits,
                s.wait_time,
                s.fastpath_exceeded,
                s.stats_reset
        FROM @extschema@.powa_stat_lock_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_lock_src */

ALTER TABLE @extschema@.powa_stat_lock_src_tmp
    ALTER COLUMN wait_time SET DATA TYPE double precision;

-- multiple tables uses powa_stat_lock_history_record so we cannot simply
-- update the type.  It's however introduced in pg19beta1 and the breaking
-- change happens in pg19beta2 so we don't care about the data.
ALTER EXTENSION powa DROP TABLE @extschema@.powa_stat_lock_history;
DROP TABLE @extschema@.powa_stat_lock_history;
ALTER EXTENSION powa DROP TABLE @extschema@.powa_stat_lock_history_current;
DROP TABLE @extschema@.powa_stat_lock_history_current;

ALTER TYPE @extschema@.powa_stat_lock_history_record
    ALTER ATTRIBUTE wait_time SET DATA TYPE double precision;

CREATE TABLE @extschema@.powa_stat_lock_history (
    srvid integer NOT NULL,
    locktype text NOT NULL,
    coalesce_range tstzrange NOT NULL,
    records @extschema@.powa_stat_lock_history_record[] NOT NULL,
    mins_in_range @extschema@.powa_stat_lock_history_record NOT NULL,
    maxs_in_range @extschema@.powa_stat_lock_history_record NOT NULL,
    FOREIGN KEY (srvid)
        REFERENCES @extschema@.powa_servers(id)
        MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.powa_stat_lock_history
    ALTER COLUMN mins_in_range SET STORAGE MAIN,
    ALTER COLUMN maxs_in_range SET STORAGE MAIN;
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_stat_lock_history','');
CREATE INDEX powa_stat_lock_history_ts
    ON @extschema@.powa_stat_lock_history
    USING gist (srvid, coalesce_range);

CREATE TABLE @extschema@.powa_stat_lock_history_current (
    srvid integer NOT NULL,
    locktype text NOT NULL,
    record @extschema@.powa_stat_lock_history_record NOT NULL,
    FOREIGN KEY (srvid)
        REFERENCES @extschema@.powa_servers(id)
        MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_stat_lock_history_current','');
CREATE INDEX ON @extschema@.powa_stat_lock_history_current(srvid);

ALTER TYPE @extschema@.powa_stat_lock_history_db_record
    ALTER ATTRIBUTE wait_time SET DATA TYPE double precision;
ALTER TYPE @extschema@.powa_stat_lock_history_diff
    ALTER ATTRIBUTE wait_time SET DATA TYPE double precision;

-- Fix the toast tuple targets
SELECT @extschema@.powa_fix_toast_tuple_target();
