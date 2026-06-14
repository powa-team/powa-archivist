-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

DO $$
DECLARE
    v_typname text;
    v_suffix text;
    v_coltype text;
BEGIN
    FOREACH v_typname IN ARRAY ARRAY[
        'powa_stat_subscription_stats_history_record',
        'powa_stat_subscription_stats_history_db_record',
        'powa_stat_subscription_stats_history_diff',
        'powa_stat_subscription_stats_history_rate'
    ] LOOP
        IF v_typname = 'powa_stat_subscription_stats_history_rate' THEN
            v_suffix := '_per_sec';
            v_coltype = 'double precision';
        ELSE
            v_suffix := '';
            v_coltype = 'bigint';
        END IF;

        EXECUTE format('ALTER TYPE @extschema@.%I
            -- 19+
            RENAME ATTRIBUTE sync_error_count%2$s TO sync_table_error_count%2$s',
            v_typname, v_suffix);
        EXECUTE format('ALTER TYPE @extschema@.%I
            -- 19+
            ADD ATTRIBUTE sync_seq_error_count%2$s %3$s,
            -- pg18+
            ADD ATTRIBUTE confl_insert_exists%2$s %3$s,
            ADD ATTRIBUTE confl_update_origin_differs%2$s %3$s,
            ADD ATTRIBUTE confl_update_exists%2$s %3$s,
            -- pg19+
            ADD ATTRIBUTE confl_update_deleted%2$s %3$s,
            -- pg18+
            ADD ATTRIBUTE confl_update_missing%2$s %3$s,
            ADD ATTRIBUTE confl_delete_origin_differs%2$s %3$s,
            ADD ATTRIBUTE confl_delete_missing%2$s %3$s,
            ADD ATTRIBUTE confl_multiple_unique_conflicts%2$s %3$s',
            v_typname, v_suffix, v_coltype);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

ALTER TABLE @extschema@.powa_stat_subscription_stats_src_tmp
    -- 19+
    RENAME COLUMN sync_error_count TO sync_table_error_count;
ALTER TABLE @extschema@.powa_stat_subscription_stats_src_tmp
    -- pg19+
    ADD COLUMN sync_seq_error_count bigint NOT NULL,
    -- pg18+
    ADD COLUMN confl_insert_exists bigint NOT NULL,
    ADD COLUMN confl_update_origin_differs bigint NOT NULL,
    ADD COLUMN confl_update_exists bigint NOT NULL,
    -- pg19+
    ADD COLUMN confl_update_deleted bigint NOT NULL,
    -- pg18+
    ADD COLUMN confl_update_missing bigint NOT NULL,
    ADD COLUMN confl_delete_origin_differs bigint NOT NULL,
    ADD COLUMN confl_delete_missing bigint NOT NULL,
    ADD COLUMN confl_multiple_unique_conflicts bigint NOT NULL;

DO $$
BEGIN
    IF current_setting('server_version_num')::bigint >= 180000 THEN
        ALTER TABLE @extschema@.powa_stat_subscription_stats_src_tmp
            RENAME CONSTRAINT powa_stat_subscription_stats_src_tmp_sync_error_count_not_null
                TO powa_stat_subscription_stats_sr_sync_table_error_count_not_null;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION @extschema@.powa_stat_subscription_stats_src(integer);
CREATE FUNCTION @extschema@.powa_stat_subscription_stats_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT subid oid,
    OUT apply_error_count bigint,
    OUT sync_table_error_count bigint,
    OUT stats_reset timestamp with time zone,
    OUT sync_seq_error_count bigint,
    OUT confl_insert_exists bigint,
    OUT confl_update_origin_differs bigint,
    OUT confl_update_exists bigint,
    OUT confl_update_deleted bigint,
    OUT confl_update_missing bigint,
    OUT confl_delete_origin_differs bigint,
    OUT confl_delete_missing bigint,
    OUT confl_multiple_unique_conflicts bigint
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_pg_version_num int;
BEGIN
    IF (_srvid = 0) THEN
        v_pg_version_num := current_setting('server_version_num')::int;

        -- pg19+:
        -- sync_error_count renamed to sync_table_error_count,
        -- sync_seq_error_count added
        -- confl_update_deleted added
        IF v_pg_version_num >= 190000 THEN
            RETURN QUERY SELECT now(),
            s.subid,
            s.apply_error_count, s.sync_table_error_count,
            s.stats_reset,
            s.sync_seq_error_count,
            s.confl_insert_exists,
            s.confl_update_origin_differs,
            s.confl_update_exists,
            s.confl_update_deleted,
            s.confl_update_missing,
            s.confl_delete_origin_differs,
            s.confl_delete_missing,
            s.confl_multiple_unique_conflicts
            FROM pg_catalog.pg_stat_subscription_stats AS s;
        -- pg18+, confl_* columns added
        ELSIF v_pg_version_num >= 180000 THEN
            RETURN QUERY SELECT now(),
            s.subid,
            s.apply_error_count, s.sync_error_count AS sync_table_error_count,
            s.stats_reset,
            0::bigint AS sync_seq_error_count,
            s.confl_insert_exists,
            s.confl_update_origin_differs,
            s.confl_update_exists,
            0::bigint AS confl_update_deleted,
            s.confl_update_missing,
            s.confl_delete_origin_differs,
            s.confl_delete_missing,
            s.confl_multiple_unique_conflicts
            FROM pg_catalog.pg_stat_subscription_stats AS s;
        -- pg15+, the view is introduced
        ELSIF v_pg_version_num >= 150000 THEN
            RETURN QUERY SELECT now(),
            s.subid,
            s.apply_error_count, s.sync_error_count AS sync_table_error_count,
            s.stats_reset,
            0::bigint AS sync_seq_error_count,
            0::bigint AS confl_insert_exists,
            0::bigint AS confl_update_origin_differs,
            0::bigint AS confl_update_exists,
            0::bigint AS confl_update_deleted,
            0::bigint AS confl_update_missing,
            0::bigint AS confl_delete_origin_differs,
            0::bigint AS confl_delete_missing,
            0::bigint AS confl_multiple_unique_conflicts
            FROM pg_catalog.pg_stat_subscription_stats AS s;
        ELSE -- return an empty dataset for pg9.6- servers
            RETURN QUERY SELECT now(),
            0::oid AS subid,
            0::bigint AS apply_error_count, 0::bigint AS sync_error_count,
            NULL::timestamp with time zone AS stats_reset,
            0::bigint AS sync_seq_error_count,
            0::bigint AS confl_insert_exists,
            0::bigint AS confl_update_origin_differs,
            0::bigint AS confl_update_exists,
            0::bigint AS confl_update_deleted,
            0::bigint AS confl_update_missing,
            0::bigint AS confl_delete_origin_differs,
            0::bigint AS confl_delete_missing,
            0::bigint AS confl_multiple_unique_conflicts
            WHERE false;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
            s.subid,
            s.apply_error_count, s.sync_error_count,
            s.stats_reset,
            s.sync_seq_error_count,
            s.confl_insert_exists,
            s.confl_update_origin_differs,
            s.confl_update_exists,
            s.confl_update_deleted,
            s.confl_update_missing,
            s.confl_delete_origin_differs,
            s.confl_delete_missing,
            s.confl_multiple_unique_conflicts
        FROM @extschema@.powa_stat_subscription_stats_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_subscription_stats_src */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_stats_snapshot(_srvid integer)
 RETURNS void
AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_subscription_stats_snapshot', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert background writer statistics
    WITH rel AS (
        SELECT *
        FROM @extschema@.powa_stat_subscription_stats_src(_srvid)
    )
    INSERT INTO @extschema@.powa_stat_subscription_stats_history_current
        SELECT _srvid, subid,
        ROW(ts, apply_error_count, sync_table_error_count, stats_reset,
            sync_seq_error_count, confl_insert_exists, confl_update_origin_differs,
            confl_update_exists, confl_update_deleted, confl_update_missing,
            confl_delete_origin_differs, confl_delete_missing, confl_multiple_unique_conflicts)::@extschema@.powa_stat_subscription_stats_history_record AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.powa_stat_subscription_stats_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_subscription_stats_snapshot */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_stats_aggregate(_srvid integer)
 RETURNS void
AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_subscription_stats_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate powa_stat_subscription_stats history table
    INSERT INTO @extschema@.powa_stat_subscription_stats_history
        SELECT srvid, subid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts),
                    min((record).apply_error_count),
                    min((record).sync_table_error_count),
                    min((record).stats_reset),
                    min((record).sync_seq_error_count),
                    min((record).confl_insert_exists),
                    min((record).confl_update_origin_differs),
                    min((record).confl_update_exists),
                    min((record).confl_update_deleted),
                    min((record).confl_update_missing),
                    min((record).confl_delete_origin_differs),
                    min((record).confl_delete_missing),
                    min((record).confl_multiple_unique_conflicts))::@extschema@.powa_stat_subscription_stats_history_record,
            ROW(max((record).ts),
                    max((record).apply_error_count),
                    max((record).sync_table_error_count),
                    max((record).stats_reset),
                    max((record).sync_seq_error_count),
                    max((record).confl_insert_exists),
                    max((record).confl_update_origin_differs),
                    max((record).confl_update_exists),
                    max((record).confl_update_deleted),
                    max((record).confl_update_missing),
                    max((record).confl_delete_origin_differs),
                    max((record).confl_delete_missing),
                    max((record).confl_multiple_unique_conflicts))::@extschema@.powa_stat_subscription_stats_history_record
        FROM @extschema@.powa_stat_subscription_stats_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, subid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_stat_subscription_stats_history_current WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_subscription_stats_aggregate */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_stats_history_db_div(
    a powa_stat_subscription_stats_history_db_record,
    b powa_stat_subscription_stats_history_db_record)
 RETURNS powa_stat_subscription_stats_history_rate
 IMMUTABLE STRICT
AS $PROC$
DECLARE
    res @extschema@.powa_stat_subscription_stats_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.apply_error_count_per_sec = (a.apply_error_count - b.apply_error_count)::double precision / sec;
    res.sync_table_error_count_per_sec = (a.sync_table_error_count - b.sync_table_error_count)::double precision / sec;
    res.sync_seq_error_count_per_sec = (a.sync_seq_error_count - b.sync_seq_error_count)::double precision / sec;
    res.confl_insert_exists_per_sec = (a.confl_insert_exists - b.confl_insert_exists)::double precision / sec;
    res.confl_update_origin_differs_per_sec = (a.confl_update_origin_differs - b.confl_update_origin_differs)::double precision / sec;
    res.confl_update_exists_per_sec = (a.confl_update_exists - b.confl_update_exists)::double precision / sec;
    res.confl_update_deleted_per_sec = (a.confl_update_deleted - b.confl_update_deleted)::double precision / sec;
    res.confl_update_missing_per_sec = (a.confl_update_missing - b.confl_update_missing)::double precision / sec;
    res.confl_delete_origin_differs_per_sec = (a.confl_delete_origin_differs - b.confl_delete_origin_differs)::double precision / sec;
    res.confl_delete_missing_per_sec = (a.confl_delete_missing - b.confl_delete_missing)::double precision / sec;
    res.confl_multiple_unique_conflicts_per_sec = (a.confl_multiple_unique_conflicts - b.confl_multiple_unique_conflicts)::double precision / sec;

    return res;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_subscription_stats_history_db_div */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_stats_history_db_mi(
    a powa_stat_subscription_stats_history_db_record,
    b powa_stat_subscription_stats_history_db_record)
 RETURNS powa_stat_subscription_stats_history_diff
 IMMUTABLE STRICT
AS $PROC$
DECLARE
    res @extschema@.powa_stat_subscription_stats_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.apply_error_count = a.apply_error_count - b.apply_error_count;
    res.sync_table_error_count = a.sync_table_error_count - b.sync_table_error_count;
    res.sync_seq_error_count = a.sync_seq_error_count - b.sync_seq_error_count;
    res.confl_insert_exists = a.confl_insert_exists - b.confl_insert_exists;
    res.confl_update_origin_differs = a.confl_update_origin_differs - b.confl_update_origin_differs;
    res.confl_update_exists = a.confl_update_exists - b.confl_update_exists;
    res.confl_update_deleted = a.confl_update_deleted - b.confl_update_deleted;
    res.confl_update_missing = a.confl_update_missing - b.confl_update_missing;
    res.confl_delete_origin_differs = a.confl_delete_origin_differs - b.confl_delete_origin_differs;
    res.confl_delete_missing = a.confl_delete_missing - b.confl_delete_missing;
    res.confl_multiple_unique_conflicts = a.confl_multiple_unique_conflicts - b.confl_multiple_unique_conflicts;

    return res;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_subscription_stats_history_db_mi */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_stats_history_div(
    a powa_stat_subscription_stats_history_record,
    b powa_stat_subscription_stats_history_record)
  RETURNS powa_stat_subscription_stats_history_rate
  IMMUTABLE STRICT
AS $PROC$
DECLARE
    res @extschema@.powa_stat_subscription_stats_history_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.apply_error_count_per_sec = (a.apply_error_count - b.apply_error_count)::double precision / sec;
    res.sync_table_error_count_per_sec = (a.sync_table_error_count - b.sync_table_error_count)::double precision / sec;
    res.sync_seq_error_count_per_sec = (a.sync_seq_error_count - b.sync_seq_error_count)::double precision / sec;
    res.confl_insert_exists_per_sec = (a.confl_insert_exists - b.confl_insert_exists)::double precision / sec;
    res.confl_update_origin_differs_per_sec = (a.confl_update_origin_differs - b.confl_update_origin_differs)::double precision / sec;
    res.confl_update_exists_per_sec = (a.confl_update_exists - b.confl_update_exists)::double precision / sec;
    res.confl_update_deleted_per_sec = (a.confl_update_deleted - b.confl_update_deleted)::double precision / sec;
    res.confl_update_missing_per_sec = (a.confl_update_missing - b.confl_update_missing)::double precision / sec;
    res.confl_delete_origin_differs_per_sec = (a.confl_delete_origin_differs - b.confl_delete_origin_differs)::double precision / sec;
    res.confl_delete_missing_per_sec = (a.confl_delete_missing - b.confl_delete_missing)::double precision / sec;
    res.confl_multiple_unique_conflicts_per_sec = (a.confl_multiple_unique_conflicts - b.confl_multiple_unique_conflicts)::double precision / sec;

    return res;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_subscription_stats_history_div */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_stats_history_mi(
    a powa_stat_subscription_stats_history_record,
    b powa_stat_subscription_stats_history_record)
 RETURNS powa_stat_subscription_stats_history_diff
 IMMUTABLE STRICT
AS $PROC$
DECLARE
    res @extschema@.powa_stat_subscription_stats_history_diff;
BEGIN
    res.intvl = a.ts - b.ts;
    res.apply_error_count = a.apply_error_count - b.apply_error_count;
    res.sync_table_error_count = a.sync_table_error_count - b.sync_table_error_count;
    res.sync_seq_error_count = a.sync_seq_error_count - b.sync_seq_error_count;
    res.confl_insert_exists = a.confl_insert_exists - b.confl_insert_exists;
    res.confl_update_origin_differs = a.confl_update_origin_differs - b.confl_update_origin_differs;
    res.confl_update_exists = a.confl_update_exists - b.confl_update_exists;
    res.confl_update_deleted = a.confl_update_deleted - b.confl_update_deleted;
    res.confl_update_missing = a.confl_update_missing - b.confl_update_missing;
    res.confl_delete_origin_differs = a.confl_delete_origin_differs - b.confl_delete_origin_differs;
    res.confl_delete_missing = a.confl_delete_missing - b.confl_delete_missing;
    res.confl_multiple_unique_conflicts = a.confl_multiple_unique_conflicts - b.confl_multiple_unique_conflicts;

    return res;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_subscription_stats_history_mi */
