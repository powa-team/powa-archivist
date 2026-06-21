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

-------------------------------
-- data sources generic support
-------------------------------
CREATE FUNCTION @extschema@.powa_generic_module_setup(_pg_module text,
                                                      _counter_cols text[],
                                                      _nullable text[] DEFAULT '{}',
                                                      _need_operators boolean default true,
                                                      _key_cols text[] DEFAULT '{}',
                                                      _key_nullable boolean DEFAULT false,
                                                      _min_version integer DEFAULT 0)
RETURNS void AS
$$
DECLARE
    i integer;
    v_nb integer;
    v_module text;
    v_sql text;
    v_colname text;
    v_coltype text;
    v_kind text;
    v_null text;
    v_has_no_minmax_col bool;
    v_suffix text;
    v_accum text;
BEGIN
    IF quote_ident(_pg_module) != _pg_module THEN
        RAISE EXCEPTION '% require quoting, which is not supported',
                         _pg_module;
    END IF;

    IF _pg_module !~ '^pg_' THEN
        RAISE EXCEPTION '% is not a postgres module', _pg_module;
    END IF;

    v_module := regexp_replace(_pg_module, '^pg', 'powa');

    -- declare the module and its configuration
    INSERT INTO @extschema@.powa_modules VALUES (_pg_module, _min_version);
    INSERT INTO @extschema@.powa_module_config VALUES (0, _pg_module);
    INSERT INTO @extschema@.powa_module_functions VALUES
        (_pg_module, 'snapshot',  v_module || '_snapshot',  v_module || '_src', false),
        (_pg_module, 'aggregate', v_module || '_aggregate', NULL,               false),
        (_pg_module, 'purge',     v_module || '_purge',     NULL,               false),
        (_pg_module, 'reset',     v_module || '_reset',     NULL,               false);

    -- create the underlying record datatype(s) and operators if needed
    EXECUTE @extschema@.powa_generic_datatype_setup(v_module, _counter_cols,
                                                    _need_operators => _need_operators);
    -- create the *_src_tmp unlogged table
    v_sql := format('CREATE UNLOGGED TABLE @extschema@.%I (
    srvid integer NOT NULL,
    ts timestamp with time zone NOT NULL',
                    v_module || '_src_tmp');

    -- iterate over the key columns first
    IF _key_nullable THEN
        v_null := '';
    ELSE
        v_null := ' NOT NULL';
    END IF;
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_colname := _key_cols[i][1];
        v_coltype := _key_cols[i][2];

        -- as this is the first iteration over the columns, make sure that none
        -- of them require quoting
        IF quote_ident(v_colname) != v_colname THEN
            RAISE EXCEPTION '% require quoting, which is not supported',
                            v_colname;
        END IF;

        -- key columns should only use a few of native system types
        IF v_coltype NOT IN ('boolean', 'integer', 'name', 'oid', 'text')
        THEN
            RAISE EXCEPTION 'invalid data type % for key col %.%',
                            v_coltype, v_module, v_colname;
        END IF;

        IF v_colname = ANY (_nullable) THEN
            RAISE EXCEPTION 'invalid nullable info for key col %.%',
                            v_module, v_colname;
        END IF;

        v_sql := v_sql || ',' || chr(10) || format('    %I %s%s',
                                                   v_colname, v_coltype, v_null);
    END LOOP;

    -- then iterate over the counter columns
    v_has_no_minmax_col := false;
    FOR i IN 1..array_upper(_counter_cols, 1) LOOP
        v_colname := _counter_cols[i][1];
        v_coltype := _counter_cols[i][2];

        IF v_coltype IN ('xid', 'boolean') THEN
            v_has_no_minmax_col := true;
        END IF;

        -- as this is the first iteration over the columns, make sure that none
        -- of them require quoting
        IF quote_ident(v_colname) != v_colname THEN
            RAISE EXCEPTION '% require quoting, which is not supported',
                            v_colname;
        END IF;

        -- datasources should only use a few of native system types
        IF v_coltype NOT IN ('timestamp with time zone', 'oid',  'bigint',
                             'integer', 'numeric', 'double precision',
                             'text', 'inet', 'xid', 'pg_lsn', 'interval',
                             'boolean')
        THEN
            RAISE EXCEPTION 'invalid data type % for col %.%',
                            v_coltype, v_module, v_colname;
        END IF;

        IF v_colname = ANY (_nullable) THEN
            _nullable := array_remove(_nullable, v_colname);
            v_null := '';
        ELSE
            v_null := ' NOT NULL';
        END IF;
        v_sql := v_sql || ',' || chr(10) || format('    %I %s%s',
                                                   v_colname, v_coltype,
                                                   v_null);
    END LOOP;

    IF array_upper(_nullable, 1) IS NOT NULL THEN
        RAISE EXCEPTION 'Columns % declared as nullable, but not found in the '
                        'list of columns', _nullable;
    END IF;

    v_sql := v_sql || ')';
    EXECUTE v_sql;

    -- create the *_history table and its index
    v_suffix := v_module || '_history_record';
    IF v_has_no_minmax_col THEN
        v_suffix := v_suffix || '_minmax';
    END IF;
    v_sql := format('CREATE TABLE @extschema@.%1$I (
    srvid integer NOT NULL,', v_module || '_history');

    IF _key_nullable THEN
        v_null := '';
    ELSE
        v_null := ' NOT NULL';
    END IF;
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_sql := v_sql || format('
    %I %s%s,', _key_cols[i][1], _key_cols[i][2], v_null);
    END LOOP;

    v_sql := v_sql || format('
    coalesce_range tstzrange NOT NULL,
    records @extschema@.%2$I[] NOT NULL,
    mins_in_range @extschema@.%3$I NOT NULL,
    maxs_in_range @extschema@.%3$I NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
ALTER TABLE @extschema@.%1$I ALTER COLUMN mins_in_range SET STORAGE MAIN;
ALTER TABLE @extschema@.%1$I ALTER COLUMN maxs_in_range SET STORAGE MAIN;
CREATE INDEX %4$I ON @extschema@.%1$I USING gist(srvid, coalesce_range);',
                    v_module || '_history', v_module || '_history_record',
                    v_suffix, v_module || '_history_ts');
    EXECUTE v_sql;

    -- and the *_history_current table and index
    v_accum = 'srvid integer NOT NULL,';
    IF _key_nullable THEN
        v_null := '';
    ELSE
        v_null := ' NOT NULL';
    END IF;
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_colname := _key_cols[i][1];
        v_coltype := _key_cols[i][2];

        v_accum := v_accum || format('
    %I %s%s,', v_colname, v_coltype, v_null);
    END LOOP;
    v_sql := format('CREATE TABLE @extschema@.%1$I (
    %3$s
    record @extschema@.%2$I NOT NULL,
    FOREIGN KEY (srvid) REFERENCES @extschema@.powa_servers(id)
      MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX ON @extschema@.%1$I (srvid);',
    v_module || '_history_current', v_module || '_history_record', v_accum);
    EXECUTE v_sql;

    -- make sure the *_history and  *_history_current tables are dumped
    PERFORM pg_catalog.pg_extension_config_dump('@extschema@.' || v_module || '_history','');
    PERFORM pg_catalog.pg_extension_config_dump('@extschema@.' || v_module || '_history_current','');

    -- create the *_snapshot function
    v_accum := '_srvid';
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_colname := _key_cols[i][1];

        v_accum := v_accum || format(', %I', v_colname);
    END LOOP;
    v_sql := format('CREATE FUNCTION @extschema@.%1$I (_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := format(''@extschema@.%%I(%%s)'',
                                 %1$L, _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format(''running %%s'', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- Insert background writer statistics
    WITH rel AS (
        SELECT *
        FROM @extschema@.%2$I(_srvid)
    )
    INSERT INTO @extschema@.%3$I
        SELECT %4$s,
        ROW(ts',
                  v_module || '_snapshot',
                  v_module || '_src',
                  v_module || '_history_current',
                  v_accum);

    FOR i IN 1..array_upper(_counter_cols, 1) LOOP
        v_colname := _counter_cols[i][1];

        IF i > 1 AND (i - 1) % 3 = 0 THEN
            v_sql := v_sql || ',' || chr(10) || '            ';
        ELSE
            v_sql := v_sql || ', ';
        END IF;
        v_sql := v_sql || v_colname;
    END LOOP;

    v_sql := v_sql || format(')::@extschema@.%I AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format(''%%s - rowcount: %%s'',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM @extschema@.%I WHERE srvid = _srvid;
    END IF;

    result := true;
END;
$PROC$ language plpgsql;',
                    v_module || '_history_record', v_module || '_src_tmp');
    EXECUTE v_sql;

    -- create the *_aggregate function
    v_accum := 'srvid';
    FOR i IN 1..coalesce(array_upper(_key_cols, 1), 0) LOOP
        v_colname := _key_cols[i][1];
        v_coltype := _key_cols[i][2];

        v_sql := v_sql || format(',
            (record).%I', v_colname);

        v_accum := v_accum || ', ' || v_colname;
    END LOOP;
    v_sql := format('CREATE FUNCTION @extschema@.%1$I(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format(''@extschema@.%%I(%%s)'',
                                 %1$L, _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log(format(''running %%s'', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate %3$s history table
    INSERT INTO @extschema@.%2$I
        SELECT %4$s,
            tstzrange(min((record).ts), max((record).ts),''[]''),
            array_agg(record)',
                    v_module || '_aggregate', v_module || '_history',
                    v_module, v_accum);

    FOREACH v_kind IN ARRAY ARRAY['min', 'max'] LOOP
        v_sql := v_sql || format(',
            ROW(%s((record).ts)', v_kind);

        FOR i IN 1..array_upper(_counter_cols, 1) LOOP
            v_colname := _counter_cols[i][1];
            v_coltype := _counter_cols[i][2];

            IF v_coltype NOT IN ('xid', 'boolean') THEN
                v_sql := v_sql || format(',
                    %s((record).%I)', v_kind, v_colname);
            END IF;
        END LOOP;

        v_sql := v_sql || format(')::@extschema@.%I',
                                 v_suffix);
    END LOOP;

    v_sql := v_sql || format('
        FROM @extschema@.%1$I
        WHERE srvid = _srvid
        GROUP BY %2$s;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format(''%%s - rowcount: %%s'',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.%1$I WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql',
                    v_module || '_history_current', v_accum);
    EXECUTE v_sql;

    -- create the *_purge function
    v_sql := format('CREATE FUNCTION @extschema@.%1$I(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format(''@extschema@.%%I(%%s)'',
                                 %1$L, _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format(''running %%s'', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,%3$L,''module''::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.%2$I
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format(''%%s - rowcount: %%s'',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql
        ',
                    v_module || '_purge', v_module || '_history', _pg_module);
    EXECUTE v_sql;

    -- create the *_reset function
    v_sql := format('CREATE FUNCTION @extschema@.%1$I(_srvid integer)
RETURNS boolean AS $function$
BEGIN
    PERFORM @extschema@.powa_log(''Resetting %2$I('' || _srvid || '')'');
    DELETE FROM @extschema@.%2$I WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log(''Resetting %3$I('' || _srvid || '')'');
    DELETE FROM @extschema@.%3$I WHERE srvid = _srvid;

    PERFORM @extschema@.powa_log(''Resetting %4$I('' || _srvid || '')'');
    DELETE FROM @extschema@.%4$I WHERE srvid = _srvid;

    RETURN true;
END;
$function$ LANGUAGE plpgsql',
                    v_module || '_reset', v_module || '_history',
                    v_module || '_history_current', v_module || '_src_tmp');
    EXECUTE v_sql;
END;
$$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_generic_module_setup */


CREATE FUNCTION @extschema@.powa_generic_datatype_setup(
    _datasource text,
    _cols text[],
    _extra jsonb DEFAULT '{}',
    _need_operators boolean default true
)
RETURNS void AS
$$
DECLARE
    i integer;
    v_prefix text;
    v_sql text;
    v_record_name text;
    v_colname text;
    v_coltype text;
    v_extra text;
    v_kind text;
    c_no_agg text[];
    v_has_no_agg_col bool;
    c_no_minmax text[];
    v_has_no_minmax_col bool;
BEGIN
    IF quote_ident(_datasource) != _datasource THEN
        RAISE EXCEPTION '% require quoting, which is not supported',
                        _datasource;
    END IF;

    -- we don't put any fields for any of those datatypes in the *_history_db
    -- records, as aggregating them per-database or computing a rate wouldn't
    -- make sense
    c_no_agg := ARRAY['timestamp with time zone', 'timestamptz'];
    -- Similarly, don't put any field with datatypes that don't support min/max
    -- in the mins_in_range / maxs_in_range records
    c_no_minmax := ARRAY['xid', 'boolean'];

    -- we loop over the whole process in case we find some columns with
    -- some specific datatypes.  For those we need to create a *_history_db
    -- version datatype without such columns, as we can't do a per-db
    -- aggregation of such data.
    -- Similarly, we need to create a specific datatype for the min/max
    -- aggregated records for datatypes that don't support min/max.
    v_has_no_agg_col := false;
    v_has_no_minmax_col := false;
    FOREACH v_prefix IN ARRAY ARRAY['_history', '_history_db'] LOOP
        -- we only need _db version of the infrastructure if we skipped some
        -- columns
        EXIT WHEN v_prefix = '_history_db' AND NOT v_has_no_agg_col;

        v_record_name := _datasource || v_prefix || '_record';

        -- first, create a main record type
        -- as this is the first iteration over the columns, make sure that none
        -- of them require quoting
        v_sql := format('CREATE TYPE @extschema@.%I AS (
ts timestamp with time zone',
                        v_record_name);
        FOR i IN 1..array_upper(_cols, 1) LOOP
            v_colname := _cols[i][1];
            v_coltype := _cols[i][2];

            -- the *_history_db type version is the same as the *_history
            -- without the fields of datatype we don't aggregate
            CONTINUE WHEN v_prefix = '_history_db'
                     AND v_coltype = ANY (c_no_agg);

            IF quote_ident(v_colname) != v_colname THEN
                RAISE EXCEPTION '% require quoting, which is not supported',
                                v_colname;
            END IF;

            -- datasources should only use a few of native system types
            IF v_coltype NOT IN ('timestamp with time zone', 'oid',  'bigint',
                                 'integer', 'numeric', 'double precision',
                                 'text', 'inet', 'xid', 'pg_lsn', 'interval',
                                 'boolean')
            THEN
                RAISE EXCEPTION 'invalid data type % for col %.%',
                                v_coltype, _datasource, v_colname;
            END IF;

            IF v_coltype = ANY (c_no_minmax) THEN
                v_has_no_minmax_col := true;
            END IF;

            v_sql := v_sql || ',' || chr(10) || v_colname || ' ' || v_coltype;
        END LOOP;
        v_sql := v_sql || ')';
        EXECUTE v_sql;

        -- Create a specific min/max record if needed
        IF v_prefix = '_history' AND v_has_no_minmax_col THEN
            v_sql := format('CREATE TYPE @extschema@.%I AS (
ts timestamp with time zone',
                            v_record_name || '_minmax');
            FOR i IN 1..array_upper(_cols, 1) LOOP
                v_colname := _cols[i][1];
                v_coltype := _cols[i][2];

                CONTINUE WHEN v_coltype = ANY (c_no_minmax);

                v_sql := v_sql || ',' || chr(10) || v_colname || ' ' || v_coltype;
            END LOOP;
            v_sql := v_sql || ')';
            EXECUTE v_sql;
        END IF;

        CONTINUE WHEN NOT _need_operators;

        -- add a *history_rate and a *history_diff type, and remember if we saw
        -- (and skipped) any timestamptz column
        FOREACH v_kind IN ARRAY ARRAY['diff', 'rate'] LOOP
            CONTINUE WHEN v_prefix = '_history_db';

            IF v_kind = 'diff' THEN
                v_extra := 'intvl interval';
            ELSE
                v_extra := 'sec integer';
            END IF;
            v_sql := format('CREATE TYPE @extschema@.%I AS ('
                            || chr(10)
                            || '%s',
                            _datasource || v_prefix || '_' || v_kind, v_extra);
            FOR i IN 1..array_upper(_cols, 1) LOOP
                v_colname := _cols[i][1];
                v_coltype := _cols[i][2];

                IF v_coltype = ANY (c_no_agg)
                THEN
                    v_has_no_agg_col = true;
                    CONTINUE;
                END IF;

                v_colname = coalesce(_extra->v_kind->'colname'->>v_colname,
                                     v_colname);

                IF v_kind = 'rate' THEN
                    v_colname := v_colname ||
                        coalesce(_extra->v_kind->'suffix'->>v_colname,
                                 '_per_sec');
                    IF v_coltype != 'numeric' THEN
                        v_coltype := 'double precision';
                    END IF;
                END IF;

                v_sql := v_sql || ', ' || chr(10)
                    || quote_ident(v_colname) || ' ' || v_coltype;
            END LOOP;
            v_sql := v_sql || ')';
            EXECUTE v_sql;
        END LOOP;

        -- add a *_mi() function
        v_sql := format('CREATE FUNCTION @extschema@.%1$I(
a @extschema@.%2$I,
b @extschema@.%2$I)
RETURNS @extschema@.%3$I AS
$_$
DECLARE
    res @extschema@.%3$I;
BEGIN
    res.intvl = a.ts - b.ts;',
                        _datasource || v_prefix || '_mi',
                        v_record_name,
                        -- the datatype is the same for the _db version
                        _datasource || '_history_diff');
        FOR i in 1..array_upper(_cols, 1) LOOP
            CONTINUE WHEN _cols[i][2] = ANY (c_no_agg);
            v_colname := _cols[i][1];
            v_colname = coalesce(_extra->'mi'->'colname'->>v_colname,
                                 v_colname);

            v_sql := v_sql || chr(10)
                || format('    res.%1$I = a.%1$I - b.%1$I;', v_colname);
        END LOOP;
        v_sql := v_sql || chr(10) || chr(10) || '    return res;'
            || chr(10) || 'END;' || chr(10) || '$_$'
            || chr(10) || 'LANGUAGE plpgsql IMMUTABLE STRICT';
        EXECUTE v_sql;

        -- add a "-" operator
        v_sql := format('CREATE OPERATOR @extschema@.- ('
                        'PROCEDURE = @extschema@.%1$I,'
                        'LEFTARG = @extschema@.%2$I,'
                        'RIGHTARG = @extschema@.%2$I)',
                        _datasource || v_prefix || '_mi',
                        v_record_name);
        EXECUTE v_sql;

        -- add a *_div() function
        v_sql := format('CREATE FUNCTION @extschema@.%1$I(
a @extschema@.%2$I,
b @extschema@.%2$I)
RETURNS @extschema@.%3$I AS
$_$
DECLARE
    res @extschema@.%3$I;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;',
                        _datasource || v_prefix || '_div',
                        v_record_name,
                        -- the datatype is the same for the _db version
                        _datasource || '_history_rate');
        FOR i in 1..array_upper(_cols, 1) LOOP
            CONTINUE WHEN _cols[i][2] = ANY (c_no_agg);

            v_colname := _cols[i][1];
            v_extra := coalesce(_extra->'rate'->'colname'->>v_colname,
                                v_colname);
            v_extra := v_extra ||
                coalesce(_extra->'rate'->'suffix'->>v_colname, '_per_sec');

            v_sql := v_sql || chr(10)
                || format('    res.%1$I = (a.%2$I - b.%2$I)::double precision / sec;',
                          v_extra, v_colname);
    END LOOP;
    v_sql := v_sql || '

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT';
    EXECUTE v_sql;

    -- add a "/" operator
    v_sql := format('CREATE OPERATOR @extschema@./ ('
                    'PROCEDURE = @extschema@.%1$I,'
                    'LEFTARG = @extschema@.%2$I,'
                    'RIGHTARG = @extschema@.%2$I)',
                    _datasource || v_prefix || '_div',
                    v_record_name);
    EXECUTE v_sql;
    END LOOP;
END;
$$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_generic_datatype_setup */

-------------------
-- pg_stat_recovery
-------------------
SELECT @extschema@.powa_generic_module_setup('pg_stat_recovery',
$${
{last_replayed_read_lsn, pg_lsn}, {last_replayed_end_lsn, pg_lsn},
{last_replayed_tli, integer},
{replay_end_lsn, pg_lsn}, {replay_end_tli, integer},
{recovery_last_xact_time, timestamp with time zone},
{current_chunk_start_time, timestamp with time zone},
{pause_state, text}
}$$,
-- pg_stat_recovery only exists on pg19+
_min_version => 190000
);

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_recovery_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT last_replayed_read_lsn pg_lsn,
    OUT last_replayed_end_lsn pg_lsn,
    OUT last_replayed_tli integer,
    OUT replay_end_lsn pg_lsn,
    OUT replay_end_tli integer,
    OUT recovery_last_xact_time timestamp with time zone,
    OUT current_chunk_start_time timestamp with time zone,
    OUT pause_state text
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
                s.last_replayed_read_lsn,
                s.last_replayed_end_lsn,
                s.last_replayed_tli,
                s.replay_end_lsn,
                s.replay_end_tli,
                s.recovery_last_xact_time,
                s.current_chunk_start_time,
                s.pause_state
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_stat_recovery AS s ON true;
        ELSE
            RETURN QUERY SELECT now(),
                '0/0'::pg_lsn AS last_replayed_read_lsn,
                '0/0'::pg_lsn AS last_replayed_end_lsn,
                0::integer AS last_replayed_tli,
                '0/0'::pg_lsn AS replay_end_lsn,
                0::integer AS replay_end_tli,
                NULL::timestamp with time zone AS recovery_last_xact_time,
                NULL::timestamp with time zone AS current_chunk_start_time,
                NULL::text AS pause_state
            WHERE false;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
                s.last_replayed_read_lsn,
                s.last_replayed_end_lsn,
                s.last_replayed_tli,
                s.replay_end_lsn,
                s.replay_end_tli,
                s.recovery_last_xact_time,
                s.current_chunk_start_time,
                s.pause_state
        FROM @extschema@.powa_stat_recovery_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_recovery_src */

-------------------
-- pg_stat_lock
-------------------
SELECT @extschema@.powa_generic_module_setup('pg_stat_lock',
$${
{waits, bigint}, {wait_time, bigint},
{fastpath_exceeded, bigint},
{stats_reset, timestamp with time zone}
}$$,
$${
stats_reset
}$$,
_key_cols => $${
{locktype, text}
}$$,
-- pg_stat_lock only exists on pg19+
_min_version => 190000
);

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_lock_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT locktype text,
    OUT waits bigint,
    OUT wait_time bigint,
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


---------------------------------------
-- cleanup data sources generic support
---------------------------------------
DROP FUNCTION @extschema@.powa_generic_datatype_setup(text, text[], jsonb, boolean);
DROP FUNCTION @extschema@.powa_generic_module_setup(text, text[], text[], boolean, text[], boolean, integer);

-- Fix the toast tuple targets
SELECT @extschema@.powa_fix_toast_tuple_target();
