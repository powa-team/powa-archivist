-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

SET LOCAL statement_timeout = 0;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL standard_conforming_strings = on;
SET LOCAL client_min_messages = warning;
SET LOCAL search_path = pg_catalog;

ALTER TABLE @extschema@.powa_extension_config ADD COLUMN retention interval;
ALTER TABLE @extschema@.powa_module_config ADD COLUMN retention interval;
ALTER TABLE @extschema@.powa_db_module_config ADD COLUMN retention interval;

CREATE TYPE @extschema@.datasource_type AS ENUM ('module','extension','db_module');

CREATE FUNCTION @extschema@.powa_get_server_retention(_srvid integer, _feature_name text, _feature_type @extschema@.datasource_type)
RETURNS interval AS $_$
DECLARE
    v_feature_retention interval = NULL;
    v_ret interval = NULL;
BEGIN
    -- Address the local use case. We just short circuit the logic, it's a "limited" mode
    IF (_srvid = 0) THEN
        RETURN current_setting('powa.retention')::interval;
    END IF;
    -- Collector case
    -- Do we have a retention setting for this module (or extension). The query will be different depending
    -- on what _module_type is
    IF _feature_type = 'module' THEN
        SELECT retention INTO v_feature_retention FROM @extschema@.powa_module_config
           WHERE module = _feature_name
             AND srvid = _srvid;
    ELSIF _feature_type = 'extension' THEN
         SELECT retention INTO v_feature_retention FROM @extschema@.powa_extension_config
           WHERE extname = _feature_name
             AND srvid = _srvid;
    ELSEIF _feature_type = 'db_module' THEN
         SELECT retention INTO v_feature_retention FROM @extschema@.powa_db_module_config
           WHERE db_module = _feature_name
             AND srvid = _srvid;
    ELSE -- Should never happen
        RAISE EXCEPTION 'unknown feature type %', _feature_type;
    END IF;
    IF v_feature_retention IS NOT NULL THEN
        RETURN v_feature_retention;
    END IF;

    SELECT retention INTO v_ret
    FROM @extschema@.powa_servers
    WHERE id = _srvid;

    IF v_ret IS NOT NULL THEN
        RETURN v_ret;
    END IF;
    RAISE EXCEPTION 'Not retention found for server %', _srvid;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_f */

DROP FUNCTION @extschema@.powa_get_server_retention(_srvid integer);

CREATE OR REPLACE FUNCTION @extschema@.powa_databases_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_databases_purge', _srvid);
    v_rowcount    bigint;
    v_dropped_dbid oid[];
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_database','module'::@extschema@.datasource_type) INTO v_retention;

    -- Cleanup old dropped databases, over retention
    -- This will cascade automatically to powa_statements and other
    WITH dropped_databases AS
      ( DELETE FROM @extschema@.powa_databases
        WHERE dropped < (now() - v_retention * 1.2)
        AND srvid = _srvid
        RETURNING oid
        )
    SELECT array_agg(oid) INTO v_dropped_dbid FROM dropped_databases;

    PERFORM @extschema@.powa_log(format('%s (powa_databases) - rowcount: %s)',
           v_funcname,array_length(v_dropped_dbid,1)));
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_databases_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_statements_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_statements_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_statements','extension'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete data. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_statements_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_statements_hitory) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_statements_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_statements
    WHERE last_present_ts < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_statements) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_user_functions_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_user_functions','db_module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_user_functions_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_user_functions_history) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_user_functions_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_user_functions_history_db) rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_user_functions_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_indexes_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_all_indexes_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_all_indexes','db_module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_all_indexes_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_indexes_history) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_indexes_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_indexes_history_db) rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_indexes_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_all_tables_purge(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_all_tables_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_all_tables','db_module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_all_tables_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_tables_history) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_all_tables_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_all_tables_history_db) rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_all_tables_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_kcache_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_kcache_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_kcache','extension'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_kcache_metrics
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_kcache_metrics) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_kcache_metrics_db
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_kcache_metrics_db) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ language plpgsql; /* end of powa_kcache_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log('running powa_qualstats_purge(' || _srvid || ')');

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid,'pg_qualstats');

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_qualstats','extension'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_qualstats_constvalues_history
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    DELETE FROM @extschema@.powa_qualstats_quals_history
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_qualstats_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_wait_sampling_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := format('@extschema.%I(%s)',
                                 'powa_wait_sampling_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_wait_sampling','extension'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_wait_sampling_history
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_wait_sampling_history) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_wait_sampling_history_db
    WHERE upper(coalesce_range) < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s (powa_wait_sampling_history_db) - rowcount: %s',
            v_funcname, v_rowcount));
END;
$PROC$ language plpgsql
SET search_path = pg_catalog; /* end of powa_wait_sampling_purge */

CREATE OR REPLACE FUNCTION @extschema@.powa_replication_slots_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_replication_slots_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_replication_slots','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_replication_slots_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_activity_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_activity_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_activity','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_activity_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_archiver_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_archiver_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_archiver','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_archiver_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_bgwriter_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_bgwriter_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_bgwriter','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_bgwriter_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_checkpointer_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_checkpointer_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_checkpointer','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_checkpointer_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_database_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_database_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_database','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_database_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_database_conflicts_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_database_conflicts_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_database_conflicts','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_database_conflicts_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_io_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_io_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_io','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_io_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_replication_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_replication_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_replication','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_replication_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_slru_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_slru_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_slru','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_slru_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_subscription_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_subscription','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_subscription_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_subscription_stats_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_subscription_stats_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_subscription_stats','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_subscription_stats_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_wal_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_wal_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_wal','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_wal_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_wal_receiver_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_stat_wal_receiver_purge', _srvid);
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log(format('running %s', v_funcname));

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    SELECT @extschema@.powa_get_server_retention(_srvid,'pg_stat_wal_receiver','module'::@extschema@.datasource_type) INTO v_retention;

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM @extschema@.powa_stat_wal_receiver_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - rowcount: %s',
            v_funcname, v_rowcount));
END;
$function$;
