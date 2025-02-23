-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

CREATE OR REPLACE FUNCTION @extschema@.powa_take_snapshot(_srvid integer = 0) RETURNS integer
AS $PROC$
DECLARE
  purgets timestamp with time zone;
  purge_seq  bigint;
  r          record;
  v_state    text;
  v_msg      text;
  v_detail   text;
  v_hint     text;
  v_context  text;
  v_title    text = 'PoWA - ';
  v_rowcount bigint;
  v_nb_err int = 0;
  v_errs     text[] = '{}';
  v_pattern  text = '@extschema@.powa_take_snapshot(%s): function %s.%I failed:
              state  : %s
              message: %s
              detail : %s
              hint   : %s
              context: %s';
  v_pattern_simple text = '@extschema@.powa_take_snapshot(%s): function %s.%I failed: %s';

  v_pattern_cat  text = '@extschema@.powa_take_snapshot(%s): function @extschema@.powa_catalog_generic_snapshot for catalog %s failed:
              state  : %s
              message: %s
              detail : %s
              hint   : %s
              context: %s';
  v_pattern_cat_simple text = '@extschema@.powa_take_snapshot(%s): function @extschema@.powa_catalog_generic_snapshot for catalog %s failed: %s';
  v_coalesce bigint;
  v_catname text;
BEGIN
    PERFORM set_config('application_name',
        v_title || ' snapshot database list',
        false);
    PERFORM @extschema@.powa_log('start of powa_take_snapshot(' || _srvid || ')');

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    UPDATE @extschema@.powa_snapshot_metas
    SET coalesce_seq = coalesce_seq + 1,
        errors = NULL,
        snapts = now()
    WHERE srvid = _srvid
    RETURNING coalesce_seq INTO purge_seq;

    PERFORM @extschema@.powa_log(format('coalesce_seq(%s): %s', _srvid, purge_seq));

    IF (_srvid = 0) THEN
        SELECT current_setting('powa.coalesce') INTO v_coalesce;
    ELSE
        SELECT powa_coalesce
        FROM @extschema@.powa_servers
        WHERE id = _srvid
        INTO v_coalesce;
    END IF;

    -- For all enabled snapshot functions in the powa_functions table, execute
    FOR r IN SELECT CASE external
                WHEN true THEN quote_ident(nsp.nspname)
                ELSE '@extschema@'
             END AS schema, function_name AS funcname
             FROM @extschema@.powa_all_functions AS pf
             LEFT JOIN pg_extension AS ext ON pf.kind = 'extension'
                AND ext.extname = pf.name
             LEFT JOIN pg_namespace AS nsp ON nsp.oid = ext.extnamespace
             WHERE operation='snapshot'
             AND enabled
             AND srvid = _srvid
             ORDER BY priority, name
    LOOP
      -- Call all of them, for the current srvid
      BEGIN
        PERFORM @extschema@.powa_log(format('calling snapshot function: %s.%I',
                                     r.schema, r.funcname));
        PERFORM set_config('application_name',
            v_title || quote_ident(r.funcname) || '(' || _srvid || ')', false);

        EXECUTE format('SELECT %s.%I(%s)', r.schema, r.funcname, _srvid);
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS
              v_state   = RETURNED_SQLSTATE,
              v_msg     = MESSAGE_TEXT,
              v_detail  = PG_EXCEPTION_DETAIL,
              v_hint    = PG_EXCEPTION_HINT,
              v_context = PG_EXCEPTION_CONTEXT;

          RAISE warning '%', format(v_pattern, _srvid, r.schema, r.funcname,
            v_state, v_msg, v_detail, v_hint, v_context);

          v_errs := array_append(v_errs, format(v_pattern_simple, _srvid,
                r.schema, r.funcname, v_msg));

          v_nb_err = v_nb_err + 1;
      END;
    END LOOP;

    -- Coalesce datas if needed. The _srvid % 20 is there to avoid having all coalesces run at once
    IF ( ((purge_seq + (_srvid % 20) ) % v_coalesce ) = 0 )
    THEN
      PERFORM @extschema@.powa_log(
        format('coalesce needed, srvid: %s - seq: %s - coalesce seq: %s',
        _srvid, purge_seq, v_coalesce ));

      FOR r IN SELECT CASE external
                  WHEN true THEN quote_ident(nsp.nspname)
                  ELSE '@extschema@'
               END AS schema, function_name AS funcname
               FROM @extschema@.powa_all_functions AS pf
               LEFT JOIN pg_extension AS ext ON pf.kind = 'extension'
                  AND ext.extname = pf.name
               LEFT JOIN pg_namespace AS nsp ON nsp.oid = ext.extnamespace
               WHERE operation='aggregate'
               AND enabled
               AND srvid = _srvid
               ORDER BY priority, name
      LOOP
        -- Call all of them, for the current srvid
        BEGIN
          PERFORM @extschema@.powa_log(format('calling aggregate function: %s.%I(%s)',
                r.schema, r.funcname, _srvid));

          PERFORM set_config('application_name',
              v_title || quote_ident(r.funcname) || '(' || _srvid || ')',
              false);

          EXECUTE format('SELECT %s.%I(%s)', r.schema, r.funcname, _srvid);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;

            RAISE warning '%', format(v_pattern, _srvid, r.schema, r.funcname,
                v_state, v_msg, v_detail, v_hint, v_context);

            v_errs := array_append(v_errs, format(v_pattern_simple, _srvid,
                    r.schema, r.funcname, v_msg));

            v_nb_err = v_nb_err + 1;
        END;
      END LOOP;

      PERFORM set_config('application_name',
          v_title || 'UPDATE powa_snapshot_metas.aggets',
          false);
      UPDATE @extschema@.powa_snapshot_metas
      SET aggts = now()
      WHERE srvid = _srvid;
    END IF;

    -- We also purge, at the pass after the coalesce
    -- The _srvid % 20 is there to avoid having all purges run at once
    IF ( ((purge_seq + (_srvid % 20)) % v_coalesce) = 1 )
    THEN
      PERFORM @extschema@.powa_log(
        format('purge needed, srvid: %s - seq: %s coalesce seq: %s',
        _srvid, purge_seq, v_coalesce));

      FOR r IN SELECT CASE external
                    WHEN true THEN quote_ident(nsp.nspname)
                    ELSE '@extschema@'
               END AS schema, function_name AS funcname
               FROM @extschema@.powa_all_functions AS pf
               LEFT JOIN pg_extension AS ext ON pf.kind = 'extension'
                  AND ext.extname = pf.name
               LEFT JOIN pg_namespace AS nsp ON nsp.oid = ext.extnamespace
               WHERE operation='purge'
               AND enabled
               AND srvid = _srvid
               ORDER BY priority, name
      LOOP
        -- Call all of them, for the current srvid
        BEGIN
          PERFORM @extschema@.powa_log(format('calling purge function: %s.%I(%s)',
                r.schema, r.funcname, _srvid));
          PERFORM set_config('application_name',
              v_title || quote_ident(r.funcname) || '(' || _srvid || ')',
              false);

          EXECUTE format('SELECT %s.%I(%s)', r.schema, r.funcname, _srvid);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;

            RAISE warning '%', format(v_pattern, _srvid, r.schema, r.funcname,
                v_state, v_msg, v_detail, v_hint, v_context);

            v_errs := array_append(v_errs, format(v_pattern_simple, _srvid,
                  r.schema, r.funcname, v_msg));

            v_nb_err = v_nb_err + 1;
        END;
      END LOOP;

      PERFORM set_config('application_name',
          v_title || 'UPDATE powa_snapshot_metas.purgets',
          false);
      UPDATE @extschema@.powa_snapshot_metas
      SET purgets = now()
      WHERE srvid = _srvid;
    END IF;

    -- and finally we call the snapshot function for the per-db catalog import,
    -- if this is a remote server
    IF (_srvid != 0) THEN
      FOR v_catname IN SELECT catname FROM @extschema@.powa_catalogs ORDER BY priority
      LOOP
        PERFORM @extschema@.powa_log(format('calling catalog function: %s.%I(%s, %s)',
              '@extschema@', 'powa_catalog_generic_snapshot', _srvid, v_catname));
        PERFORM set_config('application_name',
            v_title || quote_ident('powa_catalog_generic_snapshot')
                    || '(' || _srvid || ', ' || v_catname || ')', false);

        BEGIN
          PERFORM @extschema@.powa_catalog_generic_snapshot(_srvid, v_catname);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;

            RAISE warning '%', format(v_pattern_cat, _srvid, v_catname,
                v_state, v_msg, v_detail, v_hint, v_context);

            v_errs := array_append(v_errs, format(v_pattern_cat_simple, _srvid,
                  v_catname, v_msg));

            v_nb_err = v_nb_err + 1;
        END;
      END LOOP;
    END IF;

    IF (v_nb_err > 0) THEN
      UPDATE @extschema@.powa_snapshot_metas
      SET errors = v_errs
      WHERE srvid = _srvid;
    END IF;

    PERFORM @extschema@.powa_log('end of powa_take_snapshot(' || _srvid || ')');
    PERFORM set_config('application_name',
        v_title || 'snapshot finished',
        false);

    return v_nb_err;
END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_take_snapshot(int) */

CREATE OR REPLACE FUNCTION @extschema@.powa_check_created_extensions()
RETURNS event_trigger
LANGUAGE plpgsql
AS $_$
DECLARE
    v_extname text;
    v_res bool;
BEGIN
    SELECT extname INTO v_extname
    FROM pg_event_trigger_ddl_commands() d
    JOIN pg_extension e ON d.classid = 'pg_extension'::regclass
        AND d.objid = e.oid
    JOIN @extschema@.powa_extensions p USING (extname)
    WHERE d.object_type = 'extension';

    -- Bail out if this isn't a known extension
    IF (v_extname IS NULL) THEN
        RETURN;
    END IF;

    RAISE LOG 'powa: automatically activing extension %', v_extname;
    SELECT @extschema@.powa_activate_extension(0, v_extname) INTO v_res;

    IF (NOT v_res) THEN
        RAISE WARNING 'Could not automatically activate extension "%"', v_extname;
    END IF;
END;
$_$
SET search_path = pg_catalog; /* end of powa_check_created_extensions */

CREATE OR REPLACE FUNCTION @extschema@.powa_check_dropped_extensions()
RETURNS event_trigger
LANGUAGE plpgsql
AS $_$
DECLARE
    v_extname text;
    v_state   text;
    v_msg     text;
    v_detail  text;
    v_hint    text;
    v_context text;
BEGIN
    -- We unregister extensions regardless the "enabled" field
    FOR v_extname IN SELECT pe.extname
        FROM pg_event_trigger_dropped_objects() d
        LEFT JOIN @extschema@.powa_extensions pe ON pe.extname = d.object_name
        WHERE d.object_type = 'extension'
    LOOP
        BEGIN
            RAISE LOG 'powa: automatically deactiving extension %', v_extname;
            PERFORM @extschema@.powa_deactivate_extension(0, v_extname);
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;
            RAISE WARNING 'Could not deactivate extension %:"
                state  : %
                message: %
                detail : %
                hint   : %
                context: %', v_extname, v_state, v_msg, v_detail, v_hint,
                             v_context;
        END;
    END LOOP;
END;
$_$
SET search_path = pg_catalog; /* end of powa_check_dropped_extensions */

CREATE OR REPLACE FUNCTION @extschema@.powa_user_functions_aggregate(_srvid integer)
RETURNS void AS $PROC$
DECLARE
    v_funcname    text := format('@extschema@.%I(%s)',
                                 'powa_user_functions_aggregate', _srvid);
    v_rowcount    bigint;
BEGIN
    PERFORM @extschema@.powa_log('running powa_user_functions_aggregate(' || _srvid ||')');

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

    -- aggregate user_functions table
    INSERT INTO @extschema@.powa_user_functions_history
        (srvid, dbid, funcid, coalesce_range, records,
                mins_in_range, maxs_in_range)
        SELECT srvid, dbid, funcid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts), min((record).calls),min((record).total_time),
                min((record).self_time))::@extschema@.powa_user_functions_history_record,
            ROW(max((record).ts), max((record).calls),max((record).total_time),
                max((record).self_time))::@extschema@.powa_user_functions_history_record
        FROM @extschema@.powa_user_functions_history_current
        WHERE srvid = _srvid
        GROUP BY srvid, dbid, funcid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_user_functions_history_current) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_user_functions_history_current WHERE srvid = _srvid;

    -- aggregate user_functions_db table
    INSERT INTO @extschema@.powa_user_functions_history_db
        (srvid, dbid, coalesce_range, records, mins_in_range, maxs_in_range)
        SELECT srvid, dbid,
            tstzrange(min((record).ts), max((record).ts),'[]'),
            array_agg(record),
            ROW(min((record).ts), min((record).calls),min((record).total_time),
                min((record).self_time))::@extschema@.powa_user_functions_history_record,
            ROW(max((record).ts), max((record).calls),max((record).total_time),
                max((record).self_time))::@extschema@.powa_user_functions_history_record
        FROM @extschema@.powa_user_functions_history_current_db
        WHERE srvid = _srvid
        GROUP BY srvid, dbid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.powa_log(format('%s - (powa_user_functions_history_current_db) rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM @extschema@.powa_user_functions_history_current_db WHERE srvid = _srvid;
 END;
$PROC$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_user_functions_aggregate */

CREATE OR REPLACE FUNCTION @extschema@.powa_stat_replication_src(IN _srvid integer,
    OUT ts timestamp with time zone,
    OUT current_lsn pg_lsn,
    OUT pid integer,
    OUT usename text,
    OUT application_name text,
    OUT client_addr inet,
    OUT backend_start timestamp with time zone,
    OUT backend_xmin xid,
    OUT state text,
    OUT sent_lsn pg_lsn,
    OUT write_lsn pg_lsn,
    OUT flush_lsn pg_lsn,
    OUT replay_lsn pg_lsn,
    OUT write_lag interval,
    OUT flush_lag interval,
    OUT replay_lag interval,
    OUT sync_priority integer,
    OUT sync_state text,
    OUT reply_time timestamp with time zone
) RETURNS SETOF record STABLE AS $PROC$
DECLARE
    v_current_lsn pg_lsn;
    v_pg_version_num int;
BEGIN
    IF (_srvid = 0) THEN
        v_pg_version_num := current_setting('server_version_num')::int;

        IF v_pg_version_num < 100000 THEN
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

        -- We use a LEFT JOIN on the pg_stat_replication view to make sure that
        -- we always return at least one (all-NULL) row, so client apps can
        -- detect when all the replication connections are down.
        --
        -- We handle older versions compatibility even if we don't actually
        -- enable them for pg 12 and below, are there are no aggregate
        -- functions for pg_lsn datatype before pg13.  This way if the
        -- repository server is on pg13+ and a remote is on pg12-, we can still
        -- support that datasource.

        -- pg12+, reply_time is added
        IF v_pg_version_num >= 120000 THEN
            RETURN QUERY SELECT now,
            v_current_lsn,
            s.pid, s.usename::text AS usename, s.application_name, s.client_addr,
            s.backend_start, s.backend_xmin, s.state, s.sent_lsn, s.write_lsn,
            s.flush_lsn, s.replay_lsn, s.write_lag, s.flush_lag, s.replay_lag,
            s.sync_priority, s.sync_state, s.reply_time
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_stat_replication AS s ON true;
        -- pg10+, *_location fields renamed to *_lsn, and *_lag fields added
        ELSIF v_pg_version_num >= 100000 THEN
            RETURN QUERY SELECT now,
            v_current_lsn,
            s.pid, s.usename::text AS usename, s.application_name, s.client_addr,
            s.backend_start, s.backend_xmin, s.state, s.sent_lsn, s.write_lsn,
            s.flush_lsn, s.replay_lsn, s.write_lag, s.flush_lag, s.replay_lag,
            s.sync_priority, s.sync_state, NULL::timestamptz AS reply_time
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_stat_replication AS s ON true;
        -- pg9.4+ definition
        ELSE
            RETURN QUERY SELECT now,
            v_current_lsn,
            s.pid, s.usename::text AS usename, s.application_name, s.client_addr,
            s.backend_start, s.backend_xmin, s.state,
            s.sent_location AS sent_lsn, s.write_location AS write_lsn,
            s.flush_location AS flush_lsn, s.replay_location AS replay_lsn,
            NULL::interval AS write_lag, NULL::interval AS flush_lag,
            NULL::interval AS replay_lag,
            s.sync_priority, s.sync_state, NULL::timestamptz AS reply_time
            FROM (SELECT now() AS now) n
            LEFT JOIN pg_catalog.pg_stat_replication AS s ON true;
        END IF;
    ELSE
        RETURN QUERY SELECT s.ts,
        s.current_lsn,
        s.pid, s.usename, s.application_name, s.client_addr,
        s.backend_start, s.backend_xmin, s.state, s.sent_lsn, s.write_lsn,
        s.flush_lsn, s.replay_lsn, s.write_lag, s.flush_lag, s.replay_lag,
        s.sync_priority, s.sync_state, s.reply_time
        FROM @extschema@.powa_stat_replication_src_tmp AS s
        WHERE s.srvid = _srvid;
    END IF;
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_stat_replication_src */
