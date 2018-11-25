-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit


INSERT INTO powa_functions (module, operation, function_name, added_manually, enabled) VALUES
      ('pg_stat_statements', 'purge', 'powa_databases_purge', false, true);


CREATE OR REPLACE FUNCTION powa_statements_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    ignore_regexp text :='^[[:space:]]*(DEALLOCATE|BEGIN|PREPARE TRANSACTION|COMMIT PREPARED|ROLLBACK PREPARED)';
    v_funcname    text := 'powa_statements_snapshot';
    v_rowcount    bigint;
BEGIN
    -- In this function, we capture statements, and also aggregate counters by database
    -- so that the first screens of powa stay reactive even though there may be thousands
    -- of different statements
    -- We only capture databases that are still there
    PERFORM powa_log(format('running %I', v_funcname));

    WITH capture AS(
        SELECT pgss.*
        FROM pg_stat_statements pgss
        JOIN pg_roles r ON pgss.userid = r.oid
        WHERE pgss.query !~* ignore_regexp
        AND NOT (r.rolname = ANY (string_to_array(current_setting('powa.ignored_users'),',')))
        AND dbid IN (SELECT oid FROM powa_databases WHERE dropped IS NULL)
    ),

    missing_statements AS(
        INSERT INTO powa_statements (queryid, dbid, userid, query)
            SELECT queryid, dbid, userid, query
            FROM capture c
            WHERE NOT EXISTS (SELECT 1
                              FROM powa_statements ps
                              WHERE ps.queryid = c.queryid
                              AND ps.dbid = c.dbid
                              AND ps.userid = c.userid
            )
    ),

    by_query AS (
        INSERT INTO powa_statements_history_current
            SELECT queryid, dbid, userid,
            ROW(
                now(), calls, total_time, rows, shared_blks_hit, shared_blks_read,
                shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read,
                local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
                blk_read_time, blk_write_time
            )::powa_statements_history_record AS record
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_statements_history_current_db
            SELECT dbid,
            ROW(
                now(), sum(calls), sum(total_time), sum(rows), sum(shared_blks_hit), sum(shared_blks_read),
                sum(shared_blks_dirtied), sum(shared_blks_written), sum(local_blks_hit), sum(local_blks_read),
                sum(local_blks_dirtied), sum(local_blks_written), sum(temp_blks_read), sum(temp_blks_written),
                sum(blk_read_time), sum(blk_write_time)
            )::powa_statements_history_record AS record
            FROM capture
            GROUP BY dbid
    )

    SELECT count(*) INTO v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true; -- For now we don't care. What could we do on error except crash anyway?
END;
$PROC$ language plpgsql; /* end of powa_statements_snapshot */


CREATE OR REPLACE FUNCTION powa_databases_purge() RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_databases_purge';
    v_rowcount    bigint;
    v_dropped_dbid oid[];
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Cleanup old dropped databases, over retention
    WITH dropped_databases AS
      ( DELETE FROM powa_databases
        WHERE dropped < (now() - current_setting('powa.retention')::interval*1.2)
        RETURNING oid
        )
    SELECT array_agg(oid) INTO v_dropped_dbid FROM dropped_databases;

    perform powa_log(format('%I (powa_databases) - rowcount: %s)',
           v_funcname,array_length(v_dropped_dbid,1)));

    -- This will cascade automatically to qualstat
    DELETE FROM powa_statements WHERE dbid = ANY (v_dropped_dbid);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements) - rowcount: %s)',
           v_funcname, v_rowcount));
END;
$PROC$ LANGUAGE plpgsql; /* end of powa_databases_purge */


CREATE OR REPLACE FUNCTION powa_statements_purge() RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_statements_purge';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_statements_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_hitory) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_statements_history_db WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

END;
$PROC$ LANGUAGE plpgsql; /* end of powa_statements_purge */

CREATE OR REPLACE FUNCTION powa_user_functions_purge() RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_user_functions_purge';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_user_functions_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

END;
$PROC$ LANGUAGE plpgsql; /* end of powa_user_functions_purge */


CREATE OR REPLACE FUNCTION powa_all_relations_purge() RETURNS void AS $PROC$
DECLARE
    v_funcname    text := 'powa_all_relations_purge';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_all_relations_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

END;
$PROC$ LANGUAGE plpgsql; /* end of powa_all_relations_purge */


/*
 * powa_kcache snapshot collection.
 */
CREATE OR REPLACE FUNCTION powa_kcache_snapshot() RETURNS void as $PROC$
DECLARE
  result bool;
    v_funcname    text := 'powa_kcache_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    WITH capture AS (
        SELECT *
        FROM pg_stat_kcache() k
        JOIN pg_roles r ON r.oid = k.userid
        WHERE NOT (r.rolname = ANY (string_to_array(current_setting('powa.ignored_users'),',')))
        AND dbid IN (SELECT oid FROM powa_databases WHERE dropped IS NULL)
    ),

    by_query AS (
        INSERT INTO powa_kcache_metrics_current (queryid, dbid, userid, metrics)
            SELECT queryid, dbid, userid, (now(), reads, writes, user_time, system_time)::kcache_type
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_kcache_metrics_current_db (dbid, metrics)
            SELECT dbid, (now(), sum(reads), sum(writes), sum(user_time), sum(system_time))::kcache_type
            FROM capture
            GROUP BY dbid
    )

    SELECT COUNT(*) into v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true;
END
$PROC$ language plpgsql; /* end of powa_kcache_snapshot */


CREATE OR REPLACE FUNCTION powa_qualstats_snapshot() RETURNS void as $PROC$
DECLARE
    result     bool;
    v_funcname text := 'powa_qualstats_snapshot';
    v_rowcount bigint;
BEGIN
  PERFORM powa_log(format('running %I', v_funcname));

  WITH capture AS (
    SELECT pgqs.*, s.query
    FROM pg_qualstats_by_query pgqs
    JOIN powa_statements s USING(queryid, dbid, userid)
    JOIN pg_roles r ON s.userid = r.oid
    AND NOT (r.rolname = ANY (string_to_array(current_setting('powa.ignored_users'),',')))
    WHERE dbid IN (SELECT oid FROM powa_databases WHERE dropped IS NULL)
  ),
  missing_quals AS (
      INSERT INTO powa_qualstats_quals (qualid, queryid, dbid, userid, quals)
        SELECT DISTINCT qs.qualnodeid, qs.queryid, qs.dbid, qs.userid, array_agg(DISTINCT q::qual_type)
        FROM capture qs,
        LATERAL (SELECT (unnest(quals)).*) as q
        WHERE NOT EXISTS (
          SELECT 1
          FROM powa_qualstats_quals nh
          WHERE nh.qualid = qs.qualnodeid AND nh.queryid = qs.queryid
            AND nh.dbid = qs.dbid AND nh.userid = qs.userid
        )
        GROUP BY qualnodeid, queryid, dbid, userid
      RETURNING *
  ),
  by_qual AS (
      INSERT INTO powa_qualstats_quals_history_current (qualid, queryid, dbid, userid, ts, occurences, execution_count, nbfiltered)
      SELECT qs.qualnodeid, qs.queryid, qs.dbid, qs.userid, now(), sum(occurences), sum(execution_count), sum(nbfiltered)
        FROM capture as qs
        GROUP BY qualnodeid, qs.queryid, qs.dbid, qs.userid
      RETURNING *
  ),
  by_qual_with_const AS (
      INSERT INTO powa_qualstats_constvalues_history_current(qualid, queryid, dbid, userid, ts, occurences, execution_count, nbfiltered, constvalues)
      SELECT qualnodeid, qs.queryid, qs.dbid, qs.userid, now(), occurences, execution_count, nbfiltered, constvalues
      FROM capture as qs
  )
  SELECT COUNT(*) into v_rowcount
  FROM capture;

  perform powa_log(format('%I - rowcount: %s',
        v_funcname, v_rowcount));

  result := true;
  PERFORM pg_qualstats_reset();
END
$PROC$ language plpgsql; /* end of powa_qualstats_snapshot */


/*
 * powa_wait_sampling snapshot collection.
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_snapshot() RETURNS void as $PROC$
DECLARE
  result bool;
    v_funcname    text := 'powa_wait_sampling_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    WITH capture AS (
        -- the various background processes report wait events but don't have
        -- associated queryid.  Gather them all under a fake 0 dbid
        SELECT COALESCE(pgss.dbid, 0) AS dbid, s.event_type, s.event, s.queryid,
            sum(s.count) as count
        FROM pg_wait_sampling_profile s
        -- pg_wait_sampling doesn't offer a per (userid, dbid, queryid) view,
        -- only per pid, but pid can be reused for different databases or users
        -- so we cannot deduce db or user from it.  However, queryid should be
        -- unique across differet databases, so we retrieve the dbid this way.
        LEFT JOIN pg_stat_statements(false) pgss ON pgss.queryid = s.queryid
        WHERE event_type IS NOT NULL AND event IS NOT NULL
        AND COALESCE(pgss.dbid, 0) IN (SELECT oid FROM powa_databases WHERE dropped IS NULL UNION ALL SELECT 0)
        GROUP BY pgss.dbid, s.event_type, s.event, s.queryid
    ),

    by_query AS (
        INSERT INTO powa_wait_sampling_history_current (queryid, dbid,
                event_type, event, record)
            SELECT queryid, dbid, event_type, event, (now(), count)::wait_sampling_type
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_wait_sampling_history_current_db (dbid,
                event_type, event, record)
            SELECT dbid, event_type, event, (now(), sum(count))::wait_sampling_type
            FROM capture
            GROUP BY dbid, event_type, event
    )

    SELECT COUNT(*) into v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true;
END
$PROC$ language plpgsql; /* end of powa_wait_sampling_snapshot */

