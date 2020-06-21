-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

ALTER TABLE public.powa_statements ADD IF NOT EXISTS last_present_ts timestamptz NULL DEFAULT now();
--create a performance index to speed up clean up process
create index  if not exists powa_statements_mru_idx on powa_statements (last_present_ts);

CREATE OR REPLACE FUNCTION powa_statements_snapshot(_srvid integer) RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_statements_snapshot';
    v_rowcount    bigint;
BEGIN
    -- In this function, we capture statements, and also aggregate counters by database
    -- so that the first screens of powa stay reactive even though there may be thousands
    -- of different statements
    -- We only capture databases that are still there
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    WITH capture AS(
        SELECT *
        FROM powa_statements_src(_srvid)
    ),
    mru as (UPDATE powa_statements set last_present_ts = now() 
            from capture         
            WHERE powa_statements.queryid = capture.queryid
              AND powa_statements.dbid = capture.dbid
              AND powa_statements.userid = capture.userid
              AND powa_statements.srvid = _srvid   ),
    missing_statements AS(
        INSERT INTO powa_statements (srvid, queryid, dbid, userid, query)
            SELECT _srvid, queryid, dbid, userid, query
            FROM capture c
            WHERE NOT EXISTS (SELECT 1
                              FROM powa_statements ps
                              WHERE ps.queryid = c.queryid
                              AND ps.dbid = c.dbid
                              AND ps.userid = c.userid
                              AND ps.srvid = _srvid
            )
    ),

    by_query AS (
        INSERT INTO powa_statements_history_current
            SELECT _srvid, queryid, dbid, userid,
            ROW(
                ts, calls, total_time, rows, shared_blks_hit, shared_blks_read,
                shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read,
                local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
                blk_read_time, blk_write_time
            )::powa_statements_history_record AS record
            FROM capture
    ),

    by_database AS (
        INSERT INTO powa_statements_history_current_db
            SELECT _srvid, dbid,
            ROW(
                ts, sum(calls), sum(total_time), sum(rows), sum(shared_blks_hit), sum(shared_blks_read),
                sum(shared_blks_dirtied), sum(shared_blks_written), sum(local_blks_hit), sum(local_blks_read),
                sum(local_blks_dirtied), sum(local_blks_written), sum(temp_blks_read), sum(temp_blks_written),
                sum(blk_read_time), sum(blk_write_time)
            )::powa_statements_history_record AS record
            FROM capture
            GROUP BY dbid, ts
    )

    SELECT count(*) INTO v_rowcount
    FROM capture;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_statements_src_tmp WHERE srvid = _srvid;
    END IF;

    result := true; -- For now we don't care. What could we do on error except crash anyway?
END;
$PROC$ language plpgsql; /* end of powa_statements_snapshot */


CREATE OR REPLACE FUNCTION public.powa_statements_purge(_srvid integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$   
DECLARE
    v_funcname    text := 'powa_statements_purge(' || _srvid || ')';
    v_rowcount    bigint;
    v_retention   interval;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    PERFORM powa_prevent_concurrent_snapshot(_srvid);

    SELECT powa_get_server_retention(_srvid) INTO v_retention;

    -- Delete obsolete data. We only bother with already coalesced data
    DELETE FROM powa_statements_history
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_hitory) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_statements_history_db
    WHERE upper(coalesce_range)< (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements_history_db) - rowcount: %s',
            v_funcname, v_rowcount));

    DELETE FROM powa_statements
    WHERE last_present_ts < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements) - rowcount: %s',
            v_funcname, v_rowcount));

END;
$function$
;
