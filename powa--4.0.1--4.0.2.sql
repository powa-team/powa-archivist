-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

ALTER TABLE public.powa_statements ADD IF NOT EXISTS created_ts timestamptz NULL DEFAULT now();

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
    WHERE created_ts < (now() - v_retention)
    AND srvid = _srvid;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    perform powa_log(format('%I (powa_statements) - rowcount: %s',
            v_funcname, v_rowcount));

END;
$function$
;
