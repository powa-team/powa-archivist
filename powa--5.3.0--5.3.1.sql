-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit
/*
 * powa_qualstats_purge
 */
CREATE OR REPLACE FUNCTION @extschema@.powa_qualstats_purge(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_retention   interval;
BEGIN
    PERFORM @extschema@.powa_log('running powa_qualstats_purge(' || _srvid || ')');

    PERFORM @extschema@.powa_prevent_concurrent_snapshot(_srvid);

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
