-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

CREATE OR REPLACE FUNCTION powa_all_relations_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_all_relations_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Insert cluster-wide relation statistics
    WITH rel(dbid, r) AS (
        SELECT oid,
            powa_stat_all_rel(oid)
        FROM pg_database
    )
    INSERT INTO powa_all_relations_history_current
        SELECT dbid, (r).relid,
        ROW(now(),(r).numscan, (r).tup_returned, (r).tup_fetched,
            (r).n_tup_ins, (r).n_tup_upd, (r).n_tup_del, (r).n_tup_hot_upd,
            (r).n_liv_tup, (r).n_dead_tup, (r).n_mod_since_analyze,
            (r).blks_read, (r).blks_hit, (r).last_vacuum, (r).vacuum_count,
        (r).last_autovacuum, (r).autovacuum_count, (r).last_analyze,
            (r).analyze_count, (r).last_autoanalyze,
            (r).autoanalyze_count)::powa_all_relations_history_record AS record
        FROM rel;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_all_relations_snapshot */

CREATE OR REPLACE FUNCTION powa_user_functions_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
    v_funcname    text := 'powa_user_functions_snapshot';
    v_rowcount    bigint;
BEGIN
    PERFORM powa_log(format('running %I', v_funcname));

    -- Insert cluster-wide user function statistics
    WITH func(dbid, r) AS (
        SELECT oid,
            powa_stat_user_functions(oid)
        FROM pg_database
    )
    INSERT INTO powa_user_functions_history_current
        SELECT dbid, (r).funcid,
        ROW(now(), (r).calls,
            (r).total_time,
            (r).self_time)::powa_user_functions_history_record AS record
        FROM func;

    GET DIAGNOSTICS v_rowcount = ROW_COUNT;

    perform powa_log(format('%I - rowcount: %s',
            v_funcname, v_rowcount));

    result := true;
END;
$PROC$ language plpgsql; /* end of powa_user_functions_snapshot */
