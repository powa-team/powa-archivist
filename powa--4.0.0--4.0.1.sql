-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

CREATE OR REPLACE FUNCTION powa_all_relations_history_db_div(
    a powa_all_relations_history_db_record,
    b powa_all_relations_history_db_record)
RETURNS powa_all_relations_history_db_rate AS
$_$
DECLARE
    res powa_all_relations_history_db_rate;
    sec integer;
BEGIN
    res.sec = extract(EPOCH FROM (a.ts - b.ts));
    IF res.sec = 0 THEN
        sec = 1;
    ELSE
        sec = res.sec;
    END IF;
    res.seq_scan_per_sec = (a.seq_scan - b.seq_scan)::double precision / sec;
    res.idx_scan_per_sec = (a.idx_scan - b.idx_scan)::double precision / sec;
    res.tup_returned_per_sec = (a.tup_returned - b.tup_returned)::double precision / sec;
    res.tup_fetched_per_sec = (a.tup_fetched - b.tup_fetched)::double precision / sec;
    res.n_tup_ins_per_sec = (a.n_tup_ins - b.n_tup_ins)::double precision / sec;
    res.n_tup_upd_per_sec = (a.n_tup_upd - b.n_tup_upd)::double precision / sec;
    res.n_tup_del_per_sec = (a.n_tup_del - b.n_tup_del)::double precision / sec;
    res.n_tup_hot_upd_per_sec = (a.n_tup_hot_upd - b.n_tup_hot_upd)::double precision / sec;
    res.n_liv_tup_per_sec = (a.n_liv_tup - b.n_liv_tup)::double precision / sec;
    res.n_dead_tup_per_sec = (a.n_dead_tup - b.n_dead_tup)::double precision / sec;
    res.n_mod_since_analyze_per_sec = (a.n_mod_since_analyze - b.n_mod_since_analyze)::double precision / sec;
    res.blks_read_per_sec = (a.blks_read - b.blks_read)::double precision / sec;
    res.blks_hit_per_sec = (a.blks_hit - b.blks_hit)::double precision / sec;
    res.vacuum_count_per_sec = (a.vacuum_count - b.vacuum_count)::double precision / sec;
    res.autovacuum_count_per_sec = (a.autovacuum_count - b.autovacuum_count)::double precision / sec;
    res.analyze_count_per_sec = (a.analyze_count - b.analyze_count)::double precision / sec;
    res.autoanalyze_count_per_sec = (a.autoanalyze_count - b.autoanalyze_count)::double precision / sec;

    return res;
END;
$_$
LANGUAGE plpgsql IMMUTABLE STRICT;
