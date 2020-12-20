-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

ALTER TABLE public.powa_extensions DROP CONSTRAINT powa_extensions_srvid_fkey;
ALTER TABLE public.powa_extensions ADD
    FOREIGN KEY (srvid) REFERENCES public.powa_servers (id)
    MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE;


CREATE OR REPLACE FUNCTION powa_qualstats_snapshot(_srvid integer) RETURNS void as $PROC$
DECLARE
    result     bool;
    v_funcname text := 'powa_qualstats_snapshot';
    v_rowcount bigint;
BEGIN
  PERFORM powa_log(format('running %I', v_funcname));

  PERFORM powa_prevent_concurrent_snapshot(_srvid);

  WITH capture AS (
    SELECT *
    FROM powa_qualstats_src(_srvid) q
    WHERE EXISTS (SELECT 1
      FROM powa_statements s
      WHERE s.srvid = _srvid
      AND q.queryid = s.queryid
      AND q.dbid = s.dbid
      AND q.userid = s.userid)
  ),
  missing_quals AS (
      INSERT INTO public.powa_qualstats_quals (srvid, qualid, queryid, dbid, userid, quals)
        SELECT DISTINCT _srvid AS srvid, qs.qualnodeid, qs.queryid, qs.dbid, qs.userid,
          array_agg(DISTINCT q::qual_type)
        FROM capture qs,
        LATERAL (SELECT (unnest(quals)).*) as q
        WHERE NOT EXISTS (
          SELECT 1
          FROM powa_qualstats_quals nh
          WHERE nh.srvid = _srvid
            AND nh.qualid = qs.qualnodeid
            AND nh.queryid = qs.queryid
            AND nh.dbid = qs.dbid
            AND nh.userid = qs.userid
        )
        GROUP BY srvid, qualnodeid, qs.queryid, qs.dbid, qs.userid
      RETURNING *
  ),
  by_qual AS (
      INSERT INTO public.powa_qualstats_quals_history_current (srvid, qualid, queryid,
        dbid, userid, ts, occurences, execution_count, nbfiltered,
        mean_err_estimate_ratio, mean_err_estimate_num)
      SELECT _srvid AS srvid, qs.qualnodeid, qs.queryid, qs.dbid, qs.userid,
          ts, sum(occurences), sum(execution_count), sum(nbfiltered),
          avg(mean_err_estimate_ratio), avg(mean_err_estimate_num)
        FROM capture as qs
        GROUP BY srvid, ts, qualnodeid, qs.queryid, qs.dbid, qs.userid
      RETURNING *
  ),
  by_qual_with_const AS (
      INSERT INTO public.powa_qualstats_constvalues_history_current(srvid, qualid,
        queryid, dbid, userid, ts, occurences, execution_count, nbfiltered,
        mean_err_estimate_ratio, mean_err_estimate_num, constvalues)
      SELECT _srvid, qualnodeid, qs.queryid, qs.dbid, qs.userid, ts,
        occurences, execution_count, nbfiltered, mean_err_estimate_ratio,
        mean_err_estimate_num, constvalues
      FROM capture as qs
  )
  SELECT COUNT(*) into v_rowcount
  FROM capture;

  perform powa_log(format('%I - rowcount: %s',
        v_funcname, v_rowcount));

    IF (_srvid != 0) THEN
        DELETE FROM powa_qualstats_src_tmp WHERE srvid = _srvid;
    END IF;

  result := true;

  -- pg_qualstats metrics are not accumulated, so we force a reset after every
  -- snapshot.  For local snapshot this is done here, remote snapshots will
  -- rely on the collector doing it through query_cleanup.
  IF (_srvid = 0) THEN
    PERFORM pg_qualstats_reset();
  END IF;
END
$PROC$ language plpgsql; /* end of powa_qualstats_snapshot */

CREATE OR REPLACE FUNCTION public.powa_statements_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM public.powa_log('Resetting powa_statements_history(' || _srvid || ')');
    DELETE FROM public.powa_statements_history WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_statements_history_current(' || _srvid || ')');
    DELETE FROM public.powa_statements_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_statements_history_db(' || _srvid || ')');
    DELETE FROM public.powa_statements_history_db WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_statements_history_current_db(' || _srvid || ')');
    DELETE FROM public.powa_statements_history_current_db WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_statements_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_statements_src_tmp WHERE srvid = _srvid;

    -- if 3rd part datasource has FK on it, throw everything away
    DELETE FROM public.powa_statements WHERE srvid = _srvid;
    PERFORM public.powa_log('Resetting powa_statements(' || _srvid || ')');

    RETURN true;
END;
$function$; /* end of powa_statements_reset */

CREATE OR REPLACE FUNCTION public.powa_user_functions_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM public.powa_log('Resetting powa_user_functions_history(' || _srvid || ')');
    DELETE FROM public.powa_user_functions_history WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_user_functions_history_current(' || _srvid || ')');
    DELETE FROM public.powa_user_functions_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_user_functions_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_user_functions_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_user_functions_reset */

CREATE OR REPLACE FUNCTION public.powa_all_relations_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM public.powa_log('Resetting powa_all_relations_history(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_history WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_all_relations_history_db(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_history_db WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_all_relations_history_current(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_all_relations_history_current_db(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_history_current_db WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_all_relations_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_all_relations_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_all_relations_reset */

CREATE OR REPLACE FUNCTION public.powa_stat_bgwriter_reset(_srvid integer)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM public.powa_log('Resetting powa_stat_bgwriter_history(' || _srvid || ')');
    DELETE FROM public.powa_stat_bgwriter_history WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_stat_bgwriter_history_current(' || _srvid || ')');
    DELETE FROM public.powa_stat_bgwriter_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('Resetting powa_stat_bgwriter_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_stat_bgwriter_src_tmp WHERE srvid = _srvid;

    RETURN true;
END;
$function$; /* end of powa_stat_bgwriter_reset */

/*
 * powa_kcache reset
 */
CREATE OR REPLACE FUNCTION powa_kcache_reset(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_kcache_reset(' || _srvid || ')';
    v_rowcount    bigint;
BEGIN
    PERFORM public.powa_log(format('running %I', v_funcname));

    PERFORM public.powa_log('resetting powa_kcache_metrics(' || _srvid || ')');
    DELETE FROM public.powa_kcache_metrics WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_kcache_metrics_db(' || _srvid || ')');
    DELETE FROM public.powa_kcache_metrics_db WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_kcache_metrics_current(' || _srvid || ')');
    DELETE FROM public.powa_kcache_metrics_current WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_kcache_metrics_current_db(' || _srvid || ')');
    DELETE FROM public.powa_kcache_metrics_current_db WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_kcache_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_kcache_src_tmp WHERE srvid = _srvid;
END;
$PROC$ language plpgsql; /* end of powa_kcache_reset */

/*
 * powa_qualstats_reset
 */
CREATE OR REPLACE FUNCTION powa_qualstats_reset(_srvid integer)
RETURNS void as $PROC$
BEGIN
  PERFORM public.powa_log('running powa_qualstats_reset(' || _srvid || ')');

  PERFORM public.powa_log('resetting powa_qualstats_quals(' || _srvid || ')');
  DELETE FROM public.powa_qualstats_quals WHERE srvid = _srvid;
  -- cascaded :
  -- powa_qualstats_quals_history
  -- powa_qualstats_quals_history_current
  -- powa_qualstats_constvalues_history
  -- powa_qualstats_constvalues_history_current

  PERFORM public.powa_log('resetting powa_qualstats_src_tmp(' || _srvid || ')');
  DELETE FROM public.powa_qualstats_src_tmp WHERE srvid = _srvid;
END;
$PROC$ language plpgsql; /* end of powa_qualstats_reset */

/*
 * powa_wait_sampling reset
 */
CREATE OR REPLACE FUNCTION powa_wait_sampling_reset(_srvid integer)
RETURNS void as $PROC$
DECLARE
    v_funcname    text := 'powa_wait_sampling_reset(' || _srvid || ')';
    v_rowcount    bigint;
BEGIN
    PERFORM public.powa_log(format('running %I', v_funcname));

    PERFORM public.powa_log('resetting powa_wait_sampling_history(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_history WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_wait_sampling_history_db(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_history_db WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_wait_sampling_history_current(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_history_current WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_wait_sampling_history_current_db(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_history_current_db WHERE srvid = _srvid;

    PERFORM public.powa_log('resetting powa_wait_sampling_src_tmp(' || _srvid || ')');
    DELETE FROM public.powa_wait_sampling_src_tmp WHERE srvid = _srvid;
END;
$PROC$ language plpgsql; /* end of powa_wait_sampling_reset */
