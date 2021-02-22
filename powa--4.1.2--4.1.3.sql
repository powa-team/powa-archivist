-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

CREATE OR REPLACE FUNCTION powa_prevent_concurrent_snapshot(_srvid integer = 0)
RETURNS void
AS $PROC$
DECLARE
    v_state   text;
    v_msg     text;
    v_detail  text;
    v_hint    text;
    v_context text;
BEGIN
    BEGIN
        PERFORM 1
        FROM powa_snapshot_metas
        WHERE srvid = _srvid
        FOR UPDATE NOWAIT;
    EXCEPTION
    WHEN lock_not_available THEN
        RAISE EXCEPTION 'Could not lock the powa_snapshot_metas record, '
        'a concurrent snapshot is probably running';
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT;
        RAISE EXCEPTION 'Failed to lock the powa_snapshot_metas record:
            state  : %
            message: %
            detail : %
            hint   : %
            context: %', v_state, v_msg, v_detail, v_hint, v_context;
    END;
END;
$PROC$ language plpgsql; /* end of powa_prevent_concurrent_snapshot() */

CREATE OR REPLACE FUNCTION powa_qualstats_aggregate_constvalues_current(
    IN _srvid integer,
    IN _ts_from timestamptz DEFAULT '-infinity'::timestamptz,
    IN _ts_to timestamptz DEFAULT 'infinity'::timestamptz,
    OUT srvid integer,
    OUT qualid bigint,
    OUT queryid bigint,
    OUT dbid oid,
    OUT userid oid,
    OUT tstzrange tstzrange,
    OUT mu qual_values[],
    OUT mf qual_values[],
    OUT lf qual_values[],
    OUT me qual_values[],
    OUT mer qual_values[],
    OUT men qual_values[])
RETURNS SETOF record STABLE AS $_$
WITH consts AS (
  SELECT q.srvid, q.qualid, q.queryid, q.dbid, q.userid,
    min(q.ts) as mints, max(q.ts) as maxts,
    sum(q.occurences) as occurences,
    sum(q.nbfiltered) as nbfiltered,
    sum(q.execution_count) as execution_count,
    avg(q.mean_err_estimate_ratio) as mean_err_estimate_ratio,
    avg(q.mean_err_estimate_num) as mean_err_estimate_num,
    q.constvalues
  FROM powa_qualstats_constvalues_history_current q
  WHERE q.srvid = _srvid
  AND q.ts >= _ts_from AND q.ts <= _ts_to
  GROUP BY q.srvid, q.qualid, q.queryid, q.dbid, q.userid, q.constvalues
),
groups AS (
  SELECT c.srvid, c.qualid, c.queryid, c.dbid, c.userid,
    tstzrange(min(c.mints), max(c.maxts),'[]')
  FROM consts c
  GROUP BY c.srvid, c.qualid, c.queryid, c.dbid, c.userid
)
SELECT *
FROM groups,
LATERAL (
  SELECT array_agg(constvalues) as mu
  FROM (
    SELECT (constvalues, occurences, execution_count, nbfiltered,
      mean_err_estimate_ratio, mean_err_estimate_num
    )::qual_values AS constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY occurences desc
    LIMIT 20
  ) s
) as mu,
LATERAL (
  SELECT array_agg(constvalues) as mf
  FROM (
    SELECT (constvalues, occurences, execution_count, nbfiltered,
      mean_err_estimate_ratio, mean_err_estimate_num
    )::qual_values AS constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY CASE WHEN execution_count = 0 THEN 0 ELSE nbfiltered / execution_count::numeric END DESC
    LIMIT 20
  ) s
) as mf,
LATERAL (
  SELECT array_agg(constvalues) as lf
  FROM (
    SELECT (constvalues, occurences, execution_count, nbfiltered,
      mean_err_estimate_ratio, mean_err_estimate_num
    )::qual_values AS constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY CASE WHEN execution_count = 0 THEN 0 ELSE nbfiltered / execution_count::numeric END ASC
    LIMIT 20
  ) s
) as lf,
LATERAL (
  SELECT array_agg(constvalues) as me
  FROM (
    SELECT (constvalues, occurences, execution_count, nbfiltered,
      mean_err_estimate_ratio, mean_err_estimate_num
    )::qual_values AS constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY execution_count desc
    LIMIT 20
  ) s
) as me,
LATERAL (
  SELECT array_agg(constvalues) as mer
  FROM (
    SELECT (constvalues, occurences, execution_count, nbfiltered,
      mean_err_estimate_ratio, mean_err_estimate_num
    )::qual_values AS constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY mean_err_estimate_ratio desc
    LIMIT 20
  ) s
) as mer,
LATERAL (
  SELECT array_agg(constvalues) as men
  FROM (
    SELECT (constvalues, occurences, execution_count, nbfiltered,
      mean_err_estimate_ratio, mean_err_estimate_num
    )::qual_values AS constvalues
    FROM consts
    WHERE consts.qualid = groups.qualid AND consts.queryid = groups.queryid
    AND consts.dbid = groups.dbid AND consts.userid = groups.userid
    ORDER BY mean_err_estimate_num desc
    LIMIT 20
  ) s
) as men;
$_$ LANGUAGE sql; /* end of powa_qualstats_aggregate_constvalues_current */

