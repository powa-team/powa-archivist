-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET client_min_messages = warning;
SET escape_string_warning = off;
SET search_path = public, pg_catalog;

CREATE OR REPLACE FUNCTION powa_take_snapshot() RETURNS void AS $PROC$
DECLARE
  purgets timestamp with time zone;
  purge_seq bigint;
  funcname text;
  v_state   text;
  v_msg     text;
  v_detail  text;
  v_hint    text;
  v_context text;
  v_title   text = 'PoWA - ';

BEGIN
    PERFORM set_config('application_name',
        v_title || ' snapshot database list',
        false);

    -- Keep track of existing databases
    WITH missing AS (
        SELECT d.oid, d.datname
        FROM pg_database d
        LEFT JOIN powa_databases p ON d.oid = p.oid
        WHERE p.oid IS NULL
    )
    INSERT INTO powa_databases
    SELECT * FROM missing;

    -- Keep track of renamed databases
    WITH renamed AS (
        SELECT d.oid, d.datname
        FROM pg_database AS d
        JOIN powa_databases AS p ON d.oid = p.oid
        WHERE d.datname != p.datname
    )
    UPDATE powa_databases AS p
    SET datname = r.datname
    FROM renamed AS r
    WHERE p.oid = r.oid;

    -- Keep track of when databases are dropped
    WITH dropped AS (
        SELECT p.oid
        FROM powa_databases p
        LEFT JOIN pg_database d ON p.oid = d.oid
        WHERE d.oid IS NULL
        AND p.dropped IS NULL)
    UPDATE powa_databases p
    SET dropped = now()
    FROM dropped d
    WHERE p.oid = d.oid;

    -- For all enabled snapshot functions in the powa_functions table, execute
    FOR funcname IN SELECT function_name
                 FROM powa_functions
                 WHERE operation='snapshot' AND enabled LOOP
      -- Call all of them, with no parameter
      RAISE debug 'fonction: %',funcname;
      BEGIN
        PERFORM set_config('application_name',
            v_title || quote_ident(funcname) || '()',
            false);
        EXECUTE 'SELECT ' || quote_ident(funcname)||'()';
      EXCEPTION
        WHEN OTHERS THEN
          GET STACKED DIAGNOSTICS
              v_state   = RETURNED_SQLSTATE,
              v_msg     = MESSAGE_TEXT,
              v_detail  = PG_EXCEPTION_DETAIL,
              v_hint    = PG_EXCEPTION_HINT,
              v_context = PG_EXCEPTION_CONTEXT;
          RAISE warning 'powa_take_snapshot(): function "%" failed:
              state  : %
              message: %
              detail : %
              hint   : %
              context: %', funcname, v_state, v_msg, v_detail, v_hint, v_context;

      END;
    END LOOP;

    -- Coalesce datas if needed
    SELECT nextval('powa_coalesce_sequence'::regclass) INTO purge_seq;

    IF (  purge_seq
            % current_setting('powa.coalesce')::bigint ) = 0
    THEN
      FOR funcname IN SELECT function_name
                   FROM powa_functions
                   WHERE operation='aggregate' AND enabled LOOP
        -- Call all of them, with no parameter
        BEGIN
          PERFORM set_config('application_name',
              v_title || quote_ident(funcname) || '()',
              false);
          EXECUTE 'SELECT ' || quote_ident(funcname)||'()';
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;
            RAISE warning 'powa_take_snapshot(): function "%" failed:
                state  : %
                message: %
                detail : %
                hint   : %
                context: %', funcname, v_state, v_msg, v_detail, v_hint, v_context;

        END;
      END LOOP;
      UPDATE powa_last_aggregation SET aggts = now();
    END IF;
    -- Once every 10 packs, we also purge
    IF (  purge_seq
            % (current_setting('powa.coalesce')::bigint *10) ) = 0
    THEN
      FOR funcname IN SELECT function_name
                   FROM powa_functions
                   WHERE operation='purge' AND enabled LOOP
        -- Call all of them, with no parameter
        BEGIN
          PERFORM set_config('application_name',
              v_title || quote_ident(funcname) || '()',
              false);
          EXECUTE 'SELECT ' || quote_ident(funcname)||'()';
        EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_state   = RETURNED_SQLSTATE,
                v_msg     = MESSAGE_TEXT,
                v_detail  = PG_EXCEPTION_DETAIL,
                v_hint    = PG_EXCEPTION_HINT,
                v_context = PG_EXCEPTION_CONTEXT;
            RAISE warning 'powa_take_snapshot(): function "%" failed:
                state  : %
                message: %
                detail : %
                hint   : %
                context: %', funcname, v_state, v_msg, v_detail, v_hint, v_context;

        END;
      END LOOP;
      PERFORM set_config('application_name',
          v_title || 'UPDATE powa_last_purge',
          false);
      UPDATE powa_last_purge SET purgets = now();
    END IF;
    PERFORM set_config('application_name',
        v_title || 'snapshot finished',
        false);
END;
$PROC$ LANGUAGE plpgsql;
