-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

DROP FUNCTION powa_log(text);
DO $anon$
BEGIN
    IF current_setting('server_version_num')::int < 90600 THEN
        CREATE FUNCTION powa_log (msg text) RETURNS void
        LANGUAGE plpgsql
        AS $_$
        DECLARE
            v_debug bool;
        BEGIN
            BEGIN
                SELECT current_setting('powa.debug')::bool INTO v_debug;
            EXCEPTION WHEN OTHERS THEN
                v_debug = false;
            END;
            IF v_debug THEN
                RAISE WARNING '%', msg;
            ELSE
                RAISE DEBUG '%', msg;
            END IF;
        END;
        $_$;
    ELSE
        CREATE FUNCTION powa_log (msg text) RETURNS void
        LANGUAGE plpgsql
        AS $_$
        BEGIN
            IF COALESCE(current_setting('powa.debug', true), 'off')::bool THEN
                RAISE WARNING '%', msg;
            ELSE
                RAISE DEBUG '%', msg;
            END IF;
        END;
        $_$;
    END IF;
END;
$anon$;

