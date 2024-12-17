-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

ALTER TABLE @extschema@.powa_module_config DROP COLUMN added_manually;

SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_module_config','');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.powa_module_functions','');

CREATE FUNCTION @extschema@.powa_activate_module(_srvid int, _module text) RETURNS boolean
AS $_$
DECLARE
    v_res bool;
BEGIN
    IF (_srvid IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_module: no server id provided';
    END IF;

    IF (_module IS NULL) THEN
        RAISE EXCEPTION 'powa_activate_module: no module provided';
    END IF;

    -- Check that the module is known.
    SELECT COUNT(*) = 1 INTO v_res
    FROM @extschema@.powa_modules
    WHERE module = _module;

    IF (NOT v_res) THEN
        RAISE EXCEPTION 'Module "%" is not known', _module;
    END IF;

    -- The record may already be present, but the enabled flag could be off.
    -- If so simply enable it.  Otherwise, add the needed record.
    SELECT COUNT(*) > 0 INTO v_res
    FROM @extschema@.powa_module_config
    WHERE module = _module
    AND srvid = _srvid;

    IF (v_res) THEN
        UPDATE @extschema@.powa_module_config
        SET enabled = true
        WHERE enabled = false
        AND srvid = _srvid
        AND module = _module;
    ELSE
        INSERT INTO @extschema@.powa_module_config
            (srvid, module)
        VALUES
            (_srvid, _module);
    END IF;

    RETURN true;
END;
$_$ LANGUAGE plpgsql
SET search_path = pg_catalog; /* end of powa_activate_module */
