-- General setup
\set SHOW_CONTEXT never

-- Check for local server reset
SELECT "PoWA".powa_reset(0);

-- check catalog FK
DO
$_$
DECLARE
    v_dbid oid;
    v_nb integer;
    v_catname text;
    v_prefix text;
BEGIN
    SELECT oid INTO v_dbid FROM "PoWA".powa_catalog_databases
    WHERE srvid = 1 AND datname = current_database();

    DELETE FROM "PoWA".powa_catalog_databases
    WHERE srvid = 1 AND datname = current_database();
    FOR v_catname IN SELECT catname FROM "PoWA".powa_catalogs
    LOOP
        -- get the necessary object name
        SELECT 'powa_catalog_' || replace(v_catname, 'pg_', '') INTO v_prefix;

        -- There shouldn't be any row left for that databa in any catalog
        EXECUTE format('SELECT count(*) FROM "PoWA".%I WHERE dbid = %s',
            v_prefix, v_dbid) INTO v_nb;
        IF v_nb != 0 THEN
            RAISE WARNING 'table "PoWA".% for catalog % has % rows',
                v_prefix, v_catname, v_nb;
        END IF;

        -- but there should be record in the src_tmp tables
        EXECUTE format('SELECT count(*) FROM "PoWA".%I', v_prefix) INTO v_nb;
        IF v_nb = 0 THEN
            RAISE WARNING 'table "PoWA".% for catalog % has % rows',
                v_prefix || '_src_tmp', v_catname, v_nb;
        END IF;
    END LOOP;
END;
$_$ LANGUAGE plpgsql;

SELECT "PoWA".powa_reset(1);

-- There shouldn't be any row left for that server in any catalog
DO
$_$
DECLARE
    v_nb integer;
    v_catname text;
    v_prefix text;
BEGIN
    FOR v_catname IN SELECT catname FROM "PoWA".powa_catalogs
    LOOP
        -- get the necessary object name
        SELECT 'powa_catalog_' || replace(v_catname, 'pg_', '') INTO v_prefix;

        EXECUTE format('SELECT count(*) FROM "PoWA".%I', v_prefix) INTO v_nb;
        IF v_nb != 0 THEN
            RAISE WARNING 'table "PoWA".% for catalog % has % rows',
                v_prefix, v_catname, v_nb;
        END IF;

        EXECUTE format('SELECT count(*) FROM "PoWA".%I', v_prefix) INTO v_nb;
        IF v_nb != 0 THEN
            RAISE WARNING 'table "PoWA".% for catalog % has % rows',
                v_prefix || '_src_tmp', v_catname, v_nb;
        END IF;
    END LOOP;
END;
$_$ LANGUAGE plpgsql;

-- Check remote server removal
DELETE FROM "PoWA".powa_servers WHERE id = 1;

-- dropping powa will leave the role initially created, and we should be able
-- to reinstall powa again
DROP EXTENSION powa;
CREATE EXTENSION powa WITH SCHEMA "PoWA";

-- we shouldn't assigned the existing role to our pseudo predefined role, even
-- if they exist with the default name
SELECT * FROM "PoWA".powa_roles WHERE rolname IS NOT NULL;

-- and also shouldn't put back any ACL on the previous roles
SELECT has_table_privilege('powa_admin', '"PoWA".powa_servers', 'SELECT');

-- We shouldn't be able to grant or revoke privileges in that situation
SELECT "PoWA".powa_revoke();
SELECT "PoWA".powa_grant();

-- we should be able to reuse the initially created roles
SELECT "PoWA".setup_powa_roles(true);
SELECT has_table_privilege('powa_admin', '"PoWA".powa_servers', 'SELECT');

-- cleanup ACL and remove the powa pseudo predefined roles
SELECT "PoWA".powa_revoke();
SELECT has_table_privilege('powa_admin', '"PoWA".powa_servers', 'SELECT');
DROP ROLE powa_admin;
DROP ROLE powa_read_all_data;
DROP ROLE powa_read_all_metrics;
DROP ROLE powa_write_all_data;
DROP ROLE powa_snapshot;
DROP ROLE powa_signal_backend;
