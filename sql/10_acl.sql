-- Check the relations for which powa_admin is missing ACL

CREATE FUNCTION has_table_or_seq_privilege(relkind "char", rolname text,
                                           relid oid, priv text)
RETURNS bool
AS $$
BEGIN
    IF relkind = 'S' THEN
        RETURN has_sequence_privilege(rolname, relid, priv);
    ELSE
        RETURN has_table_privilege(rolname, relid, priv);
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION check_has_privilege(rolname text,
                                    tbl_priv text[], seq_priv text[])
RETURNS TABLE (powa_role text, relname name, relkind "char", priv text)
AS $$
    WITH ext AS (
        SELECT c.oid, c.relname, c.relkind
        FROM pg_depend d
        JOIN pg_extension e ON d.refclassid = 'pg_extension'::regclass
            AND e.oid = d.refobjid
            AND e.extname = 'powa'
        JOIN pg_class c ON d.classid = 'pg_class'::regclass
            AND c.oid = d.objid
    ),
    acls(priv, isseq) AS (
        SELECT unnest(tbl_priv), false
        UNION ALL
        SELECT unnest(seq_priv), true
    )
    SELECT rolname AS powa_role, relname, relkind, priv
    FROM ext
    JOIN acls ON acls.isseq = (ext.relkind = 'S')
    WHERE NOT has_table_or_seq_privilege(relkind, rolname, ext.oid, priv)
    ORDER BY relname, priv;
$$ LANGUAGE sql;

CREATE FUNCTION check_has_not_privilege(rolname text,
                                        tbl_priv text[], seq_priv text[])
RETURNS TABLE (powa_role text, relname name, relkind "char", priv text)
AS $$
    WITH ext AS (
        SELECT c.oid, c.relname, c.relkind
        FROM pg_depend d
        JOIN pg_extension e ON d.refclassid = 'pg_extension'::regclass
            AND e.oid = d.refobjid
            AND e.extname = 'powa'
        JOIN pg_class c ON d.classid = 'pg_class'::regclass
            AND c.oid = d.objid
    ),
    acls(priv, isseq) AS (
        SELECT unnest(tbl_priv), false
        UNION ALL
        SELECT unnest(seq_priv), true
    )
    SELECT rolname AS powa_role, relname, relkind, priv
    FROM ext
    JOIN acls ON acls.isseq = (ext.relkind = 'S')
    WHERE has_table_or_seq_privilege(relkind, rolname, ext.oid, priv)
    ORDER BY relname, priv;
$$ LANGUAGE sql;


-- powa_admin should have all privileges on all relations
SELECT powa_role, relname, priv
FROM check_has_privilege('powa_admin',
    array ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES',
           'TRIGGER'],
    array ['USAGE', 'SELECT', 'UPDATE']);

-- powa_read_all_data should have SELECT privilege on all relation except
 -- *_src_tmp tables and sequences
SELECT powa_role, relname, priv
FROM check_has_privilege('powa_read_all_data',
    array ['SELECT'],
    array []::text[]);

-- powa_read_all_data should not have non-SELECT privilege on any table, and no
-- privilege on sequences
SELECT powa_role, relname, priv
FROM check_has_not_privilege('powa_read_all_data',
    array ['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'],
    array ['USAGE', 'SELECT', 'UPDATE']);

-- powa_read_all_metrics should be the same as powa_read_all_data except that
-- it can't acceess any pg_qualstats related table
SELECT powa_role, relname, priv
FROM check_has_privilege('powa_read_all_metrics',
    array ['SELECT'],
    array []::text[]);

SELECT powa_role, relname, priv
FROM check_has_not_privilege('powa_read_all_metrics',
    array ['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'],
    array ['USAGE', 'SELECT', 'UPDATE']);

-- powa_write_all_data should have SELECT/INSERT/UPDATE/DELETE/TRUNCATE
-- privileges on all relations (and all privileges on sequences)
SELECT powa_role, relname, priv
FROM check_has_privilege('powa_write_all_data',
    array ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE'],
    array ['USAGE', 'SELECT', 'UPDATE']);

-- powa_write_all_data should not have TRIGGER/REFERENCES privileges on any
-- relations
SELECT powa_role, relname, priv
FROM check_has_not_privilege('powa_write_all_data',
    array ['TRIGGER', 'REFERENCES'],
    array []::text[]);

-- powa_snapshot should have SELECT/INSERT/UPDATE/DELETE/TRUNCATE
-- privileges on all metric-related relations (and all privileges on sequences)
-- only
SELECT powa_role, relname, relkind, array_agg(priv)
FROM check_has_privilege('powa_snapshot',
    array ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE'],
    array ['USAGE', 'SELECT', 'UPDATE'])
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- powa_snapshot should not have TRIGGER/REFERENCES privileges on any relations
SELECT powa_role, relname, priv
FROM check_has_not_privilege('powa_snapshot',
    array ['TRIGGER', 'REFERENCES'],
    array []::text[]);

-- and try to detect any unexpected GRANT on powa_snapshot, as any newly
-- created table will have too many privileges granted unless explicitly
-- handled
SELECT DISTINCT powa_role, relname
FROM check_has_not_privilege('powa_snapshot',
    array ['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE'],
    array ['USAGE', 'SELECT', 'UPDATE'])
WHERE relkind != 'v'
AND relname NOT LIKE '%history'
AND relname NOT LIKE '%history\_db'
AND relname NOT LIKE '%history\_current'
AND relname NOT LIKE '%history\_current\_db'
AND relname NOT LIKE '%src\_tmp'
AND relname NOT LIKE 'powa\_catalog\_%'
AND relname NOT LIKE '%qualstats%'
AND relname NOT LIKE '%kcache%'
AND relname NOT IN ('powa_databases', 'powa_snapshot_metas', 'powa_statements');

-- powa_signal_backend should not have any privilege on any relation
SELECT powa_role, relname, priv
FROM check_has_not_privilege('powa_signal_backend',
    array ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES',
           'TRIGGER'],
    array ['USAGE', 'SELECT', 'UPDATE']);
