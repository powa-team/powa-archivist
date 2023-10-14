-- we can't test API unless there are module config for remote servers
SELECT count(distinct srvid) > 0 AS ok FROM "PoWA".powa_module_config
WHERE srvid != 0;

-- check for missing module config
SELECT pm.module
FROM "PoWA".powa_modules pm
LEFT JOIN "PoWA".powa_module_config pmc USING (module)
LEFT JOIN "PoWA".powa_servers ps ON ps.id = pmc.srvid
WHERE ps.id IS NULL OR pmc.module IS NULL;

-- check that all declared functions have been defined
SELECT pmf.module, pmf.function_name
FROM "PoWA".powa_module_functions pmf
LEFT JOIN pg_proc p ON p.proname = pmf.function_name
LEFT JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE p.proname IS NULL;

-- same for src function
SELECT pmf.module, pmf.query_source
FROM "PoWA".powa_module_functions pmf
LEFT JOIN pg_proc p ON p.proname = pmf.query_source
LEFT JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE pmf.query_source IS NOT NULL AND p.proname IS NULL;
