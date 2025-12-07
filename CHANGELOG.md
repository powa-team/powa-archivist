## 5.1.1

  - Bugfixes
    - Fix TOAST table size double counting (Marc Cousin)
    - Fix various `pg_stat_io` with pg18+ (Julien Rouhaud)

## 5.1.0

  - Performance
    - Improve indexes on `*_current` tables (Julien Rouhaud)
  - Bugfixes
    - Improve age accuracy for `pg_stat_activity` metrics (Julien Rouhaud,
      thanks to Marc Cousin for the report)
    - Fix powa compatibility with `pg_dump` (Julien Rouhaud)
    - Fix `pg_stat_wal` compatibility with pg18 (Julien Rouhaud, thanks to
      Stefan Le Breton for the report)
    - Fix `pg_stat_io` compatibility with pg18 (Julien Rouhaud)

## 5.0.3

  - Performance
    - Make all history tables have TOAST_TUPLE_TARGET=128 set (Marc Cousin)
  - Bugfixes
    - Fix txid_current() usage when FullXactId > max xid (Julien Rouhaud,
      thanks to github user Nickuru for the repor)
  - Misc
    - Add a new powa_stat_get_activity(srvid, from, to) function (Julien
      Rouhaud, Thanks to Marc Cousin for the request)
## 5.0.2

  - Performance
    - Smooth out the coalesces and purges (Marc Cousin and Julien Rouhaud)
  - Bugfixes
    - Fix DROP EXTENSION issues when powa is installed (Julien Rouhaud, thanks
      to Michael Vitale for the report)
    - Fix aggregation of pg_stat_user_functions datasource (Marc Cousin)
    - Fix pg_stat_replication compatibility on pg12 and below (Julien Rouhaud)
    - Fix powa_delete_and_purge_server to also purge `*_src_tmp tables` (Julien
      Rouhaud, thanks to github user jw1u1 for the report)
  - Misc
    - Fix compatibility with pg18 (Georgy Shelkovy)

## 5.0.1

  - Bugfixes
    - Fix pg_dump error on powa_module_config (Julien Rouhaud, thanks to github
      user guruguruguru for the report)
    - Fix powa_delete_and_purge_server function when pg_track_settings is
      installed (Julien Rouhaud, thanks to Thomas Reiss for report)

## 5.0.0

This is a major rework of PoWA.  Please note that there is no upgrade procedure
to switch to this version. You need to remove the old one and install the new
5.0.0 version.

  - Breaking changes
    - Remove support for postgres 9.4 (Julien Rouhaud)
  - New feature
    - Allow installation of any extensions in any schema (Julien Rouhaud)
    - Introduce snapshot of per-database object (remote mode only) (Julien
      Rouhaud)
    - Introduce snapshot of per-database catalogs (remote mode only) (Julien
      Rouhaud)
    - Add powa_* pseudo predefined roles to ease permissions (Julien Rouhaud)
    - Add pg_stat_activity metrics (Julien Rouhaud)
    - Add pg_stat_archiver metrics (Julien Rouhaud)
    - Add pg_stat_replication metrics (Julien Rouhaud)
    - Add pg_stat_replication_slots metrics (Julien Rouhaud)
    - Add pg_stat_database metrics (Julien Rouhaud)
    - Add pg_stat_io metrics (Julien Rouhaud)
    - Add JIT metrics in pg_stat_statements 1.10 and 1.11
    - Add pg_stat_database_conflicts metrics (Julien Rouhaud)
    - Add pg_stat_slru metrics (Julien Rouhaud)
    - Add pg_stat_wal metrics (Julien Rouhaud)
    - Add pg_stat_wal_receiver metrics (Julien Rouhaud)
    - Add pg_stat_subscription metrics (Julien Rouhaud)
    - Add pg_stat_subscription_stats metrics (Julien Rouhaud)
  - Bugfixes
    - Fix long standing bug in pg_stat_kcache metrics calculation (Julien
      Rouhaud)
  - Misc
    - Add compatibility with postgres 17 (Julien Rouhaud)
    - Remove powa_stat_* handling from powa_functions (Julien Rouhaud)
    - Add compatibility with pg_stat_statements 1.11 (Julien Rouhaud)
    - Improve aggregated record lookup performance (Marc Cousin, Julien Rouhaud)

## 4.2.2

  - Bugfixes
    - Fix the toplevel field position in powa_statements_src function (Julien
      Rouhaud, thanks to Thomas Reiss for the report)

## 4.2.1

  - Bugfixes
    - Fix an issue in 4.1.4 - 4.2.0 extension script that can prevent
      upgrading.  Versions 4.2.0 and 4.2.1 are identical, version 4.2.1 only
      exist to provide a fixed 4.1.4 - 4.2.1 direct ugprade script (Julien
      Rouhaud, thanks to Yuriy Vountesmery for the report)

## 4.2.0

  - New feature
    - Add pg_stat_statements.toplevel field (Marc Cousin, Julien Rouhaud)
  - Bugfixes
    - Fix a possible long waiting time when the background worker is asleep and
      another session is waiting for a pending event, like a DROP DATABASE
      (Julien Rouhaud, thanks for github user anikin-aa for the report)
  - Fix pg_stat_kcache support when not all metrics are available (Julien
    Rouhaud)
  - Misc
    - Add compatibility with postgres 16 (Julien Rouhaud)
    - Immediately exit the bgworker in binary upgrade mode, which could lead to
      data corruption in the powa database (Julien Rouhaud)

## 4.1.4

  - Bugfixes
    - Fix compatibility with standard_conforming_strings = off (Julien Rouhaud,
      per report from github user arnobnq)
  - Misc
    - Add compatibility with postgres 15

## 4.1.3

  - Bugfixes
    - Handle possibly duplicated query in powa_statements_src (Julien Rouhaud,
      per report from github user gjedeer)
    - Fix column name and ORDER BY in
      powa_qualstats_aggregate_constvalues_current function (Adrien Nayrat)
    - Fix powa_kcache_src compatibility with pgsk 2.1- (Julien Rouhaud, per
      report from github user hrawulwa)
    - Fix powa_wait_sampling_unregister() to accept a server id (Julien
      Rouhaud)
  - Performance improvement
    - Rewrite powa_qualstats_aggregate_constvalues_current with Window
      Function, making those at least twice as fast (Adrien Nayrat)
  - Misc
    - Narrow the error condition in powa_prevent_concurrent_snapshot() (Denis
      Laxalde)
    - Make sure that GUC won't leak the extension script (Julien Rouhaud)
    - Don't rely on public being in search_path in event trigger code (Julien
      Rouhaud)
## 4.1.2

  - Bugfixes
    - Fix remote server removal by adding ON UPDATE / ON DELETE CASCADE clause
      for the powa_extensions foreign keys.  Thanks to Andriy Bartash for the
      report.
    - Fix pg_qualstats snapshot.  Thanks to github user alepaes1975 for the
      report.
    - Clear `*_src_tmp` tables on reset operation.  Those tables are supposed
      to be empty, but if anything goes wrong and those tables contains
      problematic data, it could prevent users from cleaning up the situation.
      Thanks to Marc Cousin for the report.

## 4.1.1

  - Bugfixes
    - Fix regression tests for version 4.1 (Christoph Berg)

## 4.1.0

  - New features:
    - Add compatibility with pg_stat_statements 1.8 / pg13 (Julien Rouhaud)
    - Clean up statements that haven't been executed during the configured
      retention interval (Andriy Bartash)
    - Store the postgres and external extension versios in PoWA catalog (Julien
      Rouhaud)
    - Add compatibility with pg_stat_kcache 2.2 (Julien Rouhaud)
    - Don't require to load 'powa' anymore (Julien Rouhaud)
  - Bugfixes
    - Ignore quals that don't match powa_statements row during snapshot.  This
      fixes pg_qualstats foreign key errors that can happen during snapshots
      (Julien Rouhaud)
    - Fix pg_wait_sampling counters if multiple users run the same queries
      (Marc Cousin and Julien Rouhaud)

## 4.0.1

  - Bugfixes
    - Fix typo in powa_all_relations_history_db_div
    - Fix regression tests

## 4.0.0

This is a major rework of PoWA.  Please note that there is no upgrade procedure
to switch to this version. You need to remove the old one and install the new
4.0.0 version.

  - New features:
    - Add a remote capture mode, allowing to gather data from a remote server
      and store them on a central repository.  This avoids the overhead of
      storig and processing performance data on the local instance, and also
      allows using PoWA on hot-standby server (Julien Rouhaud, Thanks to Adrien
      Nayrat for extensive testing).
    - Store new metrics added in pg_stat_kcache 2.1.0 (Julien Rouhaud)
    - Aggregate relation statistics per database (Alexander Kukushkin)
    - Add support for pg_qualstats 2.0.0 (Julien Rouhaud)
    - Add a query_cleanup column to powa_functions (Julien Rouhaud)
  - Miscellaneous:
    - Add support for makefile option NO_PGXS (Julien Rouhaud)
    - Cleanup old databases after the expiration period, and stop gathering
      data belonging to dropped database when doing the snapshots (Marc Cousin)
    - Fix possible bug with background worker type in pg_stat_activity
      (github user ppetrov91)
    - Add some missing indexes (Julien Rouhaud, thanks to PoWA for noticing)
    - Add compatibility with upcoming pg13 (Julien Rouhaud)
    - Reduce noise is powa is disabled and the target database doesn't exist
      (Julien Rouhaud)
  - Bugfix
    - Schema qualify powa_take_snapshot() call, so powa can work without public
      being in the superuser search_path (Julien Rouhaud)
    - Fix powa_snapshot_metas dump config (Julien Rouhaud, reported by Adrien
      Nayrat)
    - Fix long standing bug in pg_qualstats aggregation
    - Fix typos in SQL comments (Magnus Hagander)

## 3.2.0 (2018-10-14)

  - New features:
    - Add support for pg_wait_sampling extension (Julien Rouhaud)
  - Miscellaneous:
    - Reduce logs when PoWA is deactivated (Julien Rouhaud)
  - Bugfix:
    - Fix possible bug if an error happens during the stats retrieval (Julien
      Rouhaud)

## 3.1.2 (2018-05-30)

  - Miscellaneous:
    - Add pg11 compatibility (Julien Rouhaud)
    - Catch errors in Debian packaging test script (Christoph Berg, spotted by
      Niels Thykier)

## 3.1.1 (2017-09-19)

  - Bugfix:
    - Fix unsafe coding with sighup handler (Andreas Seltenreich, Julien
      Rouhaud)
    - Make sure we wait at least powa.frequency between two snapshot (Marc Cousin
      and Julien Rouhaud)
    - Fix win32 portability of compute_powa_frequeny() (Julien Rouhaud)
    - Don't try to read dbentry->tables if it's NULL (Julien Rouhaud)
    - Fix compilation for platform with HAVE_CLOCK_GETTIME (Julien Rouhaud,
      reported by Maxence Ahlouche)
  - Miscellaneous:
    - Add pg10 Compatibility (Julien Rouhaud)
    - Only execute once the powa_stat functions (Julien Rouhaud)

## 3.1.0 (2016-07-29)

  - Fix issue leading to impossibility to stop the worker without shutting down
    the database
  - Fix cluster wide statistics to get fresh values
  - Report PoWA collector activity in pg_stat_activity and process title
  - add a new powa.debug parameter
  - Purge at the same frequency as we coalesce. We just don't do both at the same iteration
  - Fix bloat issue
  - Add + and / operators on powa types to get delta and counters per second
    given two records

## 3.0.1 (2016-02-09)

  - Don't track 2PC related statements, as they're not normalized by
    pg_stat_statements. Upgrade script will do all the needed cleanup.
  - Restore the install_all.sql file to easily setup PoWA.
  - Maintain a cache of pg_database to allow seeing dropped database in the UI.
    See issue https://github.com/powa-team/powa/issues/63
  - Don't try to load PoWA if it's not in shared_preload_libraries

## 3.0.0 (2015-11-06)

This is a major rework of PoWA.  Please note that there is no upgrade procedure
to switch to this version. You need to remove the old one and install the new
3.0.0 version.

  - Handle pg_qualtats 0.0.7
  - Sample cluster wide statistics, for relations and functions
  - Fix the powa reset function, and rename it to powa_reset()
  - Add min/max records to improve performance when analyzing big time interval
  - Allow disabling some statistics sampling
  - Handle pg_track_settings extension
  - Add a GUC to ignore some users activity in sampled data

## 2.0.1 (2015-07-27)

  - Handle creation/suppression of supported extensions.
  - Remove the install_all script

## 2.0 (2015-02-06)

Major rework of the extension. PoWA 2 is now only compatible with PostgreSQL
version 9.4 and above. PoWA 2 is also now compatible with external extensions,
such as [pg_qualstats](https://github.com/powa-team/pg_qualstats) or
[pg_stat_kcache](https://github.com/powa-team/pg_stat_kcache). Third-part
extensions can also now be implemented easily.

The UI is also now in a [new repository](https://github.com/powa-team/powa-web),
with more frequent release cycle.

## 1.2.1 (2015-01-16)

No changes in core.

New features and changes in UI :
  - UI is now compatible with mojolicious 5.0 and more
  - UI can now connect to multiple servers, and credentials can be specified for each server
  - Use ISO 8601 timestamp format
  - Add POWA_CONFIG_FILE variable to specify config file location
  - Better charts display on small screens

When upgrading from 1.2:
  - No change on the extension
  - the format of the database section of the powa.conf has changed, to allow multiple servers specification. Please read INSTALL.md for more details about it.

## 1.2 (2014-10-27)

News features and fixes in core :
  - Display more metrics : temporary data, I/O time, average runtime
  - Fix timestamp for snapshots
  - DEALLOCATE and BEGIN statements are now ignored
  - PoWA history tables are now marked as "to be dumped" by pg_dump
  - Improve performance for "per database aggregated stats"

News features and changes in UI :
  - Follow the selected time interval between each page
  - Add a title to each page
  - Display metrics for each query page
  - Move database selector as a menu entry
  - Display human readable metrics
  - Fix empty graph bug

When upgrading from older versions :
  - Upgrade the core with ALTER EXTENSION powa UPDATE.
  - The format of the database section of the powa.conf has changed. The new format is :

     "dbname"   : "powa",
     "host"     : "127.0.0.1",
     "port"     : "5432",

 (instead of one line containing the dbi:Pg connection info)


## 1.1 (2014-08-18)

**POWA is now production ready**

Features:

  - Various UI improvments
  - More documentation
  - New demo mode
  - Plugin support
  - The code is now under the PostgreSQL license
  - New website
  - New logo

Bug fixes:

  - Use a temporary table for unpacked records to avoid unnecessary bloat


## 1.0 (2014-06-13)

**Hello World ! This is the first public release of POWA**

Features:

  - Web UI based on Mojolicious
  - Graph and dynamic charts
  - Packed the code as an extension
  - PL functions

