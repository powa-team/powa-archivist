/*-------------------------------------------------------------------------
 *
 * powa.c: PoWA background worker
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if PG_VERSION_NUM < 90400
#error "PoWA requires PostgreSQL 9.4 or later"
#endif

/* For a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* Access a database */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

/* Some catalog elements */
#include "catalog/pg_type.h"
#include "utils/timestamp.h"

/* There is a GUC */
#include "utils/guc.h"

/* We use tuplestore */
#include "funcapi.h"

/* pgsats access */
#include "pgstat.h"

/* rename process */
#include "utils/ps_status.h"

PG_MODULE_MAGIC;

#define POWA_STAT_FUNC_COLS	4	/* # of cols for functions stat SRF */
#define POWA_STAT_TAB_COLS	21	/* # of cols for relations stat SRF */
#define MIN_POWA_FREQUENCY	5000 /* minimum ms between two snapshots */

typedef enum
{
	POWA_STAT_FUNCTION,
	POWA_STAT_TABLE
}	PowaStatKind;

void			_PG_init(void);
static bool		powa_check_frequency_hook(int *newval, void **extra, GucSource source);
static void		compute_powa_frequency(void);
static int64	compute_next_wakeup(void);

Datum		powa_stat_user_functions(PG_FUNCTION_ARGS);
Datum		powa_stat_all_rel(PG_FUNCTION_ARGS);
static Datum powa_stat_common(PG_FUNCTION_ARGS, PowaStatKind kind);

PG_FUNCTION_INFO_V1(powa_stat_user_functions);
PG_FUNCTION_INFO_V1(powa_stat_all_rel);

#if (PG_VERSION_NUM >= 90500)
PGDLLEXPORT void powa_main(Datum main_arg) pg_attribute_noreturn();
#else
PGDLLEXPORT void powa_main(Datum main_arg) __attribute__((noreturn));
#endif

static void powa_sighup(SIGNAL_ARGS);
static void powa_process_sighup(void);

static instr_time	last_start;					/* last snapshot start */

static int			powa_frequency;				/* powa.frequency GUC */
static instr_time	time_powa_frequency;		/* same in instr_time format */
static bool			force_snapshot = false;		/* used to force snapshot after enabling powa */
static int			powa_retention;				/* powa.retention GUC */
static int			powa_coalesce;			 	/* powa.coalesce GUC */
static char		   *powa_database = NULL;	 	/* powa.database GUC */
static char 	   *powa_ignored_users = NULL;	/* powa.ignored_users GUC */
static bool			powa_debug = false;			/* powa.debug GUC */

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;

static bool
powa_check_frequency_hook(int *newval, void **extra, GucSource source)
{
	if (*newval >= MIN_POWA_FREQUENCY || *newval == -1)
		return true;

	return false;
}

static void
compute_powa_frequency(void)
{
	int local_frequency = powa_frequency;

	/*
	 * If PoWA is deactivated, arbitrarily set a sleep time of one hour to save
	 * resources.  The actual sleep time is not problematic since the
	 * reactivation can only be done with a sighup, which will set the latch
	 * used for the sleep.
	 */
	if (powa_frequency == -1)
		local_frequency = 3600000;

	/* Initialize time_powa_frequency to do maths with it */
#ifndef WIN32
	INSTR_TIME_SET_ZERO(time_powa_frequency);
	time_powa_frequency.tv_sec = local_frequency / 1000; /* Seconds */
#else
	time_powa_frequency.QuadPart = local_frequency / 1000 * GetTimerFrequency();
#endif
}

static int64
compute_next_wakeup(void)
{
	instr_time time_to_wait, now;

	/*
	 * If powa was deactivated and is now reactivated, we force an immediate
	 * snapshot, and start usual interval sleep.  In order to do that, we have
	 * to setup a new last_start reference to now minus powa_frequency so that
	 * the next calls to this function will return expected sleep time.
	 */
	if (force_snapshot)
	{
		force_snapshot = false;
		INSTR_TIME_SET_CURRENT(last_start);
		INSTR_TIME_SUBTRACT(last_start, time_powa_frequency);
		return 0;
	}

	memcpy(&time_to_wait, &last_start, sizeof(last_start));
	INSTR_TIME_ADD(time_to_wait, time_powa_frequency);
	INSTR_TIME_SET_CURRENT(now);
	INSTR_TIME_SUBTRACT(time_to_wait, now);

	return INSTR_TIME_GET_MICROSEC(time_to_wait);
}

/*
 * As of powa 4, this extension can be with a remote snapshot daemon instead of
 * the dedicated background worker.  In order to allow this daemon to use the
 * statistic functions powa provide, we now allow to load the extension from
 * outside shared_preload_libraries.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/*
	 * This setting can be used for datasource function, so we always define
	 * this GUC
	 */
	DefineCustomStringVariable("powa.ignored_users",
							   "Defines a coma-separated list of users to ignore when taking activity snapshot",
							   NULL,
							   &powa_ignored_users,
							   NULL, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("powa.debug",
							   "Provide logs to help troubleshooting issues",
							   NULL,
							   &powa_debug,
							   false, PGC_USERSET, 0, NULL, NULL, NULL);

	/*
	 * The rest of the GUCs are not required when the bgworker isn't active,
	 * but it can be useful when manually calling powa_take_snapshot(), and
	 * defining them doesn't hurt anyway
	 */
	DefineCustomIntVariable("powa.frequency",
						 "Defines the frequency in seconds of the snapshots",
							NULL,
							&powa_frequency,
							300000,
							-1,
							INT_MAX / 1000,
							PGC_SUSET, GUC_UNIT_MS,
							powa_check_frequency_hook,
							NULL,
							NULL);

	DefineCustomIntVariable("powa.coalesce",
							"Defines the amount of records to group together in the table (more compact)",
							NULL,
							&powa_coalesce,
							100, 5, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("powa.retention",
							"Automatically purge data older than N minutes",
							NULL,
							&powa_retention,
							HOURS_PER_DAY * MINS_PER_HOUR,
							0,
							INT_MAX / SECS_PER_MINUTE,
							PGC_SUSET, GUC_UNIT_MIN, NULL, NULL, NULL);

	EmitWarningsOnPlaceholders("powa");

	/*
	 * Following code is only needed for the bgworker, so only used when powa
	 * is loaded in shared_preload_libraries.
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("powa.database",
						   "Defines the database of the workload repository",
							   NULL,
							   &powa_database,
							   "powa", PGC_POSTMASTER, 0, NULL, NULL, NULL);

	/*
	 * Register the worker processes
	 */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;		/* Must write to the
																 * database */
#if (PG_VERSION_NUM >= 100000)
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "powa");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "powa_main");
#else
	worker.bgw_main = powa_main;
#endif
	snprintf(worker.bgw_name, BGW_MAXLEN, "powa");
	worker.bgw_restart_time = 10;
	worker.bgw_main_arg = (Datum) 0;
#if (PG_VERSION_NUM >= 90400)
	worker.bgw_notify_pid = 0;
#endif
	RegisterBackgroundWorker(&worker);
}


void
powa_main(Datum main_arg)
{
	char	   *query_snapshot = "SELECT public.powa_take_snapshot()";
	static char *query_appname = "SET application_name = 'PoWA - collector'";
	int64		us_to_wait; /* Should be uint64 per postgresql's spec, but we
							   may have negative result, in our tests */

	/* check powa_frequency validity, and if powa is enabled */
	compute_powa_frequency();

	/*
	 * Set up signal handler, then unblock signals
	 */
	pqsignal(SIGHUP, powa_sighup);

	BackgroundWorkerUnblockSignals();

	if (powa_frequency == -1)
	{
		elog(LOG, "PoWA is deactivated");
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	/*
	 * First check if local snapshot is disabled, and sleep at that point if
	 * that's the case.  This avoids spurious logging if powa is in
	 * shared_preload_libraries but the target database hasn't been created.
	 */
	while (powa_frequency == -1)
	{
		/* Check if a SIGHUP has been received */
		powa_process_sighup();

		if (powa_frequency != -1)
			break;

		/* sleep */
		WaitLatch(&MyProc->procLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
				  3600000
#if PG_VERSION_NUM >= 100000
				  ,PG_WAIT_EXTENSION
#endif
				  );
		ResetLatch(&MyProc->procLatch);
	}

	/* Define the snapshot reference time when the bgworker start */
	INSTR_TIME_SET_CURRENT(last_start);

	/* Connect to POWA database */
#if PG_VERSION_NUM >= 110000
	BackgroundWorkerInitializeConnection(powa_database, NULL, 0);
#else
	BackgroundWorkerInitializeConnection(powa_database, NULL);
#endif

	elog(LOG, "POWA connected to database %s", quote_identifier(powa_database));

	set_ps_display("init"
#if PG_VERSION_NUM < 130000
			, false
#endif
			);
	StartTransactionCommand();
	SetCurrentStatementStartTimestamp();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, query_appname);
	SPI_execute(query_appname, false, 0);
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	set_ps_display("idle"
#if PG_VERSION_NUM < 130000
			, false
#endif
			);

	/*------------------
	 * Main loop of POWA
	 * We exit from here if:
	 *	 - we got a SIGINT (default bgworker sig handlers)
	 *	 - powa.frequency becomes < 0 (change config and SIGHUP)
	 */
	for (;;)
	{
		/* Check if a SIGHUP has been received */
		powa_process_sighup();

		if (powa_frequency != -1)
		{
			set_ps_display("snapshot"
#if PG_VERSION_NUM < 130000
			, false
#endif
			);
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());
			pgstat_report_activity(STATE_RUNNING, query_snapshot);
			SPI_execute(query_snapshot, false, 0);
			pgstat_report_activity(STATE_RUNNING, query_appname);
			SPI_execute(query_appname, false, 0);
			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
			pgstat_report_stat(false);
			pgstat_report_activity(STATE_IDLE, NULL);
			set_ps_display("idle"
#if PG_VERSION_NUM < 130000
			, false
#endif
			);
		}

		/* sleep loop */
		for (;;)
		{
			StringInfoData buf;

			/* Check if a SIGHUP has been received */
			powa_process_sighup();

			/*
			 * Compute if there is still some time to wait (we could have been
			 * woken up by a latch, or snapshot took more than frequency)
			 */
			us_to_wait = compute_next_wakeup();
			if (us_to_wait <= 0)
				break;

			/* Tell the world we are waiting */
			elog(DEBUG1, "Waiting for %li milliseconds", us_to_wait/1000);
			initStringInfo(&buf);
			appendStringInfo(&buf, "-- sleeping for %li seconds",
							 us_to_wait / 1000000);
			pgstat_report_activity(STATE_IDLE, buf.data);
			pfree(buf.data);

			/* sleep */
			WaitLatch(&MyProc->procLatch,
					  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					  us_to_wait/1000
#if PG_VERSION_NUM >= 100000
					  ,PG_WAIT_EXTENSION
#endif
					  );
			ResetLatch(&MyProc->procLatch);
		} /* end of sleep loop */

		/*
		 * We've stop waiting. Let's increment the snapshot reference time to
		 * it's ideal target, not to now, so errors don't add up
		 */
		INSTR_TIME_ADD(last_start, time_powa_frequency);
	} /* end of snapshot loop */
}


/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, do sanity
 *		check, recompute the frequency and set our latch to wake it up.
 */
static void
powa_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Do all the needed work if a SIGHUP has been received
 *		- reread the config file
 *		- exit the bgworker if the frequency is invalid
 *		- compute the time_powa_frequency var
 */
static void
powa_process_sighup(void)
{
	if (got_sighup)
	{
		int old_powa_frequency = powa_frequency;

		got_sighup = false;

		ProcessConfigFile(PGC_SIGHUP);
		/* setup force_snapshot if powa is reactivated */
		if (old_powa_frequency == -1 && powa_frequency != -1)
		{
			elog(LOG, "PoWA is activated");
			force_snapshot = (old_powa_frequency == -1 && powa_frequency != -1);
		}
		else if (old_powa_frequency != -1 && powa_frequency == -1)
			elog(LOG, "PoWA is deactivated");

		compute_powa_frequency();
	}
}

Datum
powa_stat_user_functions(PG_FUNCTION_ARGS)
{
	return powa_stat_common(fcinfo, POWA_STAT_FUNCTION);
}

Datum
powa_stat_all_rel(PG_FUNCTION_ARGS)
{
	return powa_stat_common(fcinfo, POWA_STAT_TABLE);
}

static Datum
powa_stat_common(PG_FUNCTION_ARGS, PowaStatKind kind)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
#if PG_VERSION_NUM < 150000
	Oid			dbid = PG_GETARG_OID(0);
	Oid			backend_dbid;
	PgStat_StatDBEntry *dbentry;
	HASH_SEQ_STATUS hash_seq;
#endif

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

#if PG_VERSION_NUM < 150000
	/* -----------------------------------------------------
	 * Force deep statistics retrieval of specified database.
	 *
	 * Deep means to also include tables and functions HTAB, which is what we
	 * want here.
	 *
	 * The stat collector isn't suppose to act this way, since a backend can't
	 * access data outside the database it's connected to.  It's not a problem
	 * here since we only need the identifier that are stored in the pgstats,
	 * the UI will connect to the database to do the lookup.
	 *
	 * So, to ensure we'll have fresh statitics of the wanted database, we have
	 * to do following (ugly) tricks:
	 *
	 * - clear the current statistics cache. If a previous function already
	 *	 asked for statistics in the same transaction, calling
	 *	 pgstat_fetch_stat_dbentry would() just return the cache, which would
	 *	 probably belong to another database. As the powa snapshot works inside
	 *	 a function, we have the guarantee that this function will be called for
	 *	 all the databases in a single transaction anyway.
	 *
	 * - change the global var MyDatabaseId to the wanted databaseid. pgstat
	 *	 is designed to only retrieve statistics for current database, so we
	 *	 need to fool it.
	 *
	 * - call pgstat_fetch_stat_dbentry().
	 *
	 * - restore MyDatabaseId.
	 *
	 * - and finally clear again the statistics cache, to make sure any further
	 *	 statement in the transaction will see the data related to the right
	 *	 database.
	 *
	 *	 The pgstat_fetch_stat_dbentry() has to be done inside a PG_TRY bloc so
	 *	 we make sure that MyDatabaseId is restored and the statistics cache is
	 *	 cleared if an error happens during the call.
	 */

	pgstat_clear_snapshot();

	backend_dbid = MyDatabaseId;
	MyDatabaseId = dbid;

	PG_TRY();
	{
		dbentry = pgstat_fetch_stat_dbentry(dbid);
	}
	PG_CATCH();
	{
		MyDatabaseId = backend_dbid;
		pgstat_clear_snapshot();

		PG_RE_THROW();
	}
	PG_END_TRY();

	MyDatabaseId = backend_dbid;

	if (dbentry != NULL && dbentry->functions != NULL &&
		dbentry->tables != NULL)
	{
		switch (kind)
		{
			case POWA_STAT_FUNCTION:
				{
					PgStat_StatFuncEntry *funcentry = NULL;

					hash_seq_init(&hash_seq, dbentry->functions);
					while ((funcentry = hash_seq_search(&hash_seq)) != NULL)
					{
						Datum		values[POWA_STAT_FUNC_COLS];
						bool		nulls[POWA_STAT_FUNC_COLS];
						int			i = 0;

						memset(values, 0, sizeof(values));
						memset(nulls, 0, sizeof(nulls));

						values[i++] = ObjectIdGetDatum(funcentry->functionid);
						values[i++] = Int64GetDatum(funcentry->f_numcalls);
						values[i++] = Float8GetDatum(((double) funcentry->f_total_time) / 1000.0);
						values[i++] = Float8GetDatum(((double) funcentry->f_self_time) / 1000.0);

						Assert(i == POWA_STAT_FUNC_COLS);

						tuplestore_putvalues(tupstore, tupdesc, values, nulls);
					}
					break;
				}
			case POWA_STAT_TABLE:
				{
					PgStat_StatTabEntry *tabentry = NULL;

					hash_seq_init(&hash_seq, dbentry->tables);
					while ((tabentry = hash_seq_search(&hash_seq)) != NULL)
					{
						Datum		values[POWA_STAT_TAB_COLS];
						bool		nulls[POWA_STAT_TAB_COLS];
						int			i = 0;

						memset(values, 0, sizeof(values));
						memset(nulls, 0, sizeof(nulls));

						/* Oid of the table (or index) */
						values[i++] = ObjectIdGetDatum(tabentry->tableid);

						values[i++] = Int64GetDatum((int64) tabentry->numscans);

						values[i++] = Int64GetDatum((int64) tabentry->tuples_returned);
						values[i++] = Int64GetDatum((int64) tabentry->tuples_fetched);
						values[i++] = Int64GetDatum((int64) tabentry->tuples_inserted);
						values[i++] = Int64GetDatum((int64) tabentry->tuples_updated);
						values[i++] = Int64GetDatum((int64) tabentry->tuples_deleted);
						values[i++] = Int64GetDatum((int64) tabentry->tuples_hot_updated);

						values[i++] = Int64GetDatum((int64) tabentry->n_live_tuples);
						values[i++] = Int64GetDatum((int64) tabentry->n_dead_tuples);
						values[i++] = Int64GetDatum((int64) tabentry->changes_since_analyze);

						values[i++] = Int64GetDatum((int64) (tabentry->blocks_fetched - tabentry->blocks_hit));
						values[i++] = Int64GetDatum((int64) tabentry->blocks_hit);

						/* last vacuum */
						if (tabentry->vacuum_timestamp == 0)
							nulls[i++] = true;
						else
							values[i++] = TimestampTzGetDatum(tabentry->vacuum_timestamp);
						values[i++] = Int64GetDatum((int64) tabentry->vacuum_count);

						/* last_autovacuum */
						if (tabentry->autovac_vacuum_timestamp == 0)
							nulls[i++] = true;
						else
							values[i++] = TimestampTzGetDatum(tabentry->autovac_vacuum_timestamp);
						values[i++] = Int64GetDatum((int64) tabentry->autovac_vacuum_count);

						/* last_analyze */
						if (tabentry->analyze_timestamp == 0)
							nulls[i++] = true;
						else
							values[i++] = TimestampTzGetDatum(tabentry->analyze_timestamp);
						values[i++] = Int64GetDatum((int64) tabentry->analyze_count);

						/* last_autoanalyze */
						if (tabentry->autovac_analyze_timestamp == 0)
							nulls[i++] = true;
						else
							values[i++] = TimestampTzGetDatum(tabentry->autovac_analyze_timestamp);
						values[i++] = Int64GetDatum((int64) tabentry->autovac_analyze_count);

						Assert(i == POWA_STAT_TAB_COLS);

						tuplestore_putvalues(tupstore, tupdesc, values, nulls);
					}
					break;
				}
		}
	}

	/*
	 * Make sure any subsequent statistic retrieving will not see the one we
	 * just fetched
	 */
	pgstat_clear_snapshot();
#endif

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
