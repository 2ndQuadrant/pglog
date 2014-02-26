/*-------------------------------------------------------------------------
 *
 * pglog.c
 *		  PostgreSQL log via SQL
 *
 * Copyright (c) 2014, 2ndQuadrant Ltd
 * Some parts Copyright (c) 2010-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglog/pglog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pglog_helpers.h"

#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "postmaster/syslogger.h"

PG_MODULE_MAGIC;

/*
 * SQL functions
 */
extern Datum pglog_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pglog_handler);

/*
 * FDW callback routines
 */
static void pglogGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid);
static void pglogGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid);
static ForeignScan *pglogGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses);
static void pglogBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *pglogIterateForeignScan(ForeignScanState *node);
static void pglogReScanForeignScan(ForeignScanState *node);
static void pglogEndForeignScan(ForeignScanState *node);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
pglog_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	elog(DEBUG1,"Entering function %s",__func__);

	/* Check if logging_collector is on and CSV is enabled */
	if (! Logging_collector || !strstr(Log_destination_string, "csvlog"))
		ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_HANDLE),
			errmsg("Cannot instantiate the 'pglog' extension handler"),
			errhint("'pgxlog' requires you to set 'log_collector = on' and to add 'csvlog' to 'log_destination'")
		));


	/* Set handlers */
	fdwroutine->GetForeignRelSize = pglogGetForeignRelSize;
	fdwroutine->GetForeignPaths = pglogGetForeignPaths;
	fdwroutine->GetForeignPlan = pglogGetForeignPlan;
	fdwroutine->BeginForeignScan = pglogBeginForeignScan;
	fdwroutine->IterateForeignScan = pglogIterateForeignScan;
	fdwroutine->ReScanForeignScan = pglogReScanForeignScan;
	fdwroutine->EndForeignScan = pglogEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * pglogGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
pglogGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	PgLogPlanState *fdw_private;

	elog(DEBUG1,"Entering function %s",__func__);

	/*
	 * Fetch options.  We only need filenames at this point, but we might as
	 * well get everything and not need to re-fetch it later in planning.
	 */
	fdw_private = (PgLogPlanState *) palloc(sizeof(PgLogPlanState));
	fdw_private->i = 0;
	fdw_private->filenames = initLogFileNames();
	baserel->fdw_private = (void *) fdw_private;

	/* Estimate relation size */
	estimate_size(root, baserel, fdw_private);
}

/*
 * pglogGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
static void
pglogGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	PgLogPlanState *fdw_private = (PgLogPlanState *) baserel->fdw_private;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *columns;
	List	   *coptions = NIL;

	elog(DEBUG1,"Entering function %s",__func__);
	/* Decide whether to selectively perform binary conversion */
	if (check_selective_binary_conversion(baserel,
										  foreigntableid,
										  &columns))
		coptions = list_make1(makeDefElem("convert_selectively",
										  (Node *) columns));

	/* Estimate costs */
	estimate_costs(root, baserel, fdw_private,
				   &startup_cost, &total_cost);

	/*
	 * Create a ForeignPath node and add it as only possible path.	We use the
	 * fdw_private list of the path to carry the convert_selectively option;
	 * it will be propagated into the fdw_private list of the Plan node.
	 */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 coptions));

	/*
	 * If data file was sorted, and we knew it somehow, we could insert
	 * appropriate pathkeys into the ForeignPath node to tell the planner
	 * that.
	 */
}

/*
 * pglogGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
pglogGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses)
{
	Index		scan_relid = baserel->relid;

	elog(DEBUG1,"Entering function %s",__func__);

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							best_path->fdw_private);
}

/*
 * pglogBeginForeignScan
 *		Initiate access to the log by creating CopyState
 */
static void
pglogBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
	List	   *options;
	CopyState	cstate;
	PgLogExecutionState *festate;

	elog(DEBUG1,"Entering function %s",__func__);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Initialise the log file names structure */
	festate = (PgLogExecutionState *) palloc(sizeof(PgLogExecutionState));
	festate->filenames = initLogFileNames();

	/* Forces CSV format */
	options = list_make1(makeDefElem("format", (Node *) makeString("csv")));

	/* Add any options from the plan (currently only convert_selectively) */
	options = list_concat(options, plan->fdw_private);

	/*
	 * Create CopyState from FDW options.  We always acquire all columns, so
	 * as to match the expected ScanTupleSlot signature.
	 * Starts from the first file in the list.
	 */
	cstate = BeginCopyFrom(node->ss.ss_currentRelation,
						   festate->filenames[0],
						   false,
						   NIL,
						   options);

	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	festate->cstate = cstate;

	node->fdw_state = (void *) festate;
}

/*
 * pglogIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
pglogIterateForeignScan(ForeignScanState *node)
{
	PgLogExecutionState *festate = (PgLogExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	bool		found;
	ErrorContextCallback errcallback;

	/* Set up callback to identify error line number. */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) festate->cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/*
	 * The protocol for loading a virtual tuple into a slot is first
	 * ExecClearTuple, then fill the values/isnull arrays, then
	 * ExecStoreVirtualTuple.  If we don't find another row in the file, we
	 * just skip the last step, leaving the slot empty as required.
	 *
	 * We can pass ExprContext = NULL because we read all columns from the
	 * file, so no need to evaluate default expressions.
	 *
	 * We can also pass tupleOid = NULL because we don't allow oids for
	 * foreign tables.
	 */
	ExecClearTuple(slot);
	found = NextCopyFrom(festate->cstate, NULL,
						 slot->tts_values, slot->tts_isnull,
						 NULL);
	if (found)
		ExecStoreVirtualTuple(slot);

	/* Remove error callback. */
	error_context_stack = errcallback.previous;

	return slot;
}

/*
 * pglogReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
pglogReScanForeignScan(ForeignScanState *node)
{
	PgLogExecutionState *festate = (PgLogExecutionState *) node->fdw_state;
	elog(DEBUG1,"Entering function %s",__func__);

	EndCopyFrom(festate->cstate);

	festate->cstate = BeginCopyFrom(node->ss.ss_currentRelation,
									festate->filenames[0],
									false,
									NIL,
									NIL);
}

/*
 * pglogEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
pglogEndForeignScan(ForeignScanState *node)
{
	PgLogExecutionState *festate = (PgLogExecutionState *) node->fdw_state;
	elog(DEBUG1,"Entering function %s",__func__);

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
		EndCopyFrom(festate->cstate);
}
