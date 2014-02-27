/*-------------------------------------------------------------------------
 *
 * pglog_helpers.c
 *		  Helper functions for pglog extension
 *
 * Copyright (c) 2014, 2ndQuadrant Ltd
 * Some parts Copyright (c) 2010-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglog/pglog_helpers.c
 *
 *-------------------------------------------------------------------------
 */

#include "pglog_helpers.h"

#include <sys/stat.h>

#include "utils/rel.h"
#include "access/sysattr.h"
#include "postmaster/syslogger.h"
#include "dirent.h"
#include "storage/fd.h"

/*
 * check_selective_binary_conversion
 *
 * Check to see if it's useful to convert only a subset of the file's columns
 * to binary.  If so, construct a list of the column names to be converted,
 * return that at *columns, and return TRUE.  (Note that it's possible to
 * determine that no columns need be converted, for instance with a COUNT(*)
 * query.  So we can't use returning a NIL list to indicate failure.)
 */
bool
check_selective_binary_conversion(RelOptInfo *baserel,
								  Oid foreigntableid,
								  List **columns)
{
	ListCell   *lc;
	Relation	rel;
	TupleDesc	tupleDesc;
	AttrNumber	attnum;
	Bitmapset  *attrs_used = NULL;
	bool		has_wholerow = false;
	int			numattrs;
	int			i;

	elog(DEBUG1,"Entering function %s",__func__);

	*columns = NIL;				/* default result */

	/* Collect all the attributes needed for joins or final output. */
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid,
				   &attrs_used);

	/* Add all the attributes used by restriction clauses. */
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &attrs_used);
	}

	/* Convert attribute numbers to column names. */
	rel = heap_open(foreigntableid, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);

	while ((attnum = bms_first_member(attrs_used)) >= 0)
	{
		/* Adjust for system attributes. */
		attnum += FirstLowInvalidHeapAttributeNumber;

		if (attnum == 0)
		{
			has_wholerow = true;
			break;
		}

		/* Ignore system attributes. */
		if (attnum < 0)
			continue;

		/* Get user attributes. */
		if (attnum > 0)
		{
			Form_pg_attribute attr = tupleDesc->attrs[attnum - 1];
			char	   *attname = NameStr(attr->attname);

			/* Skip dropped attributes (probably shouldn't see any here). */
			if (attr->attisdropped)
				continue;
			*columns = lappend(*columns, makeString(pstrdup(attname)));
		}
	}

	/* Count non-dropped user attributes while we have the tupdesc. */
	numattrs = 0;
	for (i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = tupleDesc->attrs[i];

		if (attr->attisdropped)
			continue;
		numattrs++;
	}

	heap_close(rel, AccessShareLock);

	/* If there's a whole-row reference, fail: we need all the columns. */
	if (has_wholerow)
	{
		*columns = NIL;
		return false;
	}

	/* If all the user attributes are needed, fail. */
	if (numattrs == list_length(*columns))
	{
		*columns = NIL;
		return false;
	}

	return true;
}

/*
 * Estimate size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
void
estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  PgLogPlanState *fdw_private)
{
	struct stat stat_buf;
	BlockNumber pages;
	double		ntuples;
	double		nrows;

	elog(DEBUG1,"Entering function %s",__func__);

	/* TODO: loop through any log file (currently uses only the first) */

	/*
	 * Get size of the file.  It might not be there at plan time, though, in
	 * which case we have to use a default estimate.
	 */
	if (stat(fdw_private->filenames[0], &stat_buf) < 0)
		stat_buf.st_size = 10 * BLCKSZ;

	/*
	 * Convert size to pages for use in I/O cost estimate later.
	 */
	pages = (stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ;
	if (pages < 1)
		pages = 1;
	fdw_private->pages = pages;

	/*
	 * Estimate the number of tuples in the file.
	 */
	if (baserel->pages > 0)
	{
		/*
		 * We have # of pages and # of tuples from pg_class (that is, from a
		 * previous ANALYZE), so compute a tuples-per-page estimate and scale
		 * that by the current file size.
		 */
		double		density;

		density = baserel->tuples / (double) baserel->pages;
		ntuples = clamp_row_est(density * (double) pages);
	}
	else
	{
		/*
		 * Otherwise we have to fake it.  We back into this estimate using the
		 * planner's idea of the relation width; which is bogus if not all
		 * columns are being read, not to mention that the text representation
		 * of a row probably isn't the same size as its internal
		 * representation.	Possibly we could do something better, but the
		 * real answer to anyone who complains is "ANALYZE" ...
		 */
		int			tuple_width;

		tuple_width = MAXALIGN(baserel->width) +
			MAXALIGN(sizeof(HeapTupleHeaderData));
		ntuples = clamp_row_est((double) stat_buf.st_size /
								(double) tuple_width);
	}
	fdw_private->ntuples = ntuples;

	/*
	 * Now estimate the number of rows returned by the scan after applying the
	 * baserestrictinfo quals.
	 */
	nrows = ntuples *
		clauselist_selectivity(root,
							   baserel->baserestrictinfo,
							   0,
							   JOIN_INNER,
							   NULL);

	nrows = clamp_row_est(nrows);

	/* Save the output-rows estimate for the planner */
	baserel->rows = nrows;
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   PgLogPlanState *fdw_private,
			   Cost *startup_cost, Cost *total_cost)
{
	BlockNumber pages = fdw_private->pages;
	double		ntuples = fdw_private->ntuples;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

	elog(DEBUG1,"Entering function %s",__func__);

	/*
	 * We estimate costs almost the same way as cost_seqscan(), thus assuming
	 * that I/O costs are equivalent to a regular table file of the same size.
	 * However, we take per-tuple CPU costs as 10x of a seqscan, to account
	 * for the cost of parsing records.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
}

/*
 * Initialise the list of available log files within logging directory
 *
 * Results are returned as a char** value, dynamically created by the function
 */
char **
initLogFileNames(void)
{
	char **filenames;
	char *filename;
	int dir_length;
	int length;
	int i;
	DIR *dir;
	struct dirent *de;

	/* Initialises the file names structure */
	filenames = (char **) palloc(sizeof(char *) * MAX_LOG_FILES);
	for (i = 0; i < MAX_LOG_FILES; ++i)
		filenames[i] = 0;

	elog(DEBUG1,"Log directory: %s - filename: %s", Log_directory, Log_filename);

	/* Open log directory */
	dir = AllocateDir(Log_directory);
	dir_length = strlen(Log_directory) + 1; /* consider slash too */
	i = 0;
	while ((de = ReadDir(dir, Log_directory)) != NULL)
	{
		elog(DEBUG1,"Found directory entry: %s", de->d_name);
		/* Look for CSV files */
		length = strlen(de->d_name);
		if (length > 4 && (strcmp(de->d_name + (length - 4), ".csv") == 0))
		{
			elog(DEBUG1,"Found CSV log file: %s", de->d_name);
			/* Allocate the file name */
			length += dir_length + 1;
			filename = (char *) palloc(length * sizeof(char));
			snprintf (filename, length, "%s/%s", Log_directory, de->d_name);
			/* Insert the file in the final array */
			filenames[i++] = filename;
		}

		/* Maximum number of allowed log files reached */
		if (i == MAX_LOG_FILES)
			break;
	}
	FreeDir(dir);

	/* TODO: sorting of entries based on time? */

	return filenames;

}

/* Start to read the next log file */
void
BeginNextCopy(Relation rel, PgLogExecutionState *state)
{
	MemoryContext oldcontext;
	oldcontext = MemoryContextSwitchTo(state->scan_cxt);

	if (state->cstate) {
		EndCopyFrom(state->cstate);
	}
	elog(DEBUG1,"Opening log file: %s", state->filenames[state->i]);
	state->cstate = BeginCopyFrom(rel,
		state->filenames[state->i],
		false,
		NIL,
		state->options);

	MemoryContextSwitchTo(oldcontext);
}

/* Get the next log line */
bool
GetNextRow(Relation rel, PgLogExecutionState *state, TupleTableSlot* slot)
{
	return NextCopyFrom(state->cstate, NULL,
		slot->tts_values, slot->tts_isnull,
		NULL);
}

/* Is the last log file to be read? */
bool
isLastLogFile(PgLogExecutionState* state)
{
	elog(DEBUG1, "i: %d", state->i);
	if (state->i < (MAX_LOG_FILES -1)
		&& state->filenames[state->i + 1])
			return false;

	return true;
}
