/*-------------------------------------------------------------------------
 *
 * pglog_helpers.h
 *		  Helper functions for pglog extension
 *
 * Copyright (c) 2014, 2ndQuadrant Ltd
 * Some parts Copyright (c) 2010-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglog/pglog_helpers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOG_HELPERS_H
#define PGLOG_HELPERS_H

#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "commands/copy.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"

/* Maximum number of log files to be read */
#define MAX_LOG_FILES 16

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct pglogPlanState
{
	char **filenames; /* log file names */
	int i; /* log file index */
	BlockNumber pages; /* estimate of file's physical size */
	double ntuples; /* estimate of number of rows in file */
} PgLogPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct pglogExecutionState
{
	char **filenames; /* log file names */
	int i; /* log file index */
	CopyState cstate; /* state of reading file */
	List *options; /* options (mainly for COPY) */
	MemoryContext scan_cxt; /* context for per-scan lifespan data */
} PgLogExecutionState;

/*
 * Helper functions
 */
bool check_selective_binary_conversion(RelOptInfo *baserel,
								  Oid foreigntableid,
								  List **columns);
void estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  PgLogPlanState *fdw_private);
void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   PgLogPlanState *fdw_private,
			   Cost *startup_cost, Cost *total_cost);
char **initLogFileNames(const char *path);

void BeginNextCopy(Relation rel, PgLogExecutionState* state);
bool isLastLogFile(PgLogExecutionState* state);
bool GetNextRow(Relation rel, PgLogExecutionState *state, TupleTableSlot* slot);

#endif
