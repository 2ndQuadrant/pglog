/*-------------------------------------------------------------------------
 *
 * pglog_spool.h
 *		  Event spooling for pglog extension
 *
 * Copyright (c) 2014, 2ndQuadrant Ltd
 *
 * IDENTIFICATION
 *		  pglog/pglog_spool.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOG_SPOOL_H
#define PGLOG_SPOOL_H

#include "postgres.h"

/* GUC Variable */
extern PGDLLIMPORT char *Pglog_directory;

/* Is event spooling working? */
extern PGDLLIMPORT bool Pglog_spooling_enabled;

/* Spooling initialization function */
extern void pglog_spool_init(void);
extern void pglog_spool_fini(void);

#endif
