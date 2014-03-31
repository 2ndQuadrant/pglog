/*-------------------------------------------------------------------------
 *
 * pglog_spool.c
 *		  Event spooling for pglog extension
 *
 * Copyright (c) 2014, 2ndQuadrant Ltd
 * Some parts Copyright (c) 2010-2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglog/pglog_spool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pglog_spool.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/xact.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/ps_status.h"


/* GUC Variables */
char   *Pglog_directory = NULL;
int		Pglog_min_messages = WARNING;

/* Is event spooling working? */
bool Pglog_spooling_enabled = true;

/* Hook Functions */
static void pglog_emit_log_hook(ErrorData *edata);

/* Old hook storage for loading/unloading of the extension */
static emit_log_hook_type prev_emit_log_hook = NULL;

/* Current spool file */
static FILE *currentSpoolfile = NULL;
static char *currentSpoolfileName = NULL;
static bool spoolfileRotationRequired = false;

/*
 * buffers for formatted timestamps
 */
#define FORMATTED_TS_LEN 128
static char formatted_start_time[FORMATTED_TS_LEN];
static char formatted_log_time[FORMATTED_TS_LEN];

/* Internal functions */
static char *getSpoolfileName(const char *path, pg_time_t timestamp);
static void openSpoolfile(const char *path);
static void rotateSpoolfile(const char *path);
static void setup_formatted_log_time(void);
static void setup_formatted_start_time(void);
static inline void appendCSVLiteral(StringInfo buf, const char *data);
static const char * error_severity(int elevel);
static bool is_log_level_output(int elevel, int log_min_level);
static void fmtLogLine(StringInfo buf, ErrorData *edata);
static void pglog_emit_log_hook(ErrorData *edata);
static void guc_assign_directory(const char *newval, void *extra);
static bool guc_check_directory(char **newval, void **extra, GucSource source);

/*
 * We really want line-buffered mode for logfile output, but Windows does
 * not have it, and interprets _IOLBF as _IOFBF (bozos).  So use _IONBF
 * instead on Windows.
 */
#ifdef WIN32
#define LBF_MODE	_IONBF
#else
#define LBF_MODE	_IOLBF
#endif

/*
 * Enum definition for pglog.min_messages
 */
static const struct config_enum_entry server_message_level_options[] = {
	{"debug", DEBUG2, true},
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"error", ERROR, false},
	{"log", LOG, false},
	{"fatal", FATAL, false},
	{"panic", PANIC, false},
	{NULL, 0, false}
};

/*
 * construct logfile name using timestamp information
 *
 * Result is palloc'd.
 */
static char *
getSpoolfileName(const char *path, pg_time_t timestamp)
{
	char	   *filename;
	int			len;

	filename = palloc(MAXPGPATH);

	snprintf(filename, MAXPGPATH, "%s/", path);

	len = strlen(filename);

	/* treat Log_filename as a strftime pattern */
	pg_strftime(filename + len, MAXPGPATH - len, "pglog-%Y-%m-%d_%H%M%S.dat",
				pg_localtime(&timestamp, log_timezone));

	return filename;
}


/*
 * Open the log spool file
 */
static void
openSpoolfile(const char *path)
{
	const int	save_errno = errno;
	char        *filename  = NULL;
	FILE		*fh		   = NULL;
	mode_t		oumask;

	filename = getSpoolfileName(path, time(NULL));

	/*
	 * Create spool directory if not present; ignore errors
	 */
	mkdir(path, S_IRWXU);

	/*
	 * Note we do not let Log_file_mode disable IWUSR, since we certainly want
	 * to be able to write the files ourselves.
	 */
	oumask = umask((mode_t) ((~(Log_file_mode | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
	fh = fopen(filename, "a");
	umask(oumask);

	if (fh)
	{
		setvbuf(fh, NULL, LBF_MODE, 0);

#ifdef WIN32
		/* use CRLF line endings on Windows */
		_setmode(_fileno(fh), _O_TEXT);
#endif

		currentSpoolfile = fh;
	}
	else
	{
		/*
		 * We need to disable spooling to emit an error message here.
		 */
		Pglog_spooling_enabled = false;
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open log file \"%s\": %m",
						filename)));
	}
	pfree(filename);
	errno = save_errno;
	return;

}

/*
 * Close the current file (if any) and open a new one
 */
static void
rotateSpoolfile(const char *path)
{
	/* Close old file and free its name */
	if (currentSpoolfile) {
		fclose(currentSpoolfile);
		currentSpoolfile = NULL;
	}
	if (currentSpoolfileName) {
		pfree(currentSpoolfileName);
		currentSpoolfileName = NULL;
	}

	/* Enable spooling again */
	Pglog_spooling_enabled=true;

	/* Open a new log file */
	openSpoolfile(path);
}

/*
 * setup formatted_log_time, for consistent times between CSV and regular logs
 */
static void
setup_formatted_log_time(void)
{
	struct timeval tv;
	pg_time_t	stamp_time;
	char		msbuf[8];

	gettimeofday(&tv, NULL);
	stamp_time = (pg_time_t) tv.tv_sec;

	/*
	 * Note: we expect that guc.c will ensure that log_timezone is set up (at
	 * least with a minimal GMT value) before Log_line_prefix can become
	 * nonempty or CSV mode can be selected.
	 */
	pg_strftime(formatted_log_time, FORMATTED_TS_LEN,
	/* leave room for milliseconds... */
				"%Y-%m-%d %H:%M:%S     %Z",
				pg_localtime(&stamp_time, log_timezone));

	/* 'paste' milliseconds into place... */
	sprintf(msbuf, ".%03d", (int) (tv.tv_usec / 1000));
	strncpy(formatted_log_time + 19, msbuf, 4);
}

/*
 * setup formatted_start_time
 */
static void
setup_formatted_start_time(void)
{
	pg_time_t	stamp_time = (pg_time_t) MyStartTime;

	/*
	 * Note: we expect that guc.c will ensure that log_timezone is set up (at
	 * least with a minimal GMT value) before Log_line_prefix can become
	 * nonempty or CSV mode can be selected.
	 */
	pg_strftime(formatted_start_time, FORMATTED_TS_LEN,
				"%Y-%m-%d %H:%M:%S %Z",
				pg_localtime(&stamp_time, log_timezone));
}

/*
 * append a CSV'd version of a string to a StringInfo
 * We use the PostgreSQL defaults for CSV, i.e. quote = escape = '"'
 * If it's NULL, append nothing.
 */
static inline void
appendCSVLiteral(StringInfo buf, const char *data)
{
	const char *p = data;
	char		c;

	/* avoid confusing an empty string with NULL */
	if (p == NULL)
		return;

	appendStringInfoCharMacro(buf, '"');
	while ((c = *p++) != '\0')
	{
		if (c == '"')
			appendStringInfoCharMacro(buf, '"');
		appendStringInfoCharMacro(buf, c);
	}
	appendStringInfoCharMacro(buf, '"');
}


/*
 * error_severity --- get localized string representing elevel
 */
static const char *
error_severity(int elevel)
{
	const char *prefix;

	switch (elevel)
	{
		case DEBUG1:
		case DEBUG2:
		case DEBUG3:
		case DEBUG4:
		case DEBUG5:
			prefix = "DEBUG";
			break;
		case LOG:
		case COMMERROR:
			prefix = "LOG";
			break;
		case INFO:
			prefix = "INFO";
			break;
		case NOTICE:
			prefix = "NOTICE";
			break;
		case WARNING:
			prefix = "WARNING";
			break;
		case ERROR:
			prefix = "ERROR";
			break;
		case FATAL:
			prefix = "FATAL";
			break;
		case PANIC:
			prefix = "PANIC";
			break;
		default:
			prefix = "???";
			break;
	}

	return prefix;
}

/*
 * is_log_level_output -- is elevel logically >= log_min_level?
 *
 * We use this for tests that should consider LOG to sort out-of-order,
 * between ERROR and FATAL.  Generally this is the right thing for testing
 * whether a message should go to the postmaster log, whereas a simple >=
 * test is correct for testing whether the message should go to the client.
 */
static bool
is_log_level_output(int elevel, int log_min_level)
{
	if (elevel == LOG || elevel == COMMERROR)
	{
		if (log_min_level == LOG || log_min_level <= ERROR)
			return true;
	}
	else if (log_min_level == LOG)
	{
		/* elevel != LOG */
		if (elevel >= FATAL)
			return true;
	}
	/* Neither is LOG */
	else if (elevel >= log_min_level)
		return true;

	return false;
}

static void
fmtLogLine(StringInfo buf, ErrorData *edata)
{
	bool		print_stmt = false;

	/* static counter for line numbers */
	static long log_line_number = 0;

	/* has counter been reset in current process? */
	static int	log_my_pid = 0;

	/*
	 * This is one of the few places where we'd rather not inherit a static
	 * variable's value from the postmaster.  But since we will, reset it when
	 * MyProcPid changes.
	 */
	if (log_my_pid != MyProcPid)
	{
		log_line_number = 0;
		log_my_pid = MyProcPid;
		formatted_start_time[0] = '\0';
	}
	log_line_number++;

	/*
	 * timestamp with milliseconds
	 *
	 * Check if the timestamp is already calculated for the syslog message,
	 * and use it if so.  Otherwise, get the current timestamp.  This is done
	 * to put same timestamp in both syslog and csvlog messages.
	 */
	if (formatted_log_time[0] == '\0')
		setup_formatted_log_time();

	appendStringInfoString(buf, formatted_log_time);
	appendStringInfoChar(buf, ',');

	/* username */
	if (MyProcPort)
		appendCSVLiteral(buf, MyProcPort->user_name);
	appendStringInfoChar(buf, ',');

	/* database name */
	if (MyProcPort)
		appendCSVLiteral(buf, MyProcPort->database_name);
	appendStringInfoChar(buf, ',');

	/* Process id  */
	if (MyProcPid != 0)
		appendStringInfo(buf, "%d", MyProcPid);
	appendStringInfoChar(buf, ',');

	/* Remote host and port */
	if (MyProcPort && MyProcPort->remote_host)
	{
		appendStringInfoChar(buf, '"');
		appendStringInfoString(buf, MyProcPort->remote_host);
		if (MyProcPort->remote_port && MyProcPort->remote_port[0] != '\0')
		{
			appendStringInfoChar(buf, ':');
			appendStringInfoString(buf, MyProcPort->remote_port);
		}
		appendStringInfoChar(buf, '"');
	}
	appendStringInfoChar(buf, ',');

	/* session id */
	appendStringInfo(buf, "%lx.%x", (long) MyStartTime, MyProcPid);
	appendStringInfoChar(buf, ',');

	/* Line number */
	appendStringInfo(buf, "%ld", log_line_number);
	appendStringInfoChar(buf, ',');

	/* PS display */
	if (MyProcPort)
	{
		StringInfoData msgbuf;
		const char *psdisp;
		int			displen;

		initStringInfo(&msgbuf);

		psdisp = get_ps_display(&displen);
		appendBinaryStringInfo(&msgbuf, psdisp, displen);
		appendCSVLiteral(buf, msgbuf.data);

		pfree(msgbuf.data);
	}
	appendStringInfoChar(buf, ',');

	/* session start timestamp */
	if (formatted_start_time[0] == '\0')
		setup_formatted_start_time();
	appendStringInfoString(buf, formatted_start_time);
	appendStringInfoChar(buf, ',');

	/* Virtual transaction id */
	/* keep VXID format in sync with lockfuncs.c */
	if (MyProc != NULL && MyProc->backendId != InvalidBackendId)
		appendStringInfo(buf, "%d/%u", MyProc->backendId, MyProc->lxid);
	appendStringInfoChar(buf, ',');

	/* Transaction id */
	appendStringInfo(buf, "%u", GetTopTransactionIdIfAny());
	appendStringInfoChar(buf, ',');

	/* Error severity */
	appendStringInfoString(buf, error_severity(edata->elevel));
	appendStringInfoChar(buf, ',');

	/* SQL state code */
	appendStringInfoString(buf, unpack_sql_state(edata->sqlerrcode));
	appendStringInfoChar(buf, ',');

	/* errmessage */
	appendCSVLiteral(buf, edata->message);
	appendStringInfoChar(buf, ',');

	/* errdetail or errdetail_log */
	if (edata->detail_log)
		appendCSVLiteral(buf, edata->detail_log);
	else
		appendCSVLiteral(buf, edata->detail);
	appendStringInfoChar(buf, ',');

	/* errhint */
	appendCSVLiteral(buf, edata->hint);
	appendStringInfoChar(buf, ',');

	/* internal query */
	appendCSVLiteral(buf, edata->internalquery);
	appendStringInfoChar(buf, ',');

	/* if printed internal query, print internal pos too */
	if (edata->internalpos > 0 && edata->internalquery != NULL)
		appendStringInfo(buf, "%d", edata->internalpos);
	appendStringInfoChar(buf, ',');

	/* errcontext */
	appendCSVLiteral(buf, edata->context);
	appendStringInfoChar(buf, ',');

	/* user query --- only reported if not disabled by the caller */
	if (is_log_level_output(edata->elevel, log_min_error_statement) &&
		debug_query_string != NULL &&
		!edata->hide_stmt)
		print_stmt = true;
	if (print_stmt)
		appendCSVLiteral(buf, debug_query_string);
	appendStringInfoChar(buf, ',');
	if (print_stmt && edata->cursorpos > 0)
		appendStringInfo(buf, "%d", edata->cursorpos);
	appendStringInfoChar(buf, ',');

	/* file error location */
	if (Log_error_verbosity >= PGERROR_VERBOSE)
	{
		StringInfoData msgbuf;

		initStringInfo(&msgbuf);

		if (edata->funcname && edata->filename)
			appendStringInfo(&msgbuf, "%s, %s:%d",
							 edata->funcname, edata->filename,
							 edata->lineno);
		else if (edata->filename)
			appendStringInfo(&msgbuf, "%s:%d",
							 edata->filename, edata->lineno);
		appendCSVLiteral(buf, msgbuf.data);
		pfree(msgbuf.data);
	}
	appendStringInfoChar(buf, ',');

	/* application name */
	if (application_name)
		appendCSVLiteral(buf, application_name);

	appendStringInfoChar(buf, '\n');
}

static void
pglog_emit_log_hook(ErrorData *edata)
{
	int				save_errno;
	StringInfoData	buf;
	int				rc;

	/*
	 * Early exit if the spool directory path is not set
	 */
	if (!Pglog_spooling_enabled || Pglog_directory == NULL || strlen(Pglog_directory) <= 0)
	{
		/*
		 * Unsetting the GUCs via SIGHUP would leave a dangling file
		 * descriptor, if it exists, close it.
		 */
		if (currentSpoolfile) {
			fclose(currentSpoolfile);
		}

		goto quickExit;
	}

	/*
	 * Check if the log has to be written, if not just exit.
	 */
	if (! is_log_level_output(edata->elevel, Pglog_min_messages))
		goto quickExit;

	save_errno = errno;

	/*
	 * Initialize StringInfoDatas early, because pfree is called
	 * unconditionally at exit.
	 */
	initStringInfo(&buf);

	if (currentSpoolfile == NULL || spoolfileRotationRequired)
	{
		rotateSpoolfile(Pglog_directory);

		/* Couldn't open the destination file; give up */
		if (currentSpoolfile == NULL)
			goto exit;
	}

	/* format the log line */
	fmtLogLine(&buf, edata);

	/* write the log line
	 * TODO: this is not safe for concurrency
	 */
	fseek(currentSpoolfile, 0L, SEEK_END);
	rc = fwrite(buf.data, 1, buf.len, currentSpoolfile);

	/* can't use ereport here because of possible recursion */
	if (rc != buf.len) {
		/*
		 * We need to disable spooling to emit an error message here.
		 */
		Pglog_spooling_enabled = false;
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write log file \"%s\": %m",
						currentSpoolfileName)));
	}

	goto exit;

exit:
	pfree(buf.data);
	errno = save_errno;

quickExit:
	/* Call a previous hook, should it exist */
	if (prev_emit_log_hook != NULL)
		prev_emit_log_hook(edata);
}

static void
guc_assign_directory(const char *newval, void *extra)
{
	spoolfileRotationRequired = true;
}

static bool
guc_check_directory(char **newval, void **extra, GucSource source)
{
	/*
	 * Since canonicalize_path never enlarges the string, we can just modify
	 * newval in-place.
	 */
	canonicalize_path(*newval);
	return true;
}

/*
 * Spooling initialization function
 */
void
pglog_spool_init(void)
{
	/* Set up GUCs */
	DefineCustomStringVariable("pglog.directory",
							   "Directory where to spool log data",
							   NULL,
							   &Pglog_directory,
							   "pglog_spool",
							   PGC_SIGHUP,
							   GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY,
							   guc_check_directory,
							   guc_assign_directory,
							   NULL);

	DefineCustomEnumVariable("pglog.min_messages",
							 "Sets the message levels that are logged.",
							 "Each level includes all the levels that follow it. The later"
							 " the level, the fewer messages are sent.",
							 &Pglog_min_messages,
							 WARNING,
							 server_message_level_options,
							 PGC_SUSET,
							 GUC_NOT_IN_SAMPLE | GUC_SUPERUSER_ONLY,
							 NULL,
							 NULL,
							 NULL);

	/* Install hook */
	prev_emit_log_hook = emit_log_hook;
	emit_log_hook = pglog_emit_log_hook;
}

/*
 * Spooling unloading function
 */
void
pglog_spool_fini(void)
{
	/* Uninstall hook */
	emit_log_hook = prev_emit_log_hook;
}
