/* pglog/pglog--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pglog" to load this file. \quit

CREATE FUNCTION pglog_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pglog
  HANDLER pglog_handler
  NO VALIDATOR;

CREATE SERVER pglog_server
  FOREIGN DATA WRAPPER pglog;

CREATE TYPE pglog_severity AS ENUM (
	'DEBUG',
	'INFO',
	'NOTICE',
	'WARNING',
	'ERROR',
	'LOG',
	'FATAL',
	'PANIC',
	'???'
);

CREATE FOREIGN TABLE pglog
(
  log_time timestamp(3) with time zone,
  user_name text,
  database_name text,
  process_id integer,
  connection_from text,
  session_id text,
  session_line_num bigint,
  command_tag text,
  session_start_time timestamp with time zone,
  virtual_transaction_id text,
  transaction_id bigint,
  error_severity pglog_severity,
  sql_state_code text,
  message text,
  detail text,
  hint text,
  internal_query text,
  internal_query_pos integer,
  context text,
  query text,
  query_pos integer,
  location text,
  application_name text
) SERVER pglog_server;
