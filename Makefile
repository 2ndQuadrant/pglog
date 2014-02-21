# pglog/Makefile

MODULE_big = pglog
OBJS = pglog_helpers.o pglog.o

EXTENSION = pglog
DATA = pglog--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
