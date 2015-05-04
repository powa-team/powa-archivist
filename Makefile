MODULES = powa
EXTENSION = powa
DATA = $(wildcard *--*.sql)
DOCS = README.md

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
