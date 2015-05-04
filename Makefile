EXTENSION    = powa
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test
DOCS         = $(wildcard *.md)

PG_CONFIG = pg_config

MODULES = powa

all:

release-zip: all
	git archive --format zip --prefix=powa-${EXTVERSION}/ --output ./powa-${EXTVERSION}.zip HEAD
	unzip ./powa-$(EXTVERSION).zip
	rm ./powa-$(EXTVERSION).zip
	rm ./powa-$(EXTVERSION)/.gitignore
	rm ./powa-$(EXTVERSION)/reindent.sh
	sed -i -e "s/__VERSION__/$(EXTVERSION)/g"  ./powa-$(EXTVERSION)/META.json
	zip -r ./powa-$(EXTVERSION).zip ./powa-$(EXTVERSION)/
	rm ./powa-$(EXTVERSION) -rf

DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
