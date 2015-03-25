PROJDIR = $(SRCDIR)/src

VERBOSITY = normal

.PHONY: all install-deps help test

all: test

install-deps:
	$(SRCDIR)/build/install-deps

test: install-deps
	grunt

help:
	@echo ''
	@echo ' Targets:'
	@echo ' ----------------------'
	@echo ' all  - Run everything '
	@echo ' test - Run all tests  '
	@echo ' ----------------------'
	@echo ''
