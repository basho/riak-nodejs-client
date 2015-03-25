VERBOSITY = normal
GRUNT=$(PROJDIR)/node_modules/grunt-cli/bin/grunt

.PHONY: all install-deps help test unit-test integration-test

all: test

install-deps:
	$(PROJDIR)/build/install-deps

unit-test:
	$(GRUNT) unit

integration-test:
	$(GRUNT) unit

test: install-deps unit-test integration-test

help:
	@echo ''
	@echo ' Targets:'
	@echo ' ----------------------'
	@echo ' all  - Run everything '
	@echo ' test - Run all tests  '
	@echo ' ----------------------'
	@echo ''
