VERBOSITY = normal
GRUNT=$(PROJDIR)/node_modules/grunt-cli/bin/grunt

.PHONY: all install-deps help test unit-test integration-test publish

all: test

install-deps:
	$(PROJDIR)/build/install-deps $(PROJDIR)

lint:
	$(GRUNT) lint

unit-test:
	$(GRUNT) unit

integration-test:
	$(GRUNT) integration

security-test:
	$(GRUNT) security

test:
	$(GRUNT)

publish:
	$(PROJDIR)/build/publish $(VERSION)

help:
	@echo ''
	@echo ' Targets:'
	@echo '--------------------------------------------------'
	@echo ' all              - Run everything                '
	@echo ' lint             - Run jshint                    '
	@echo ' install-deps     - Install required dependencies '
	@echo ' test             - Run unit & integration tests  '
	@echo ' unit-test        - Run unit tests                '
	@echo ' integration-test - Run integration tests         '
	@echo ' security-test    - Run security tests            '
	@echo '--------------------------------------------------'
	@echo ''
