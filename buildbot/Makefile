PROJDIR = $(realpath $(CURDIR)/..)
include $(PROJDIR)/build/common.mk

TOOLS_DIR = $(PROJDIR)/tools/devrel
CA_DIR = $(PROJDIR)/tools/test-ca
RIAK_CONF = $(RIAK_DIR)/etc/riak.conf
ADV_CONF = $(RIAK_DIR)/etc/advanced.config
RIAK_ADMIN = $(RIAK_DIR)/bin/riak-admin

export RIAK_HOST = localhost
export RIAK_PORT = 8087

preconfigure:
	$(TOOLS_DIR)/gen-riak-conf $(RIAK_CONF) 8098 8087 18098 $(CA_DIR)/certs/cacert.pem $(CA_DIR)/certs/riak-test-cert.pem $(CA_DIR)/private/riak-test-key.pem
	$(TOOLS_DIR)/strong-consistency-conf $(RIAK_CONF)
	$(TOOLS_DIR)/gen-adv-conf $(ADV_CONF)

configure:
	$(TOOLS_DIR)/riak-cluster-config $(RIAK_ADMIN) 8098 true true

compile: install-deps

setup-integration:
	echo 'Nothing to do!'

test:
	$(RIAK_ADMIN) security disable
	$(GRUNT)
	$(RIAK_ADMIN) security enable
	$(GRUNT) security
