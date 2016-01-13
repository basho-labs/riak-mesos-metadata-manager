.PHONY: all clean deps compile test distclean
SRC_PATH ?= ..
ZK_PATH ?= $(SRC_PATH)/mesos/build/3rdparty/zookeeper-3.4.5
ZK_BIN_PATH ?= $(ZK_PATH)/bin
ZK_CONF_PATH ?= $(ZK_PATH)/conf
ZK_VERSION ?= 3.4.5

all: deps compile

clean:
	$(REBAR) clean

distclean:
	$(REBAR) delete-deps

deps:
	$(REBAR) get-deps
	$(REBAR) compile

compile: deps

test: test_setup

test_setup:
	if [ ! -d $(ZK_BIN_PATH) ] && [ ! -d zk/bin ]; then \
		curl http://archive.apache.org/dist/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz >zookeeper-3.4.5.tar.gz && \
		tar xzf zookeeper-3.4.5.tar.gz && \
		mv zookeeper-3.4.5 zk && \
		rm zookeeper-3.4.5.tar.gz ; \
	fi
	if [ ! -d zk/bin ]; then \
		mkdir -p zk/bin && \
		mkdir -p zk/lib && \
		mkdir -p zk/conf && \
		cp $(ZK_BIN_PATH)/* zk/bin/ && \
		cp $(ZK_CONF_PATH)/* zk/conf && \
		cp $(ZK_PATH)/zookeeper-$(ZK_VERSION).jar zk/ && \
		cp -r $(ZK_PATH)/lib zk/ ; \
	fi
	if [ ! -e zk/conf/zoo.cfg ]; then \
		mv zk/conf/zoo_sample.cfg zk/conf/zoo.cfg ; \
	fi

DIALYZER_APPS = erts kernel stdlib sasl eunit compiler crypto public_key

include tools.mk

ifeq ($(REBAR),)
  $(error "Rebar not found. Please set REBAR environment variable or update PATH")
endif
