# make file to hold the logic of build and test setup
ZK_VERSION ?= 3.6.2

# Apache changed the name of the archive in version 3.5.x and seperated out
# src and binary packages
ZK_MINOR_VER=$(word 2, $(subst ., ,$(ZK_VERSION)))
ifeq ($(shell test $(ZK_MINOR_VER) -le 4; echo $$?),0)
  ZK = zookeeper-$(ZK_VERSION)
else
  ZK = apache-zookeeper-$(ZK_VERSION)-bin
endif
ZK_URL = "https://archive.apache.org/dist/zookeeper/zookeeper-$(ZK_VERSION)/$(ZK).tar.gz"

PACKAGES := $(shell go list ./... | grep -v examples)

.DEFAULT_GOAL := test

$(ZK):
	wget $(ZK_URL)
	tar -zxf $(ZK).tar.gz
	rm $(ZK).tar.gz

zookeeper: $(ZK)
	# we link to a standard directory path so then the tests dont need to find based on version
	# in the test code. this allows backward compatable testing.
	ln -s $(ZK) zookeeper

.PHONY: setup
setup: zookeeper tools

.PHONY: tools
tools: tools/tools.go tools/go.mod
	mkdir -p "tools/bin"
	cd tools && go build -o bin/golangci-lint github.com/golangci/golangci-lint/cmd/golangci-lint && cd ..

.PHONY: lint
lint: tools
	go fmt ./...
	go vet ./...
	tools/bin/golangci-lint run -v --deadline 10m

.PHONY: lint-fix
lint-fix: tools
	go fmt ./...
	go vet ./...
	tools/bin/golangci-lint run -v --deadline 10m --fix

.PHONY: build
build:
	go build ./...

.PHONY: test
test: build zookeeper
	go test -timeout 500s -v -race -covermode atomic -coverprofile=profile.cov $(PACKAGES)

.PHONY: clean
clean:
	rm -f apache-zookeeper-*.tar.gz
	rm -f zookeeper-*.tar.gz
	rm -rf apache-zookeeper-*/
	rm -rf zookeeper-*/
	rm -f zookeeper
	rm -f profile.cov
	rm -rf tools/bin
