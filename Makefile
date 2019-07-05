GO111MODULE = on
IPFS_PATH ?= $(HOME)/.ipfs

build:
	export GOPROXY='https://proxy.golang.org'
	go build -buildmode=plugin -i -o styx.so plugin/plugin.go
	chmod +x styx.so

install: build
	mkdir -p "$(IPFS_PATH)/plugins/"
	cp -f styx.so "$(IPFS_PATH)/plugins/styx.so"
