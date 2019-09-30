export GO111MODULE=on

IPFS_PATH ?= $(HOME)/.ipfs
STYX_PATH ?= /tmp/styx

clean:
	rm styx styx.so

styx.so:
	export GOPROXY='https://proxy.golang.org'
	go build -buildmode=plugin -i -o styx.so plugin/plugin.go
	chmod +x styx.so

build: styx.so

www:
	rm -rf $(STYX_PATH)/www
	mkdir -p $(STYX_PATH)/www
	cp -r webui/www $(STYX_PATH)/.

install: styx.so
	mkdir -p "$(IPFS_PATH)/plugins/"
	cp -f styx.so "$(IPFS_PATH)/plugins/styx.so"
