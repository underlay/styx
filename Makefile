export GO111MODULE=on

IPFS_PATH ?= $(HOME)/.ipfs
STYX_PATH ?= /tmp/styx

build: styx.so

styx.so:
	export GOPROXY='https://proxy.golang.org'
	go build -buildmode=plugin -i -o styx.so plugin/plugin.go
	chmod +x styx.so

clean:
	rm -f styx.so

www:
	echo "Writing webui to $(STYX_PATH)/www"
	rm -rf $(STYX_PATH)/www
	mkdir -p $(STYX_PATH)/www
	cp -r webui/www $(STYX_PATH)

install: www styx.so
	mkdir -p "$(IPFS_PATH)/plugins/"
	cp -f styx.so "$(IPFS_PATH)/plugins/styx.so"
