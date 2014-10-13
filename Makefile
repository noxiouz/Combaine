
# pwd
CURDIR:=$(shell pwd)

MAIN_FILES_DIR=$(CURDIR)/main_files

# packages for go
PACKAGE_PATH=$(CURDIR)/src/github.com/noxiouz

# build dir
BUILD_DIR=$(CURDIR)/build

export GOPATH=$(CURDIR)


all: combainer_ agave_ cfgmanager_ parsing_ graphite_

deps:
	go get launchpad.net/gozk/zookeeper
	go get launchpad.net/goyaml
	go get github.com/cocaine/cocaine-framework-go/cocaine
	go get github.com/howeyc/fsnotify
	go get github.com/Sirupsen/logrus
	go get github.com/mitchellh/mapstructure
	mkdir -vp $(PACKAGE_PATH)
	if [ ! -d $(CURDIR)/src/github.com/noxiouz/Combaine ];then\
		ln -vs $(CURDIR) $(CURDIR)/src/github.com/noxiouz/Combaine; fi;

combainer_:
	go build -o $(BUILD_DIR)/main_combainer $(MAIN_FILES_DIR)/combainer_main.go

parsing_:
	go build -o $(BUILD_DIR)/main_parsing-core $(MAIN_FILES_DIR)/parsing_main.go

cfgmanager_:
	go build -o $(BUILD_DIR)/main_cfgmanager $(MAIN_FILES_DIR)/cfgmanager_main.go

agave_:
	go build -o $(BUILD_DIR)/main_agave $(MAIN_FILES_DIR)/agave_main.go

graphite_:
	go build -o $(BUILD_DIR)/main_graphite $(MAIN_FILES_DIR)/graphite_main.go

clean::
	rm -rf $(BUILD_DIR) || true
