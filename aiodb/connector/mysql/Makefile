.PHONY: shell test cli schema start

ifeq ($(GIT),)
  GIT := $(HOME)/git
endif

IMAGE := alpine-python

MYSQL := mysql
VERSION := mysql:5.7
DATA := $(HOME)/mysql_data
DATABASE := -e MYSQL_HOST=$(MYSQL)

NET := --net test
MOUNT := /opt/git
VOLUMES := -v=$(GIT):$(MOUNT)
PYTHONPATH := -e PYTHONPATH=$(MOUNT)/ergaleia:$(MOUNT)/fsm:$(MOUNT)/aiodb:.
WORKING_DIR := $(MOUNT)/aiodb/aiodb/connector/mysql
WORKING := -w $(WORKING_DIR)

CLI := docker exec -it $(MYSQL) mysql test_mysql
DOCKER := docker run --rm -it $(OPTS) $(NET) $(DATABASE) $(VOLUMES) $(PYTHONPATH) $(WORKING) $(IMAGE)

shell:
	$(DOCKER) sh

test:
	$(DOCKER) pytest $(ARGS)

cli:
	$(CLI) $(ARGS)

schema:
	docker exec -it $(MYSQL) sh -c '/usr/bin/mysql < $(WORKING_DIR)/schema.sql'

start:
	docker run --rm -d --name $(MYSQL) $(NET) $(VOLUMES) -e MYSQL_ALLOW_EMPTY_PASSWORDS=yes -v $(DATA):/var/lib/mysql/ $(VERSION)
