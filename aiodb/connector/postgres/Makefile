.PHONY: bash test cli schema start

ifeq ($(GIT),)
  GIT := $(HOME)/git
endif

IMAGE := alpine-python

DATA := $(HOME)/docker/volumes/postgres
POSTGRES := postgres
DATABASE := -e PG_HOST=$(POSTGRES)

NET := --net test
MOUNT := /opt/git
VOLUMES := -v=$(GIT):/opt/git
PYTHONPATH := -e PYTHONPATH=$(MOUNT)/fsm:$(MOUNT)/ergaleia:$(MOUNT)/aiodb:.
WORKING_DIR := $(MOUNT)/aiodb/aiodb/connector/postgres
WORKING := -w $(WORKING_DIR)

CLI := docker exec -it $(POSTGRES) psql -U postgres
DOCKER := docker run --rm -it $(OPTS) $(NET) $(DATABASE) $(VOLUMES) $(PYTHONPATH) $(WORKING) $(IMAGE)

shell:
	$(DOCKER) sh

test:
	$(DOCKER) pytest $(ARGS)

cli:
	$(CLI)

schema:
	$(CLI) -d test -f $(WORKING_DIR)/schema.sql

start: $(DATA)
	docker run --rm  -d --name $(POSTGRES) $(NET) $(VOLUMES) -e POSTGRES_PASSWORD=docker -v $(DATA):/var/lib/postgresql/data postgres

$(DATA):
	mkdir -p $(DATA)
