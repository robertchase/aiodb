.PHONY: shell lint test test_db

ifeq ($(GIT),)
  GIT := $(HOME)/git
endif

IMAGE := base-python-sql

NET := --net test
MOUNT := /opt/git
VOLUMES := -v=$(GIT):$(MOUNT)
WORKING := -w $(MOUNT)/aiodb
PYTHONPATH := -e PYTHONPATH=$(MOUNT)/ergaleia:$(MOUNT)/fsm:.

DOCKER := docker run --rm -it $(VOLUMES) $(PYTHONPATH) $(WORKING) $(NET) $(IMAGE)

shell:
	$(DOCKER) bash

lint:
	$(DOCKER) pylint aiodb

test:
	$(DOCKER) pytest tests
