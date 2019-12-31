.PHONY: shell test

ifeq ($(GIT),)
  GIT := $(HOME)/git
endif

IMAGE := alpine-python

NET := --net test
MOUNT := /opt/git
VOLUMES := -v=$(GIT):$(MOUNT)
WORKING := -w $(MOUNT)/aiodb
PYTHONPATH := -e PYTHONPATH=$(MOUNT)/ergaleia:$(MOUNT)/fsm:.

DOCKER := docker run --rm -it $(VOLUMES) $(PYTHONPATH) $(WORKING) $(NET) $(IMAGE)

shell:
	$(DOCKER) sh

flake:
	$(DOCKER) flake8

test:
	$(DOCKER) pytest aiodb/model
