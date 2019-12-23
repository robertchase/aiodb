.PHONY: shell test

ifeq ($(GIT),)
  GIT := $(HOME)/git
endif

MOUNT := /opt/git
VOLUMES := -v=$(GIT):$(MOUNT)
WORKING := -w $(MOUNT)/aiodb
PYTHONPATH := -e PYTHONPATH=$(MOUNT)/ergaleia:$(MOUNT)/fsm:.
IMAGE := alpine-python

DOCKER := docker run --rm -it $(VOLUMES) $(PYTHONPATH) $(WORKING) $(IMAGE)

shell:
	$(DOCKER) sh

flake:
	$(DOCKER) flake8

test:
	$(DOCKER) pytest $(ARGS)
