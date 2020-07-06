CWD     = $(CURDIR)
MODULE  = $(shell echo $(notdir $(CWD)) | tr "[:upper:]" "[:lower:]" )
OS     ?= $(shell uname -s)

NOW = $(shell date +%d%m%y)
REL = $(shell git rev-parse --short=4 HEAD)

PIP = $(CWD)/bin/pip3
PY  = $(CWD)/bin/python3

IP	 ?= 127.0.0.1
PORT ?= 19999

WGET = wget -c --no-check-certificate



.PHONY: all py test

all: py nm

py: $(PY) $(MODULE).py $(MODULE).ini
	IP=$(IP) PORT=$(PORT) $^



.PHONY: install update

install: $(OS)_install $(PIP) $(NIMBLE)
	$(PIP) install    -r requirements.txt
	$(MAKE) requirements.txt

update: $(OS)_update $(PIP)
	$(PIP) install -U    pip
	$(PIP) install -U -r requirements.txt
	$(MAKE) requirements.txt

$(PIP) $(PY):
	python3 -m venv .
	$(PIP) install -U pip pylint autopep8
	$(MAKE) requirements.txt
$(PYT):
	$(PIP) install -U pytest
	$(MAKE) requirements.txt

.PHONY: requirements.txt
requirements.txt: $(PIP)
	$< freeze | grep -v 0.0.0 > $@

.PHONY: Linux_install Linux_update

Linux_install Linux_update:
	sudo apt update
	sudo apt install -u `cat apt.txt`



.PHONY: master shadow release

MERGE  = Makefile README.md .gitignore .vscode apt.txt requirements.txt
MERGE += $(MODULE).py $(MODULE).ini static templates

master:
	git checkout $@
	git pull -v
	git checkout shadow -- $(MERGE)

shadow:
	git checkout $@
	git pull -v

release:
	git tag $(NOW)-$(REL)
	git push -v && git push -v --tags
	$(MAKE) shadow
