PROJECT=lease-version-reliability
VERSION=3.9.10
ENV=local

SHELL:=/bin/bash
VENV=${PROJECT}-${VERSION}
VENV_DIR=$(shell pyenv root)/versions/${VENV}
PYTHON=${VENV_DIR}/bin/python
JUPYTER_ENV_NAME=${VENV}
JUPYTER_PORT=8888
BUCKET=compstak-machine-learning

DEFAULT_GOAL: help
.PHONY: help run clean build venv ipykernel update jupyter pytest check-all

# Colors for echos
ccend=$(shell tput sgr0)
ccbold=$(shell tput bold)
ccgreen=$(shell tput setaf 2)
ccso=$(shell tput smso)

HELP_FUN = \
	%help; \
	while(<>) { push @{$$help{$$2 // 'options'}}, [$$1, $$3] if /^([a-zA-Z\-\$\(]+)\s*:.*\#\#(?:@([a-zA-Z\-\)]+))?\s(.*)$$/ }; \
	print "usage: make [target]\n\n"; \
	for (sort keys %help) { \
	print "${WHITE}$$_:${RESET}\n"; \
	for (@{$$help{$$_}}) { \
	$$sep = " " x (32 - length $$_->[0]); \
	print "  ${YELLOW}$$_->[0]${RESET}$$sep${GREEN}$$_->[1]${RESET}\n"; \
	}; \
	print "\n"; }

help: ##@other >> Show this help.
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)
	@echo ""
	@echo "Note: to activate the environment in your local shell type:"
	@echo "   $$ pyenv activate $(VENV)"

build: ##@build >> build the virtual environment with an ipykernel for jupyter and install requirements
	@echo ""
	@echo "$(ccso)--> Build $(ccend)"
	$(MAKE) venv
	$(MAKE) install
	$(MAKE) ipykernel

venv: $(VENV_DIR) ##@build >> setup the virtual environment

$(VENV_DIR):
	@echo "$(ccso)--> Install and setup pyenv and virtualenv $(ccend)"
	python3 -m pip install --upgrade pip
	pyenv virtualenv ${VENV}
	echo ${VENV} > .python-version

install: venv requirements-dev.txt ##@build >> update requirements-dev.txt inside the virtual environment
	@echo "$(ccso)--> Updating packages $(ccend)"
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt
	$(PYTHON) -m pip install pre-commit
	pre-commit install
	pre-commit autoupdate

ipykernel: venv ##@build >> install a Jupyter iPython kernel using our virtual environment
	@echo ""
	@echo "$(ccso)--> Install ipykernel to be used by jupyter notebooks $(ccend)"
	$(PYTHON) -m pip install ipykernel jupyter jupyter_contrib_nbextensions
	$(PYTHON) -m ipykernel install \
					--user \
					--name=$(VENV) \
					--display-name=$(JUPYTER_ENV_NAME)
	$(PYTHON) -m jupyter nbextension enable --py widgetsnbextension --sys-prefix

clean: ##@build >> remove all environment and build files
	@echo ""
	@echo "$(ccso)--> Removing virtual environment $(ccend)"
	pyenv virtualenv-delete --force ${VENV}
	rm .python-version

jupyter: venv ##@tool >> start a jupyter notebook
	@echo ""
	@echo "$(ccso)--> Running jupyter notebook on port $(JUPYTER_PORT) $(ccend)"
	jupyter notebook --port $(JUPYTER_PORT)

pytest: ##@tool >> run pytest
	$(PYTHON) -m pytest tests/test.py -s

check-all: ##@tool >> perform pre-commit checks
	pre-commit run --all-files

push-data: ##@data >> upload raw & processed dataset to S3
	$(MAKE) push-raw-data
	$(MAKE) push-processed-data

push-raw-data: ##@data >> upload raw dataset to S3
	tar -czvf data/raw/$(ENV).dataset.tar.gz \
	--exclude=".gitkeep" \
	--exclude="*.gz" \
	--exclude=".DS_Store" \
	-C data/raw/ \
	.
	aws s3 sync data/raw/ \
	s3://$(BUCKET)/$(PROJECT)/data/raw/ \
	--exclude "*" --include "*.gz"

push-processed-data: ##@data >> upload processed dataset to S3
	tar -czvf data/processed/$(ENV).dataset.tar.gz \
	--exclude=".gitkeep" \
	--exclude="*.gz" \
	--exclude=".DS_Store" \
	-C data/processed/ \
	.
	aws s3 sync data/processed/ \
	s3://$(BUCKET)/$(PROJECT)/data/processed/ \
	--exclude "*" --include "*.gz"

pull-data: ##@data >> download raw & processed dataset from S3
	$(MAKE) pull-raw-data
	$(MAKE) pull-processed-data

pull-raw-data: ##@data >> download raw dataset from S3
	aws s3 sync s3://$(BUCKET)/$(PROJECT)/data/raw/ data/raw
	tar -xvzf data/raw/$(ENV).dataset.tar.gz \
	-C data/raw/

pull-processed-data: ##@data >> download processed dataset from S3
	aws s3 sync s3://$(BUCKET)/$(PROJECT)/data/processed/ data/processed
	tar -xvzf data/processed/$(ENV).dataset.tar.gz \
	-C data/processed/

push-models: ##@model >> upload model to S3
	tar -czvf models/$(ENV).model.tar.gz \
	--exclude=".gitkeep" \
	--exclude="*.gz" \
	--exclude=".DS_Store" \
	-C models/ \
	.
	aws s3 sync models/ s3://$(BUCKET)/$(PROJECT)/models/ \
	--exclude "*" --include "*.gz"

pull-models: ##@model >> download model from S3
	aws s3 sync s3://$(BUCKET)/$(PROJECT)/models/ models/
	tar -xvzf models/$(ENV).model.tar.gz \
	-C models/
