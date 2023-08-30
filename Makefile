.PHONY: clean build publish test install

VERSION ?= $(shell git describe --tags | sed 's/-/\+/' | sed 's/-/\./g')
REPO ?= https://upload.pypi.org/legacy/
REPO_USER ?= __token__
REPO_PASS ?= unset

version:
	sed -i.bak "s/__version__ .*/__version__ = \"$(VERSION)\"/" harmony/__init__.py && rm harmony/__init__.py.bak

build: clean version
	python -m pip install --upgrade --quiet setuptools wheel twine
	python setup.py --quiet sdist bdist_wheel

publish: build
	python -m twine check dist/*
	python -m twine upload --username "$(REPO_USER)" --password "$(REPO_PASS)" --repository-url "$(REPO)" dist/*

clean:
	rm -rf build dist *.egg-info || true

# HARMONY-1188 - revert this command to:
# pip install -e .[dev]
install:
	pip install -r dev-requirements.txt
	pip install -r requirements.txt

lint:
	flake8 harmony

test:
	pytest --cov=harmony tests

test-no-warnings:
	pytest --disable-warnings --cov=harmony tests

cve-check:
	safety check
