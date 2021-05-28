.PHONY: clean build publish test develop

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

develop:
	pip install -e .[dev]

lint:
	flake8 harmony

test:
	pytest --cov=harmony tests

cve-check:
	safety check
