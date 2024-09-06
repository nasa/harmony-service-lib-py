#!/usr/bin/env python
# -*- coding: utf-8 -*-

# For a fully annotated version of this file and what it does, see
# https://github.com/pypa/sampleproject/blob/master/setup.py

# To upload this file to PyPI you must build it then upload it:
# python setup.py sdist bdist_wheel  # build in 'dist' folder
# python-m twine upload dist/*  # 'twine' must be installed: 'pip install twine'

import ast
import io
import re
import os
from setuptools import find_packages, setup

DEPENDENCIES = []
with open("requirements.txt", "r") as f:
    DEPENDENCIES = f.read().strip().split('\n')

DEV_DEPENDENCIES = []
with open("dev-requirements.txt", "r") as f:
    DEV_DEPENDENCIES = f.read().strip().split('\n')

EXCLUDE_FROM_PACKAGES = ["contrib", "docs", "tests*"]
CURDIR = os.path.abspath(os.path.dirname(__file__))

with io.open(os.path.join(CURDIR, "README.md"), "r", encoding="utf-8") as f:
    README = f.read()


def get_version():
    main_file = os.path.join(CURDIR, "harmony_service_lib", "__init__.py")
    _version_re = re.compile(r"__version__\s+=\s+(?P<version>.*)")
    with open(main_file, "r", encoding="utf8") as f:
        match = _version_re.search(f.read())
        version = match.group("version") if match is not None else '"unknown"'
    return str(ast.literal_eval(version))


setup(
    name="harmony-service-lib",
    version=get_version(),
    author="NASA EOSDIS Harmony Team",
    author_email="patrick@element84.com",
    description=("A library for Python-based Harmony services to parse incoming messages, "
                 "fetch data, stage data, and call back to Harmony"),
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/nasa/harmony-service-lib-py",
    packages=find_packages(exclude=EXCLUDE_FROM_PACKAGES),
    include_package_data=True,
    keywords=[],
    scripts=[],
    entry_points={
        "console_scripts": ["harmony-service-lib=harmony_service_lib.cli.__main__:main"]
    },
    zip_safe=False,
    install_requires=DEPENDENCIES,
    # HARMONY-1188 - uncomment this
    # extras_require={
    #     'dev': DEV_DEPENDENCIES
    # },
    test_suite="tests",
    python_requires=">=3.8",
    # license and classifier list:
    # https://pypi.org/pypi?%3Aaction=list_classifiers
    license="License :: OSI Approved :: Apache Software License",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent"
    ],
)