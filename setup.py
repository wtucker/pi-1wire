#!/usr/bin/env python

import os
import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


setup(
    name = "pi-1wire",
    version = "0.0.1",
    author = "Wayne Tucker",
    author_email = "wayne@tuckerlabs.com",
    description = ("1-Wire temperature controller service for Raspberry Pi"),
    license = "BSD",
    keywords = "raspberry pi sous vide homebrew",
    install_requires = [
        'pyyaml>=3.10',
        'flask>=0.8',
        'requests>=2.0.0'
    ],
    packages = ['pi_1wire'],
    tests_require = [
        'pytest>=2.5.2',
        'pytest-cov>=1.6',
        'nose>=1.1.2',
        'mock>=1.0.1'
    ],
    cmdclass = {
        'test': PyTest,
    },
    long_description = read('README.md'),
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Topic :: Home Automation",
        "License :: OSI Approved :: BSD License",
    ],
)
