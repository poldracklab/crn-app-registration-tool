#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Base module variables
"""
from __future__ import absolute_import, division, print_function, unicode_literals
from datetime import datetime

__packagename__ = 'cappat'
__version__ = '0.0.4'
__wrapperver__ = '0.0.3'
__author__ = 'Oscar Esteban'
__credits__ = ['Oscar Esteban', 'Chris F. Gorgolewski']
__license__ = 'Apache Software License'
__maintainer__ = 'The CRN developers'
__email__ = 'crn.poldracklab@gmail.com'
__status__ = 'Prototype'
__copyright__ = 'Copyright {}, {}'.format(datetime.now().year, __author__)

__description__ = """The CRN's APP Administration Tool (cappat)."""
__longdesc__ = """\
The CRN's APP Administration Tool (cappat) is a simple utility to build
and register BIDS-Apps into the CRN/Agave framework.
"""

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Science/Research',
    'Intended Audience :: Information Technology',
    'Intended Audience :: System Administrators',
    'Topic :: System :: Installation/Setup',
    'Topic :: System :: Installation/Setup',
    'Topic :: Utilities',
    'License :: OSI Approved :: {}'.format(__license__),
    'Programming Language :: Python :: 2.7',
]

DOWNLOAD_URL = 'https://github.com/poldracklab/{}'.format(__packagename__)
URL = 'http://{}.readthedocs.io/'.format(__packagename__)

# Dependencies
REQUIRES = [
    'jinja2',
    'PyYAML',
    'future'
]

# Required before running setup()
SETUP_REQUIRES = []

# Dependencies to be fetched from urls (e.g. github repos)
LINKS_REQUIRES = []

# Dependencies to install for testing (e.g. nose or pytest)
TESTS_REQUIRES = [
    'pytest-xdist',
    'mock'
]

# Dependencies to install for extra features
# For now, only documentation is enabled. Install with pip install -e .[doc]
EXTRA_REQUIRES = {
    'appgen': ['agavepy'],
    'doc': ['sphinx'],
    'tests': TESTS_REQUIRES,
}

# Enable a handle to install all extra dependencies at once
# with pip install -e .[all]
EXTRA_REQUIRES['all'] = [val for _, val in list(EXTRA_REQUIRES.items())]
