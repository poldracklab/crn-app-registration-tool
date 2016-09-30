#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
The CRN's APP Admin Tool (cappat)
=================================


"""

from __future__ import absolute_import
from .info import (
    __version__,
    __author__,
    __email__,
    __maintainer__,
    __copyright__,
    __credits__,
    __license__,
    __status__,
    __description__,
    __longdesc__
)

import logging
logger = logging.getLogger('bidsapp')
logger.setLevel(logging.DEBUG)
