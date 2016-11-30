#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Utilities: helper functions
"""
import os
from os import path as op
from errno import EEXIST

def check_folder(folder):
    """
    Creates a folder if it does not exist
    """
    if not op.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as exc:
            if not exc.errno == EEXIST:
                raise
    return folder
