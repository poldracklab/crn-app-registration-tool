#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Utilities: helper functions
"""
import os
from os import path as op
import socket
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


def getsystemname(check_env=True):
    """
    Queries the host name. If for some reason (i.e. ls5) it returns
    not enough information to identify the host, queries all the IPs
    """

    if check_env:
        hostname = os.getenv('AGAVE_EXECUTION_SYSTEM')

        if hostname is not None:
            return hostname

    hostname = socket.gethostname()

    if '.' not in hostname:
        # This is here because ls5 returns only the node name
        fqdns = list(
            set([socket.getfqdn(i[4][0])
                 for i in socket.getaddrinfo(socket.gethostname(), None)]))
        hostname = fqdns[0]
    return hostname
