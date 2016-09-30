#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Utilities: helper functions
"""
import os
import socket
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


def gethostname():
    """
    Queries the host name. If for some reason (i.e. ls5) it returns
    not enough information to identify the host, queries all the IPs
    """
    hostname = socket.gethostname()

    if len(hostname.strip('.')) == 1 and hostname.startswith('login'):
        # This is here because ls5 returns only the login node name 'loginN'
        fqdns = list(
            set([socket.getfqdn(i[4][0])
                 for i in socket.getaddrinfo(socket.gethostname(), None)]))
        hostname = fqdns[0]
    return hostname
