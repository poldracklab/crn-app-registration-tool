#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

import mock
import cappat.manager.tools as cmt


@mock.patch('cappat.manager.tools.socket.gethostname',
            mock.Mock(return_value='nid00017'))
@mock.patch('cappat.manager.tools.socket.getfqdn',
            mock.Mock(return_value='ls5.tacc.utexas.edu'))
@mock.patch('cappat.manager.tools.socket.getaddrinfo',
            mock.Mock(return_value=[(2, 2, 17, '', ('192.168.0.24', 0))]))
def test_getsystemname():
    """
    Mocks the return values in ls5, to ensure that this implementation
    of the hostname works fine
    """
    assert cmt.getsystemname() == 'ls5.tacc.utexas.edu'

# def test_getsystemname_env(monkeypatch):
#     """Use monkeypatch to fake the env variable"""
#     monkeypatch.setenv('AGAVE_EXECUTION_SYSTEM', 'test.local')
#     assert cu.getsystemname() == 'test.local'
