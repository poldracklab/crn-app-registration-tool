#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
The cappat manager factory
"""
from __future__ import absolute_import, division, print_function, unicode_literals
import logging

from .slurm import (SherlockSubmission, CircleCISubmission, TestSubmission)
from .launcher import (LauncherSubmission, Lonestar5Submission)
from .tools import getsystemname as _getsystemname

JOB_LOG = logging.getLogger('taskmanager')


class TaskManager(object):
    """
    A task manager factory class
    """
    def __init__(self):
        raise RuntimeError('This class cannot be instatiated.')

    @staticmethod
    def build(task_list, settings=None, work_dir=None,
              hostname=None):
        """
        Get the appropriate TaskManager object
        """
        hostname = settings.get('execution_system', None)

        if hostname is None:
            hostname = _getsystemname()

        JOB_LOG.info('Identified host: "%s"', hostname)

        if not hostname:
            raise RuntimeError('Could not identify execution system')

        if hostname.endswith('ls5.tacc.utexas.edu'):
            return Lonestar5Submission(task_list, settings, work_dir)
        elif hostname.endswith('stampede.tacc.utexas.edu'):
            return LauncherSubmission(task_list, settings, work_dir)
        elif hostname.endswith('stanford.edu'):
            return SherlockSubmission(task_list, settings, work_dir)
        elif hostname == 'test.circleci':
            return CircleCISubmission(task_list, settings, work_dir)
        elif hostname == 'test.local':
            return TestSubmission(task_list, settings, work_dir)
        else:
            raise RuntimeError(
                'Could not identify "{}" as a valid execution system'.format(hostname))
