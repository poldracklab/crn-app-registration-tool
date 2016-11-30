#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
""" A wrapper for systems with slurm """
from __future__ import absolute_import, division, print_function, unicode_literals

from builtins import str
import logging
import subprocess as sp

JOB_LOG = logging.getLogger('taskmanager')


def run_cmd(cmd, shell=False, env=None):
    """Runs a command line"""

    JOB_LOG.info('Executing command line: %s', ' '.join(cmd))
    try:
        result = str(sp.check_output(cmd, stderr=sp.STDOUT, shell=shell, env=env))
    except sp.CalledProcessError as error:
        JOB_LOG.critical('Error submitting (exit code %d): \n\tCmdline: %s\n\tOutput:\n\t%s',
                         error.returncode, ' '.join(cmd), error.output)
        raise
    result = '\n'.join([line for line in result.split('\n') if line.strip()])
    if not result:
        JOB_LOG.info('Command output was empty')
        return None

    JOB_LOG.info('Command output: \n%s', result)
    return result

def format_modules(modules_list):
    if not modules_list:
        return None

    if isinstance(modules_list, list):
        modules_list = ' '.join(modules_list)
    modules_list = modules_list.split(' ')

    JOB_LOG.info('Formatting modules list...')
    modules_load = []
    modules_use = []
    _is_load = False
    for i, mod in enumerate(modules_list):
        if mod == 'module':
            _is_load = False
        elif mod == 'use':
            _is_load = False
            modules_use.append(modules_list[i+1])
        elif mod == 'load':
            _is_load = True
        elif _is_load:
            modules_load.append(mod)

    modtext = []
    if modules_use:
        modtext.append('module use ' + ' '.join(modules_use))

    if not modules_load:
        JOB_LOG.warn('No modules to load were found.')
    else:
        modtext.append('module load ' + ' '.join(modules_load))

    return modtext

def time_fraction(timestr, fraction=0.90):
    """Returns a time string which is the fraction of the input"""
    return _secs2time(int(fraction * _time2secs(timestr)))

def _time2secs(timestr):
    return sum((60**i) * int(t) for i, t in enumerate(reversed(timestr.split(':'))))

def _secs2time(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)

