#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
""" A wrapper for systems with slurm """
from __future__ import absolute_import, division, print_function, unicode_literals

import os.path as op
import logging
from pprint import pformat as pf
from pkg_resources import resource_filename as pkgrf

from ..tpl import Template
from .base import TaskSubmissionBase
from .tools import run_cmd as _run_cmd

JOB_LOG = logging.getLogger('taskmanager')


class SherlockSubmission(TaskSubmissionBase):
    """
    The Sherlock submission
    """
    SLURM_TEMPLATE = op.abspath(pkgrf('cappat', 'tpl/sherlock-sbatch.jnj2'))

    def __init__(self, task_list, settings=None, work_dir=None):
        super(SherlockSubmission, self).__init__(
            task_list, settings=settings, work_dir=work_dir)
        self._settings['qos'] = self._settings['partition']

    def _generate_sbatch(self):
        """
        Generates one sbatch file per task
        """
        settings = self._settings.copy()
        JOB_LOG.info('Generating sbatch files with the following settings: \n\t%s',
                     pf(settings))
        sbatch_files = []
        for i, task in enumerate(self.task_list):
            sbatch_files.append(op.join(self.aux_dir, 'slurm-%06d.sbatch' % i))
            settings['commandline'] = task
            conf = Template(self.SLURM_TEMPLATE)
            conf.generate_conf(settings, sbatch_files[-1])
        return sbatch_files


class CircleCISubmission(SherlockSubmission):
    """
    A CircleCI submission manager to work with the slurm docker image
    """
    settings = {
        'nodes': 1,
        'time': '01:00:00',
        'partition': 'debug',
        'job_name': 'crn-bidsapp'
    }
    _cmd_prefix = ['sshpass', '-p', 'testpass',
                   'ssh', '-p', '10022', 'circleci@localhost']

    def _generate_sbatch(self):
        """
        Generates one sbatch file per task
        """
        # Remove default settings of Sherlock which are unsupported
        self._settings.pop('qos', None)
        self._settings.pop('mincpus', None)
        self._settings.pop('mem_per_cpu', None)
        self._settings.pop('modules', None)
        self._settings['work_dir'] = self._settings['work_dir'].replace(
            op.expanduser('~/'), '/')
        self._settings['work_dir'] = self._settings['work_dir'].replace(
            '~/', '/')
        return super(CircleCISubmission, self)._generate_sbatch()

    def _submit_sbatch(self, task):
        # Fix paths for docker image in CircleCI
        task = task.replace(op.expanduser('~/'), '/')
        task = task.replace('~/', '/')
        return super(CircleCISubmission, self)._submit_sbatch(task)

class TestSubmission(SherlockSubmission):
    """
    A Test submission manager to work with the slurm docker image
    """
    def _generate_sbatch(self):
        """
        Generates one sbatch file per task
        """
        # Remove default settings of Sherlock not supported
        self._settings.pop('qos', None)
        self._settings.pop('mincpus', None)
        self._settings.pop('mem_per_cpu', None)
        self._settings.pop('modules', None)
        self._settings.pop('srun_cmd', None)
        return super(TestSubmission, self)._generate_sbatch()

    def _submit_sbatch(self, task):
        return _run_cmd(['/bin/bash', task])

    def _get_jobs_status(self):
        jobs = ['%s,COMPLETED' % j for j in self.job_ids]
        return _run_cmd(['echo', '\n'.join(jobs)]).strip()

    def _run_sacct(self):
        return '\n'.join(['%s  COMPLETED  0:0' % j for j in self.job_ids])
