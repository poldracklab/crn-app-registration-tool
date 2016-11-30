#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
""" A wrapper for systems with launcher """
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import os.path as op
import logging
from pkg_resources import resource_filename as pkgrf
from io import open
from ..tpl import Template
from .base import TaskSubmissionBase

JOB_LOG = logging.getLogger('taskmanager')

class LauncherSubmission(TaskSubmissionBase):
    """
    The cappat submission manager using launcher
    """
    _cmd_prefix = ['ssh', '-oStrictHostKeyChecking=no', 'login2']
    SLURM_MAXNODES = 40
    SLURM_MAXCPUS = 16
    SLURM_TEMPLATE = op.abspath(pkgrf('cappat', 'tpl/sbatch-launcher-3.0.jnj2'))

    def _generate_sbatch(self):
        """
        Generates one launcher file
        """

        tasks_file = op.join(self.aux_dir, 'tasks_list.sh')
        batch_file = op.join(self.aux_dir, 'launcher.sbatch')
        with open(tasks_file, 'w') as lfh:
            lfh.write('\n'.join(self.task_list) + '\n')

        settings = {
            'nodes': min([len(self.task_list), self.SLURM_MAXNODES]),
            'ntasks': len(self.task_list),
            'runtime': self._settings['child_runtime'],
            'partition': self._settings.get('partition', 'normal'),
            'jobname': self._settings.get('job_name', 'openneuro'),
            'work_dir': os.getcwd(),
            'tasks_file': tasks_file,
            'ncpus': self._settings.get('ncpus', self.SLURM_MAXCPUS),
            'tasks_per_node': 1,
        }

        conf = Template(self.SLURM_TEMPLATE)
        conf.generate_conf(settings, batch_file)

        return [batch_file]


class Lonestar5Submission(LauncherSubmission):
    """
    The LS5 submission manager
    """
    SLURM_MAXCPUS = 24
