#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Utilities: Agave wrapper for sherlock
"""
import os
from os import path as op
from errno import EEXIST
import socket
from subprocess import check_output
import re

import pkg_resources as pkgr
from cappat.tpl import Template

SHERLOCK_SBATCH_TEMPLATE = pkgr.resource_filename('cappat.tpl', 'sherlock-sbatch.jnj2')
SHERLOCK_SBATCH_DEFAULTS = {
    'nodes': 1,
    'time': '01:00:00',
    'mincpus': 4,
    'mem_per_cpu': 8000,
    'modules': ['load singularity'],
    'partition': 'russpold',
    'job_name': 'crn-bidsapp',
    'job_log': 'crn-bidsapp.log'
}


class TaskManager:
    """
    A task manager factory class
    """
    @staticmethod
    def build(task_list, slurm_settings=None, temp_folder=None):
        """
        Get the appropriate TaskManager object
        """
        hostname = _gethostname()

        if not hostname:
            raise RuntimeError('Could not identify execution system')

        if hostname.endswith('ls5.tacc.utexas.edu'):
            raise NotImplementedError
        elif hostname.endswith('stanford.edu'):
            return SherlockSubmission(task_list, slurm_settings, temp_folder)
        elif hostname.endswith('stampede.tacc.utexas.edu'):
            raise NotImplementedError
        elif hostname.startswith('box') and hostname.endswith('.localdomain'):
            return CircleCISubmission(task_list, slurm_settings, temp_folder)
        else:
            raise RuntimeError(
                'Could not identify "{}" as a valid execution system'.format(hostname))


class TaskSubmissionBase(object):
    def __init__(self, task_list, slurm_settings=None, temp_folder=None):

        if not task_list:
            raise RuntimeError('a list of tasks is required')

        self.task_list = task_list
        self.jobexp = re.compile(r'Submitted batch job (?P<jobid>\d*)')

        missing = list(set(self._get_mandatory_fields()) -
                       set(list(slurm_settings.keys())))
        if missing:
            raise RuntimeError('Error filling up template with missing fields:'
                               ' {}.'.format("'%s'".join(missing)))
        self.slurm_settings = slurm_settings

        if temp_folder is None:
            temp_folder = op.join(os.getcwd(), 'log')
        _check_folder(temp_folder)
        self.temp_folder = temp_folder
        self.sbatch_files = self._generate_sbatch()
        self._job_ids = []

    @property
    def job_ids(self):
        return self._job_ids


    def _parse_jobid(self, slurm_msg):
        if isinstance(slurm_msg, (list, tuple)):
            slurm_msg = '\n'.join(slurm_msg)

        jobid = self.jobexp.search(slurm_msg).group('jobid')
        if jobid:
            self._job_ids.append(jobid)
        else:
            raise RuntimeError('Job ID could not extracted. Slurm message:\n{}'.format(
                slurm_msg))

    def _generate_sbatch(self):
        raise NotImplementedError

    def _submit_sbatch(self, task):
        raise NotImplementedError

    def _get_mandatory_fields(self):
        raise NotImplementedError

    def submit(self):
        """
        Submits a list of sbatch files and returns the assigned job ids
        """
        for task in self.sbatch_files:
            # run sbatch
            sresult = self._submit_sbatch(task)
            # parse output and get job id
            self._parse_jobid(sresult)

    def children_yield(self):
        """
        Busy wait until all jobs in the list are done
        """
        return NotImplementedError


class SherlockSubmission(TaskSubmissionBase):
    """
    The Sherlock submission
    """
    def __init__(self, task_list, slurm_settings=None, temp_folder=None):
        def_settings = SHERLOCK_SBATCH_DEFAULTS.copy()
        if not slurm_settings is None:
            def_settings.update(slurm_settings)
        def_settings['qos'] = def_settings['partition']
        super(SherlockSubmission, self).__init__(
            task_list, def_settings, temp_folder)

    def _get_mandatory_fields(self):
        return list(SHERLOCK_SBATCH_DEFAULTS.keys())

    def _generate_sbatch(self):
        """
        Generates one sbatch file per task
        """
        slurm_settings = self.slurm_settings.copy()
        sbatch_files = []
        for i, task in enumerate(self.task_list):
            sbatch_files.append(op.join(self.temp_folder, 'slurm-%06d.sbatch' % i))
            slurm_settings['commandline'] = task
            conf = Template(SHERLOCK_SBATCH_TEMPLATE)
            conf.generate_conf(slurm_settings, sbatch_files[-1])
        return sbatch_files

    def _submit_sbatch(self, task):
        return check_output(['sbatch', task])


class CircleCISubmission(SherlockSubmission):
    """
    A CircleCI submission manager to work with the slurm docker image
    """
    def _generate_sbatch(self):
        """
        Generates one sbatch file per task
        """
        # Remove default settings of Sherlock not supported
        self.slurm_settings.pop('qos', None)
        self.slurm_settings.pop('mincpus', None)
        self.slurm_settings.pop('mem_per_cpu', None)
        self.slurm_settings.pop('modules', None)
        return super(CircleCISubmission, self)._generate_sbatch()

    def _submit_sbatch(self, task):
        task = os.path.basename(task)
        return check_output([
            'sshpass', '-p', 'testuser',
            'ssh', '-p', '10022', 'testuser@localhost',
            'sbatch', os.path.join('/scratch/slurm', task)])


def _gethostname():
    hostname = socket.gethostname()

    if len(hostname.strip('.')) == 1 and hostname.startswith('login'):
        # This is here because ls5 returns only the login node name 'loginN'
        fqdns = list(
            set([socket.getfqdn(i[4][0])
                 for i in socket.getaddrinfo(socket.gethostname(), None)]))
        hostname = fqdns[0]
    return hostname

def _check_folder(folder):
    if not op.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as exc:
            if not exc.errno == EEXIST:
                raise
