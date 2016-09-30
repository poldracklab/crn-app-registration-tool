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
from time import sleep

import pkg_resources as pkgr
from cappat.tpl import Template

SLURM_FAIL_STATUS = ['CA', 'F', 'TO', 'NF', 'SE']
SLURM_WAIT_STATUS = ['R', 'PD', 'CF', 'CG']
SLEEP_SECONDS = 5

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
    slurm_settings = {
        'nodes': 1,
        'time': '01:00:00',
        'job_name': 'crn-bidsapp',
        'job_log': 'crn-bidsapp.log'
    }
    jobexp = re.compile(r'Submitted batch job (?P<jobid>\d*)')

    SLURM_TEMPLATE = pkgr.resource_filename('cappat.tpl', 'sherlock-sbatch.jnj2')

    def __init__(self, task_list, slurm_settings=None, temp_folder=None):

        if not task_list:
            raise RuntimeError('a list of tasks is required')

        self.task_list = task_list

        if slurm_settings is not None:
            self.slurm_settings.update(slurm_settings)

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
        finished_jobs = [False] * len(self._job_ids)
        while not all(finished_jobs):
            for i, jobid in enumerate(self._job_ids):
                if finished_jobs[i]:
                    continue

                status = check_output([
                    'squeue', '-j', jobid, '-o', '%t', '-h']).strip()

                if status in SLURM_FAIL_STATUS:
                    raise RuntimeError('Job id {} failed with status {}.'.format(
                        jobid, status))
                if status in SLURM_WAIT_STATUS:
                    continue
                else:
                    finished_jobs[i] = True

            sleep(SLEEP_SECONDS)

        return self._job_ids


class SherlockSubmission(TaskSubmissionBase):
    """
    The Sherlock submission
    """
    slurm_settings = {
        'nodes': 1,
        'time': '01:00:00',
        'mincpus': 4,
        'mem_per_cpu': 8000,
        'modules': ['load singularity'],
        'partition': 'russpold',
        'qos': 'russpold',
        'job_name': 'crn-bidsapp',
        'job_log': 'crn-bidsapp.log'
    }

    def __init__(self, task_list, slurm_settings=None, temp_folder=None):
        if not slurm_settings is None:
            self.slurm_settings.update(slurm_settings)
        self.slurm_settings['qos'] = self.slurm_settings['partition']
        super(SherlockSubmission, self).__init__(
            task_list, temp_folder=temp_folder)

    def _generate_sbatch(self):
        """
        Generates one sbatch file per task
        """
        slurm_settings = self.slurm_settings.copy()
        sbatch_files = []
        for i, task in enumerate(self.task_list):
            sbatch_files.append(op.join(self.temp_folder, 'slurm-%06d.sbatch' % i))
            slurm_settings['commandline'] = task
            conf = Template(self.SLURM_TEMPLATE)
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
