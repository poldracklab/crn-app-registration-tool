#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Utilities: Agave wrapper for sherlock
"""
import os
from os import path as op
from subprocess import check_output
import re
from time import sleep
import logging

import pkg_resources as pkgr
from cappat import AGAVE_JOB_LOGS
from cappat.tpl import Template
from cappat.utils import check_folder, gethostname

SLURM_FAIL_STATUS = ['CA', 'F', 'TO', 'NF', 'SE']
SLURM_WAIT_STATUS = ['R', 'PD', 'CF', 'CG']
SLEEP_SECONDS = 5

JOB_LOG = logging.getLogger('taskmanager')

class TaskManager:
    """
    A task manager factory class
    """
    @staticmethod
    def build(task_list, slurm_settings=None, temp_folder=None,
              hostname=None):
        """
        Get the appropriate TaskManager object
        """
        if hostname is None:
            hostname = gethostname()

        JOB_LOG.info('Identified host: "%s"', hostname)

        if not hostname:
            raise RuntimeError('Could not identify execution system')

        if hostname.endswith('ls5.tacc.utexas.edu'):
            raise NotImplementedError
        elif hostname.endswith('stanford.edu'):
            return SherlockSubmission(task_list, slurm_settings, temp_folder)
        elif hostname.endswith('stampede.tacc.utexas.edu'):
            raise NotImplementedError
        elif hostname == 'test.circleci' or (hostname.startswith('box') and
                                             hostname.endswith('.localdomain')):
            return CircleCISubmission(task_list, slurm_settings, temp_folder)
        elif hostname == 'test.local':
            return TestSubmission(task_list, slurm_settings, temp_folder)
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
            temp_folder = AGAVE_JOB_LOGS

        self.temp_folder = check_folder(op.abspath(temp_folder))
        self.sbatch_files = self._generate_sbatch()
        self._job_ids = []

        JOB_LOG.info('Created TaskManager type "%s"', self.__class__.__name__)

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
        return jobid

    def _generate_sbatch(self):
        raise NotImplementedError

    def _submit_sbatch(self, task):
        raise NotImplementedError

    def _get_job_status(self, jobid):
        raise NotImplementedError

    def submit(self):
        """
        Submits a list of sbatch files and returns the assigned job ids
        """
        for i, task in enumerate(self.sbatch_files):
            JOB_LOG.info('Submitting sbatch/launcher file %s (%d)', task, i)
            # run sbatch
            sresult = self._submit_sbatch(task)
            # parse output and get job id
            jobid = self._parse_jobid(sresult)
            JOB_LOG.info(
                'Submitted task %d, job ID %s was assigned', i, jobid)

    def children_yield(self):
        """
        Busy wait until all jobs in the list are done
        """
        JOB_LOG.info('Starting busy wait on jobs %s', ' '.join(self._job_ids))
        finished_jobs = [False] * len(self._job_ids)
        while not all(finished_jobs):
            for i, jobid in enumerate(self._job_ids):
                if finished_jobs[i]:
                    continue
                status = self._get_job_status(jobid)
                if status in SLURM_FAIL_STATUS:
                    raise RuntimeError('Job id {} failed with status {}.'.format(
                        jobid, status))
                if status in SLURM_WAIT_STATUS:
                    continue
                else:
                    JOB_LOG.info('Job %s finished.', jobid)
                    finished_jobs[i] = True

            pending = [jid for jid, jdone in zip(self._job_ids, finished_jobs) if not jdone]
            JOB_LOG.info('There are pending jobs: %s', ' '.join(pending))
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

    def _get_job_status(self, jobid):
        return check_output([
            'squeue', '-j', jobid, '-o', '%t', '-h']).strip()


class CircleCISubmission(SherlockSubmission):
    """
    A CircleCI submission manager to work with the slurm docker image
    """
    slurm_settings = {
        'nodes': 1,
        'time': '01:00:00',
        'partition': 'debug',
        'job_name': 'crn-bidsapp',
        'job_log': 'crn-bidsapp.log'
    }
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
        task = task.replace(os.path.expanduser('~/'), '/')
        task = task.replace('~/', '/')

        try:
            result = check_output([
                'sshpass', '-p', 'testuser',
                'ssh', '-p', '10022', 'testuser@localhost',
                'sbatch', task])
        except Exception as exc:
            JOB_LOG.critical(
                'Error submitting %s: \n\t%s', task, result)
            JOB_LOG.error(
                'Exception message: \n%s', str(exc))
            raise

        return result

    def _get_job_status(self, jobid):
        return check_output([
            'sshpass', '-p', 'testuser',
            'ssh', '-p', '10022', 'testuser@localhost',
            'squeue', '-j', jobid, '-o', '%t', '-h']).strip()

class TestSubmission(SherlockSubmission):
    """
    A Test submission manager to work with the slurm docker image
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
        return super(TestSubmission, self)._generate_sbatch()

    def _submit_sbatch(self, task):
        task = task.replace('~/', '/')
        task = task.replace(os.path.expanduser('~/'), '/')

        return check_output([
            'echo', '"Submitted batch job 49533"'])

    def _get_job_status(self, jobid):
        return check_output([
            'echo', 'FINISHED']).strip()
