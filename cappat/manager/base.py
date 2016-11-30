#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Utilities: Agave wrapper for sherlock
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
from os import path as op
import re
from time import sleep
import logging
from pprint import pformat as pf
from pkg_resources import resource_filename as pkgrf
from builtins import object

from cappat import AGAVE_JOB_LOGS, AGAVE_JOB_OUTPUT
from ..tpl import Template
from ..utils import check_folder

from .tools import (
    time_fraction as _tf,
    format_modules as _format_modules,
    run_cmd as _run_cmd)

SLURM_FAIL_STATUS = ['CA', 'F', 'TO', 'NF', 'SE']
SLURM_WAIT_STATUS = ['R', 'PD', 'CF', 'CG']
SLEEP_SECONDS = 5

JOB_LOG = logging.getLogger('taskmanager')

class TaskSubmissionBase(object):
    """
    A base class for task submission
    """
    _settings = {}
    jobexp = re.compile(r'Submitted batch job (?P<jobid>\d*)')
    _cmd_prefix = []

    SLURM_TEMPLATE = None
    GROUP_TEMPLATE = op.abspath(pkgrf('cappat', 'tpl/group-wrapper.jnj2'))

    def __init__(self, task_list, settings=None, work_dir=None):

        if not task_list:
            raise RuntimeError('a list of tasks is required')

        self.task_list = task_list
        self._jobs = {}

        if settings is not None:
            self._settings.update(settings)

        if work_dir is None:
            work_dir = os.getcwd()

        self._settings['child_runtime'] = _tf(self._settings['max_runtime'])

        self.work_dir = check_folder(op.abspath(work_dir))
        self.aux_dir = check_folder(op.join(self.work_dir, AGAVE_JOB_LOGS))
        self._settings.update(
            {'work_dir': self.work_dir, 'aux_dir': self.aux_dir}
        )

        self._group_cmd = [self._settings['executable'], self._settings['bids_dir'],
                           AGAVE_JOB_OUTPUT, 'group']
        if self._settings.get('group_args'):
            self._group_cmd += [self._settings.get('group_args')]

        JOB_LOG.info('Automatically inferred group level command: "%s"',
                     ' '.join(self.group_cmd))

        self._settings['modules'] = _format_modules(self._settings.get('modules', []))
        JOB_LOG.info('Created TaskManager type "%s" with default settings: \n\t%s',
                     self.__class__.__name__, pf(self._settings))

    @property
    def job_ids(self):
        return list(self._jobs.keys())

    @property
    def jobs(self):
        return self._jobs

    @property
    def group_cmd(self):
        return self._group_cmd

    @group_cmd.setter
    def group_cmd(self, value):
        if not isinstance(value, list):
            value = [value]

        self._group_cmd = value

    def _parse_jobid(self, slurm_msg):
        if isinstance(slurm_msg, (list, tuple)):
            slurm_msg = '\n'.join(slurm_msg)

        jobid = self.jobexp.search(slurm_msg).group('jobid')
        if jobid:
            self._jobs[jobid] = 'SUBMITTED'
        else:
            raise RuntimeError('Job ID could not extracted. Slurm message:\n{}'.format(
                slurm_msg))
        return jobid

    def _generate_sbatch(self):
        raise NotImplementedError

    def _submit_sbatch(self, task):
        return _run_cmd(self._cmd_prefix + ['sbatch', task])

    def _run_sacct(self):
        # sacct -n -X -j 10016750,10016749 -o JobID,State,ExitCode
        return _run_cmd(self._cmd_prefix + [
            'sacct', '-n', '-X', '-j', ','.join(self.job_ids),
            '-o', 'JobID,State,ExitCode'])

    def _get_job_acct(self):
        JOB_LOG.info('Checking exit code of jobs %s', ' '.join(self.job_ids))
        results = self._run_sacct()

        if results is None:
            JOB_LOG.critical('Running sacct over jobs %s did not produce any output',
                             ', '.join(self.job_ids))
            raise RuntimeError('sacct command output is empty')

        #parse results
        regexp = re.compile('(?P<jobid>\\d*) +(?P<status>\\w*)\\+? +'
                            '(?P<exit_code>\\d+):\\d+')
        exit_codes = []
        for line in results.split('\n'):
            m = regexp.search(line)
            if m is not None and all(m.groups()):
                self._jobs[m.group('jobid')] = m.group('status')
                exit_codes.append(int(m.group('exit_code')))
        return exit_codes

    def _get_jobs_status(self):
        squeue = _run_cmd(self._cmd_prefix + [
            'squeue', '-j', ','.join(self.job_ids), '-o', '%i,%t', '-h'])

        # Jobs are not in the queue anymore
        if squeue is None:
            JOB_LOG.warn('Command "squeue" was empty: jobs are completed.')
            return True

        if 'Invalid job id specified' in squeue:
            JOB_LOG.warn('Jobs completed - squeue: %s', squeue)
            return True

        pending = []
        sqexp = re.compile('(?P<jobid>\\d*),(?P<jobstatus>[' +
                           '|'.join(SLURM_WAIT_STATUS + SLURM_FAIL_STATUS) + ']*)')
        statuses = squeue.split('\n')
        for line in statuses:
            m = sqexp.search(line)
            if m is not None and all(m.groups()):
                status = m.groups()
                self._jobs[status[0]] = status[1]

                if status[1] in SLURM_FAIL_STATUS:
                    JOB_LOG.warn('Job id %s failed (%s).', *status)
                else:
                    pending.append(status[0])

        if pending:
            JOB_LOG.info('There are pending jobs: %s', ' '.join(pending))
            return False

        JOB_LOG.info('Jobs %s not present in squeue list, finishing polling.',
                     ', '.join(self.job_ids))
        return True

    def map_participant(self):
        """
        Submits a list of sbatch files and returns the assigned job ids
        """
        sbatch_files = self._generate_sbatch()
        for i, task in enumerate(sbatch_files):
            JOB_LOG.info('Submitting sbatch/launcher file %s (%d)', task, i)
            # run sbatch
            sresult = self._submit_sbatch(task)
            # parse output and get job id
            jobid = self._parse_jobid(sresult)
            JOB_LOG.info(
                'Submitted task %d, job ID %s was assigned', i, jobid)

    def wait_participant(self):
        """
        Busy wait until all jobs in the list are done
        """
        JOB_LOG.info('Starting busy wait on jobs %s',
                     ' '.join(self.job_ids))
        all_finished = False

        while not all_finished:
            all_finished = self._get_jobs_status()
            sleep(SLEEP_SECONDS)

        JOB_LOG.info('Finished wait on jobs %s', ', '.join(self.job_ids))

        # Run sacct to check the exit code of jobs
        overall_exit = sum(self._get_job_acct())

        JOB_LOG.info('Final status of jobs: %s', ', '.join([
            '%s (%s)' % (k, v) for k, v in list(self._jobs.items())]))

        if overall_exit > 0:
            failed_jobs = ['{0} (logfiles: log/bidsapp-{0}.{{err,out}}).'.format(k) for k, v in list(
                self._jobs.items()) if v != 'COMPLETED']
            JOB_LOG.critical('One or more tasks finished with non-zero code:\n'
                             '\t%s', '\n\t'.join(failed_jobs))
            raise RuntimeError('One or more tasks finished with non-zero code')
        return self.job_ids

    def run_grouplevel(self):
        """
        Run the reduce operation over the participant map
        """
        if not self.group_cmd:
            JOB_LOG.warning('Group level command not set, skipping reduce operation.')
            return True

        JOB_LOG.info('Kicking off reduce operation')
        group_wrapper = 'group-wrapper.sh'
        conf = Template(self.GROUP_TEMPLATE)
        conf.generate_conf({
            'modules': self._settings.get('modules', []),
            'cmdline': ' '.join(self.group_cmd)
        }, group_wrapper)

        if _run_cmd(['/bin/bash', group_wrapper]):
            JOB_LOG.info('Group level finished successfully.')
            return True
        return False
