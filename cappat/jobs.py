#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Utilities: Agave wrapper for sherlock
"""
import os
from os import path as op
from subprocess import check_output, CalledProcessError, STDOUT
import re
from time import sleep
import logging

import pkg_resources as pkgr
from cappat import AGAVE_JOB_LOGS
from cappat.tpl import Template
from cappat.utils import check_folder, getsystemname

SLURM_FAIL_STATUS = ['CA', 'F', 'TO', 'NF', 'SE']
SLURM_WAIT_STATUS = ['R', 'PD', 'CF', 'CG']
SLEEP_SECONDS = 5

JOB_LOG = logging.getLogger('taskmanager')


class TaskManager(object):
    """
    A task manager factory class
    """
    def __init__(self):
        raise RuntimeError('This class cannot be instatiated.')

    @staticmethod
    def build(task_list, slurm_settings=None, temp_folder=None,
              hostname=None):
        """
        Get the appropriate TaskManager object
        """
        if hostname is None:
            hostname = getsystemname()

        JOB_LOG.info('Identified host: "%s"', hostname)

        if not hostname:
            raise RuntimeError('Could not identify execution system')

        if hostname.endswith('ls5.tacc.utexas.edu'):
            return Lonestar5Submission(task_list, slurm_settings, temp_folder)
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
    """
    A base class for task submission
    """
    slurm_settings = {
        'nodes': 1,
        'time': '01:00:00',
        'job_name': 'crn-bidsapp',
        'job_log': 'crn-bidsapp.log'
    }
    jobexp = re.compile(r'Submitted batch job (?P<jobid>\d*)')

    SLURM_TEMPLATE = pkgr.resource_filename('cappat.tpl', 'sherlock-sbatch.jnj2')

    def __init__(self, task_list, slurm_settings=None, group_cmd=None, temp_folder=None):

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
        self._group_cmd = group_cmd

        JOB_LOG.info('Created TaskManager type "%s"', self.__class__.__name__)

    @property
    def job_ids(self):
        return self._job_ids

    @property
    def group_cmd(self):
        return self._group_cmd

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
        return _run_cmd(['squeue', '-j', jobid, '-o', '%t', '-h']).strip()

    def map_participant(self):
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

    def wait_participant(self):
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

            if all(finished_jobs):
                break

            pending = [jid for jid, jdone in zip(self._job_ids, finished_jobs) if not jdone]
            JOB_LOG.info('There are pending jobs: %s', ' '.join(pending))
            sleep(SLEEP_SECONDS)

        JOB_LOG.info('Finished wait on jobs %s', ' '.join(self._job_ids))
        return self._job_ids

    def run_grouplevel(self):
        """
        Run the reduce operation over the participant map
        """
        if self._group_cmd is None:
            JOB_LOG.warning('Group level command not set, skipping reduce operation.')
            return None

        JOB_LOG.info('Kicking off reduce operation')
        return _run_cmd(self._group_cmd)

class Lonestar5Submission(TaskSubmissionBase):
    """
    The LS5 submission manager
    """
    slurm_settings = {
        'nodes': 1,
        'time': '01:00:00',
        'mincpus': 1,
        'mem_per_cpu': 8000,
        'job_name': 'crn-bidsapp',
        'job_log': 'crn-bidsapp.log'
    }

    def _generate_sbatch(self):
        """
        Generates one launcher file
        """
        launcher_file = op.join(self.temp_folder, 'launch_script.sh')
        with open(launcher_file, 'w') as lfh:
            lfh.write('\n'.join(self.task_list))
        return [launcher_file]

    def _submit_sbatch(self, task):
        with open(task, 'r') as tfh:
            nodes = sum(1 for line in tfh if line.strip() and not line.strip().startswith('#'))
        maxruntime = _secs2time(int(0.85 * _time2secs(self.slurm_settings['time'])))
        values = {
            'cwd': os.getcwd(),
            'launcher_file': task,
            'nodes': nodes,
            'ncpus': 1,
            'runtime': maxruntime,
            'jobname': self.slurm_settings['job_name']
        }
        launcher_cmd = """\
\"export LAUNCHER_WORKDIR={cwd}; \
/corral-repl/utexas/poldracklab/users/wtriplet/external/ls5_launch/launch -s {launcher_file} \
-n {ncpus} -N {nodes} -d {cwd} -r {runtime} -j {jobname}\"\
""".format(**values)
        return _run_cmd(['ssh', '-oStrictHostKeyChecking=no', 'login2', launcher_cmd])


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
        return _run_cmd(['sbatch', task])


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
        # Fix paths for docker image in CircleCI
        task = task.replace(os.path.expanduser('~/'), '/')
        task = task.replace('~/', '/')
        return _run_cmd([
                'sshpass', '-p', 'testuser',
                'ssh', '-p', '10022', 'testuser@localhost',
                'sbatch', task])

    def _get_job_status(self, jobid):
        return _run_cmd([
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
        return _run_cmd(['echo', '"Submitted batch job 49533"'])

    def _get_job_status(self, jobid):
        return _run_cmd(['echo', 'FINISHED']).strip()

def _run_cmd(cmd):
    JOB_LOG.info('Executing command line: %s', ' '.join(cmd))
    try:
        result = check_output(cmd, stderr=STDOUT)
    except CalledProcessError as error:
        JOB_LOG.critical('Error submitting (exit code %d): \n\tCmdline: %s\n\tOutput:\n\t%s',
                         error.returncode, ' '.join(cmd), error.output)
        raise
    return result

def _time2secs(timestr):
    return sum((60**i) * int(t) for i, t in enumerate(reversed(timestr.split(':'))))

def _secs2time(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)

