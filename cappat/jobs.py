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
        elif hostname == 'test.circleci':
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
        'job_name': 'crn-bidsapp',
        'nodes': 1,
        'time': '01:00:00',
    }
    jobexp = re.compile(r'Submitted batch job (?P<jobid>\d*)')
    _cmd_prefix = []

    SLURM_TEMPLATE = pkgr.resource_filename('cappat', 'tpl/sherlock-sbatch.jnj2')

    def __init__(self, task_list, slurm_settings=None, group_cmd=None, temp_folder=None):

        if not task_list:
            raise RuntimeError('a list of tasks is required')

        self.task_list = task_list

        if slurm_settings is not None:
            self.slurm_settings.update(slurm_settings)

        if temp_folder is None:
            temp_folder = AGAVE_JOB_LOGS

        self.temp_folder = check_folder(op.abspath(temp_folder))
        self.slurm_settings['work_dir'] = self.temp_folder
        self.sbatch_files = self._generate_sbatch()
        self._jobs = {}
        self._group_cmd = group_cmd

        JOB_LOG.info('Created TaskManager type "%s"', self.__class__.__name__)

    @property
    def job_ids(self):
        return list(self._jobs.keys())

    @property
    def jobs(self):
        return self._jobs

    @property
    def group_cmd(self):
        return self._group_cmd

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

    def _get_job_acct(self):
        # sacct -n -X -j 10016750,10016749 -o JobID,State,ExitCode
        job_ids = list(self._jobs.keys())
        JOB_LOG.info('Checking exit code of jobs %s', ' '.join(job_ids))

        results = _run_cmd(self._cmd_prefix + [
            'sacct', '-n', '-X', '-j', ','.join(job_ids),
            '-o', 'JobID,State,ExitCode'])

        if results is None:
            JOB_LOG.critical('Running sacct over jobs %s did not produce any output',
                             ', '.join(job_ids))
            raise RuntimeError('sacct command output is empty')

        #parse results
        exit_codes = []
        for line in results.split('\n'):
            fields = line.split()
            self._jobs[fields[0]] = fields[1]
            exit_codes.append(int(fields[2].split(':')[0]))
        return exit_codes

    def _get_jobs_status(self):
        statuses = _run_cmd(self._cmd_prefix + [
            'squeue', '-j', ','.join(self.job_ids), '-o', '%i,%t', '-h'])

        # Jobs are not in the queue anymore
        if statuses is None:
            return True

        statuses = statuses.split('\n')
        if statuses[0].endswith('Invalid job id specified'):
            return True

        pending = []
        for line in statuses:
            status = line.split(',')
            if len(status) < 2:
                raise RuntimeError('Error parsing squeue output: {}'.format(
                    line))

            self._jobs[status[0]] = status[1]
            pending.append(status[0])

            if status in SLURM_FAIL_STATUS:
                raise RuntimeError('Job id {} failed with status {}.'.format(
                                   *status))

        JOB_LOG.info('There are pending jobs: %s', ' '.join(pending))
        return False

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
        JOB_LOG.info('Starting busy wait on jobs %s',
                     ' '.join(self.job_ids))
        all_finished = False

        while True:
            all_finished = self._get_jobs_status()
            if all_finished:
                break
            sleep(SLEEP_SECONDS)

        JOB_LOG.info('Finished wait on jobs %s',
                     ' '.join(self.job_ids))

        sleep(10)
        # Run sacct to check the exit code of jobs
        overall_exit = sum(self._get_job_acct())

        JOB_LOG.info('Final status of jobs: %s', ', '.join([
            '%s (%s)' % (k, v) for k, v in list(self._jobs.items())]))

        if overall_exit > 0:
            JOB_LOG.critical('One or more tasks finished with non-zero code')
            # raise RuntimeError('One or more tasks finished with non-zero code')
        return self.job_ids

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
    }

    def _generate_sbatch(self):
        """
        Generates one launcher file
        """
        launcher_file = op.join(self.temp_folder, 'launch_script.sh')
        with open(launcher_file, 'w') as lfh:
            lfh.write('\n'.join(self.task_list) + '\n')
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
export LAUNCHER_WORKDIR={cwd}; \
/corral-repl/utexas/poldracklab/users/wtriplet/external/ls5_launch/launch -s {launcher_file} \
-n {ncpus} -N {nodes} -d {cwd} -r {runtime} -j {jobname}\
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
    _cmd_prefix = ['sshpass', '-p', 'testpass',
                   'ssh', '-p', '10022', 'circleci@localhost']

    def _generate_sbatch(self):
        """
        Generates one sbatch file per task
        """
        # Remove default settings of Sherlock which are unsupported
        self.slurm_settings.pop('qos', None)
        self.slurm_settings.pop('mincpus', None)
        self.slurm_settings.pop('mem_per_cpu', None)
        self.slurm_settings.pop('modules', None)
        self.slurm_settings['work_dir'] = self.slurm_settings['work_dir'].replace(
            op.expanduser('~/'), '/')
        self.slurm_settings['work_dir'] = self.slurm_settings['work_dir'].replace(
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
        self.slurm_settings.pop('qos', None)
        self.slurm_settings.pop('mincpus', None)
        self.slurm_settings.pop('mem_per_cpu', None)
        self.slurm_settings.pop('modules', None)
        return super(TestSubmission, self)._generate_sbatch()

    def _submit_sbatch(self, task):
        return _run_cmd(['echo', '"Submitted batch job 49533"'])

    def _get_jobs_status(self):
        return _run_cmd(['echo', 'FINISHED']).strip()

    def _get_job_acct(self):
        return [0] * len(self.job_ids)

def _run_cmd(cmd, shell=False):
    JOB_LOG.info('Executing command line: %s', ' '.join(cmd))
    try:
        result = check_output(cmd, stderr=STDOUT, shell=shell)
    except CalledProcessError as error:
        JOB_LOG.critical('Error submitting (exit code %d): \n\tCmdline: %s\n\tOutput:\n\t%s',
                         error.returncode, ' '.join(cmd), error.output)
        raise
    result = '\n'.join([line for line in result.split('\n') if line.strip()])
    if not result:
        JOB_LOG.info('Command output was empty')
        return None

    JOB_LOG.info('Command output: \n%s', result)
    return result


def _time2secs(timestr):
    return sum((60**i) * int(t) for i, t in enumerate(reversed(timestr.split(':'))))

def _secs2time(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)

