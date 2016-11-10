#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
Utilities: Agave wrapper for sherlock
"""
import os
from os import path as op
import subprocess as sp
import re
from time import sleep
import logging
from pprint import pformat as pf
from pkg_resources import resource_filename as pkgrf
from io import open
from builtins import object

from cappat import AGAVE_JOB_LOGS, AGAVE_JOB_OUTPUT
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
    def build(task_list, settings=None, work_dir=None,
              hostname=None):
        """
        Get the appropriate TaskManager object
        """
        hostname = settings.get('execution_system', None)

        if hostname is None:
            hostname = getsystemname()

        JOB_LOG.info('Identified host: "%s"', hostname)

        if not hostname:
            raise RuntimeError('Could not identify execution system')

        if hostname.endswith('ls5.tacc.utexas.edu'):
            return Lonestar5Submission(task_list, settings, work_dir)
        elif hostname.endswith('stanford.edu'):
            return SherlockSubmission(task_list, settings, work_dir)
        elif hostname.endswith('stampede.tacc.utexas.edu'):
            raise NotImplementedError
        elif hostname == 'test.circleci':
            return CircleCISubmission(task_list, settings, work_dir)
        elif hostname == 'test.local':
            return TestSubmission(task_list, settings, work_dir)
        else:
            raise RuntimeError(
                'Could not identify "{}" as a valid execution system'.format(hostname))


class TaskSubmissionBase(object):
    """
    A base class for task submission
    """
    _settings = {}
    jobexp = re.compile(r'Submitted batch job (?P<jobid>\d*)')
    _cmd_prefix = []

    SLURM_TEMPLATE = op.abspath(pkgrf('cappat', 'tpl/sherlock-sbatch.jnj2'))
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

        self._settings['child_runtime'] = _secs2time(
            int(0.85 * _time2secs(self._settings['max_runtime'])))

        self.work_dir = check_folder(op.abspath(work_dir))
        self.aux_dir = check_folder(op.join(self.work_dir, AGAVE_JOB_LOGS))
        self._settings.update(
            {'work_dir': self.work_dir, 'aux_dir': self.aux_dir}
        )

        group_args = self._settings.get('group_args', '')
        self._group_cmd = [self._settings['executable'], self._settings['bids_dir'],
                           AGAVE_JOB_OUTPUT, 'group', group_args]
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
        exit_codes = []
        for line in results.split('\n'):
            fields = line.split()
            self._jobs[fields[0]] = fields[1]
            exit_codes.append(int(fields[2].split(':')[0]))
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
        statuses = squeue.split('\n')
        for line in statuses:
            status = line.split(',')
            if len(status) < 2:
                JOB_LOG.error('Error parsing squeue output: \n%s\n', line)
                raise RuntimeError('Error parsing squeue output: {}'.format(
                    line))

            self._jobs[status[0]] = status[1]

            if status in SLURM_FAIL_STATUS:
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

class Lonestar5Submission(TaskSubmissionBase):
    """
    The LS5 submission manager
    """

    def _generate_sbatch(self):
        """
        Generates one launcher file
        """
        launcher_file = op.join(self.aux_dir, 'launch_script.sh')
        with open(launcher_file, 'w') as lfh:
            lfh.write('\n'.join(self.task_list) + '\n')
        return [launcher_file]

    def _submit_sbatch(self, task):
        with open(task, 'r') as tfh:
            nodes = sum(1 for line in tfh if line.strip() and not line.strip().startswith('#'))
        values = {
            'cwd': os.getcwd(),
            'launcher_file': task,
            'nodes': nodes,
            'ncpus': 1,
            'jobname': self._settings['job_name'],
            'runtime': self._settings['child_runtime']
        }
        launcher_cmd = """\
export LAUNCHER_WORKDIR={cwd}; \
/corral-repl/utexas/poldracklab/users/wtriplet/external/ls5_launch/launch -s {launcher_file} \
-n {ncpus} -N {nodes} -d {cwd} -r {runtime} -j {jobname} -f {cwd}/{jobname}.qsub \
""".format(**values)
        return _run_cmd(['ssh', '-oStrictHostKeyChecking=no', 'login2', launcher_cmd])


class SherlockSubmission(TaskSubmissionBase):
    """
    The Sherlock submission
    """

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

def _run_cmd(cmd, shell=False, env=None):
    JOB_LOG.info('Executing command line: %s', ' '.join(cmd))
    try:
        result = sp.check_output(cmd, stderr=sp.STDOUT, shell=shell, env=env)
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

def _format_modules(modules_list):
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


def _time2secs(timestr):
    return sum((60**i) * int(t) for i, t in enumerate(reversed(timestr.split(':'))))

def _secs2time(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)

