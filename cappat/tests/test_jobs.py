#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import mock
# import pytest
from cappat.manager import TaskManager
from cappat.manager.tools import format_modules as _format_modules

JOB_SETTINGS = {
    'nodes': 1,
    'max_runtime': '00:05:00',
    'executable': 'testapp',
    'bids_dir': '~/bids/path',
    'mincpus': 1,
    'execution_system': os.getenv('CRNENV_EXECUTION_SYSTEM', 'test.local'),
    'mem_per_cpu': 4000,
    'partition': 'debug',
    'job_name': 'testjob',
    'modules': []
}


def test_read_modules():
    expected = ['module use /some/path', 'module load crnenv singularity/crn']
    result = _format_modules('use /some/path load crnenv singularity/crn')
    assert result == expected

def test_job_creation():
    tasks = ['echo "Submitted batch job 49533"',
             'echo "Submitted batch job 49534"']

    slurm = TaskManager.build(tasks, JOB_SETTINGS,
                                 work_dir=os.path.expanduser('~/scratch/slurm-1'))
    slurm.map_participant()
    assert len(slurm.job_ids) == 2

def test_group_level_cmd():
    tasks = ['echo "Submitted batch job 49533"',
             'echo "Submitted batch job 49534"']

    slurm = TaskManager.build(tasks, JOB_SETTINGS)
    assert slurm.group_cmd == ['testapp', '~/bids/path', 'out/', 'group']

def test_job_run():
    tasks = ['echo "Submitted batch job 49533"',
             'echo "Submitted batch job 49534"',
             'echo "Submitted batch job 49535"']
    slurm = TaskManager.build(tasks, JOB_SETTINGS,
                                 work_dir=os.path.expanduser('~/scratch/slurm-2'))
    slurm.map_participant()
    assert len(slurm.wait_participant()) == 3

@mock.patch('cappat.jobs.TestSubmission._get_jobs_status',
            mock.Mock(return_value=True))
@mock.patch('cappat.jobs.TestSubmission._run_sacct',
            mock.Mock(return_value='49533   FAILED   113:0'))
def test_job_fail():
    tasks = ['echo "Submitted batch job 49533"']

    slurm = TaskManager.build(tasks, JOB_SETTINGS,
                                 work_dir=os.path.expanduser('~/scratch/slurm-3'))
    slurm.map_participant()
    slurm.wait_participant()
