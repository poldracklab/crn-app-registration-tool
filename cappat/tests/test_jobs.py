#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
from glob import glob
import mock
# import pytest
from cappat import jobs as cj

JOB_SETTINGS = {
    'nodes': 1,
    'time': '00:05:00',
    'mincpus': 1,
    'mem_per_cpu': 4000,
    'partition': 'debug',
    'job_name': 'testjob',
    'modules': []
}


def test_job_creation():
    tasks = ['echo "Submitted batch job 49533"',
             'echo "Submitted batch job 49534"']

    slurm = cj.TaskManager.build(tasks, JOB_SETTINGS,
                                 work_dir=os.path.expanduser('~/scratch/slurm-1'))
    slurm.map_participant()
    assert len(slurm.job_ids) == 2

def test_job_run():
    tasks = ['echo "Submitted batch job 49533"',
             'echo "Submitted batch job 49534"',
             'echo "Submitted batch job 49535"']
    slurm = cj.TaskManager.build(tasks, JOB_SETTINGS,
                                 work_dir=os.path.expanduser('~/scratch/slurm-2'))
    slurm.map_participant()
    assert len(slurm.wait_participant()) == 3

@mock.patch('cappat.jobs.TestSubmission._get_jobs_status',
            mock.Mock(return_value=True))
@mock.patch('cappat.jobs.TestSubmission._run_sacct',
            mock.Mock(return_value='49533   FAILED   113:0'))
def test_job_fail():
    tasks = ['echo "Submitted batch job 49533"']

    slurm = cj.TaskManager.build(tasks, JOB_SETTINGS,
                                 work_dir=os.path.expanduser('~/scratch/slurm-3'))
    slurm.map_participant()
    slurm.wait_participant()
