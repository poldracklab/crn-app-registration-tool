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
    'job_log': '/scratch/testjob',
    'modules': []
}

def fake_hostname():
    return 'circleci.test.host'

#@mock.patch('cappat.jobs._gethostname', side_effect=fake_hostname)
#@mock.patch('cappat.jobs._gethostname')

@mock.patch('cappat.utils.gethostname', return_value='circleci.test.host')
def test_job_creation():
    tasks = ['testapp ~/Data out/ participant --participant_label '
             '10 11 12 -w work/sjob-0000  >> log/sjob-0000.log']

    slurm = cj.TaskManager.build(tasks, JOB_SETTINGS,
                                 temp_folder=os.path.expanduser('~/scratch/slurm'))
    slurm.submit()
    assert len(slurm.job_ids) == 1

def test_job_run():
    tasks = ['testapp ~/Data out/ participant --participant_label '
             '10 11 12 -w work/sjob-0000  >> log/sjob-0000.log']

    slurm = cj.TaskManager.build(tasks, JOB_SETTINGS,
                                 temp_folder=os.path.expanduser('~/scratch/slurm'))
    slurm.submit()
    assert len(slurm.children_yield()) == 1
