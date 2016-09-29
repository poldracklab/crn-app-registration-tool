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
import pkg_resources as pkgr
from cappat.tpl import Template

SBATCH_TEMPLATE = pkgr.resource_filename('cappat.tpl', 'sherlock-sbatch.jnj2')
SBATCH_FIELDS = ['nodes', 'time', 'mincpus', 'mem_per_cpu', 'partition',
                 'job_name', 'job_log']


def tearup(task_list, slurm_settings, out_folder=None):
    """
    Generates one sbatch file per task
    """

    if out_folder is None:
        out_folder = op.join(os.getcwd(), 'log')
    _check_folder(out_folder)

    missing = list(set(SBATCH_FIELDS) - set(list(slurm_settings.keys())))
    if missing:
        raise RuntimeError('Error filling up template with missing fields:'
                           ' {}.'.format("'%s'".join(missing)))

    sbatch_files = []
    for i, task in enumerate(task_list):
        sbatch_files.append(op.join(out_folder, 'slurm-%06d.sbatch' % i))
        slurm_settings['commandline'] = task
        conf = Template(SBATCH_TEMPLATE)
        conf.generate_conf(slurm_settings, sbatch_files[-1])
    return sbatch_files

def submit(sbatch_files):
    """
    Submits a list of sbatch files and returns the assigned job ids
    """
    job_ids = []
    for slurm_job in sbatch_files:
        # run sbatch
        pass
        # parse output and get job id

    return job_ids

def children_yield(job_ids):
    """
    Busy wait until all jobs in the list are done
    """

    return True


def _check_folder(folder):
    if not op.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as exc:
            if not exc.errno == EEXIST:
                raise
