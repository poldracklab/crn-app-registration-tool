#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
from glob import glob
import pytest
from cappat import wrapper as cw

class SetUp:
    def __init__(self):
        self._path = os.getenv('DS030_PATH')
        if self._path is None:
            raise RuntimeError('$DS030_PATH is not defined')
        self._subject_list = sorted([
            os.path.basename(sub)[4:]
            for sub in glob(os.path.join(self._path, 'sub-*'))
        ])
        if not self._subject_list:
            raise RuntimeError('No subjects found in BIDS-folder "{}"'.format(
                self._path))

    @property
    def path(self):
        return self._path

    @property
    def subject_list(self):
        return self._subject_list



@pytest.fixture
def data():
    return SetUp()

@pytest.mark.parametrize("participant_label,no_random,expected", [
    (None, False, ['%02d' % i for i in range(1, 14)]),
    ('sub-10 sub-05', False, ['05', '10']),
    ('10 05', False, ['05', '10']),
    (None, True, ['%02d' % i for i in range(1, 14)]),
    ('sub-10 sub-05', True, ['05', '10']),
    ('10 05', True, ['05', '10']),
    pytest.mark.xfail(('sub-05 sub-15', True, ['05', '15']), raises=RuntimeError),
    pytest.mark.xfail(('05 15', True, ['05', '15']), raises=RuntimeError),
    pytest.mark.xfail(('27 15', True, ['27', '15']), raises=RuntimeError)])
def test_get_subject_list_part(data, participant_label, no_random, expected):
    sub_list = cw.get_subject_list(data.path,
                                   participant_label,
                                   no_randomize=no_random)
    assert  len(set(expected) - set(sub_list)) == 0


@pytest.mark.parametrize("group_size,expected", [
    (3, ['mriqc ~/Data ' + os.getcwd() + '/out participant --participant_label 10 11 12 -w work/sjob-0000  >> log/sjob-0000.log']),
    (2, ['mriqc ~/Data ' + os.getcwd() + '/out participant --participant_label 10 11 -w work/sjob-0000  >> log/sjob-0000.log',
         'mriqc ~/Data ' + os.getcwd() + '/out participant --participant_label 12 -w work/sjob-0001  >> log/sjob-0001.log']),
    (1, ['mriqc ~/Data ' + os.getcwd() + '/out participant --participant_label 10 -w work/sjob-0000  >> log/sjob-0000.log',
         'mriqc ~/Data ' + os.getcwd() + '/out participant --participant_label 11 -w work/sjob-0001  >> log/sjob-0001.log',
         'mriqc ~/Data ' + os.getcwd() + '/out participant --participant_label 12 -w work/sjob-0002  >> log/sjob-0002.log'])
])
def test_get_task_list(group_size, expected):
    assert cw.get_task_list('~/Data', 'mriqc', ['10', '11', '12'], group_size=group_size) == expected
