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

    @property
    def path(self):
        return self._path

    @property
    def subject_list(self):
        return self._subject_list



@pytest.fixture
def data():
    return SetUp()


def test_get_subject_list_nopar(data):
    sub_list = cw.get_subject_list(data.path, no_randomize=False)
    assert sorted(sub_list) == data.subject_list


@pytest.mark.parametrize("participant_label", [
    'sub-10 sub-05',
    '10 05'])
def test_get_subject_list_part(data, participant_label):
    sub_list = cw.get_subject_list(data.path,
                                   participant_label,
                                   no_randomize=False)
    assert  sorted(sub_list) == ['05', '10']

@pytest.mark.xfail(raises=RuntimeError)
@pytest.mark.parametrize("participant_label", [
    'sub-15 sub-05',
    '15 05',
    '15 27'])
def test_get_subject_list_raise(data, participant_label):
    sub_list = cw.get_subject_list(data.path,
                                   participant_label,
                                   no_randomize=False)
    assert  sorted(sub_list) == ['05', '10']
