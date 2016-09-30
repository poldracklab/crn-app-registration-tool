#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""
The Agave wrapper in python
"""
from os import path as op
from glob import glob
from random import shuffle
from argparse import ArgumentParser, RawTextHelpFormatter
from textwrap import dedent

from cappat import __version__


def get_subject_list(bids_dir, participant_label=None, no_randomize=False):
    """
    Returns a the list of subjects to be processed

    """
    # Build settings dict
    bids_dir = op.abspath(bids_dir)
    all_subjects = sorted([op.basename(subj)[4:] for subj in glob(op.join(bids_dir, 'sub-*'))])

    if participant_label is None:
        participant_label = ''
    participant_label = [s for s in participant_label.strip().split(' ') if s]

    if not participant_label:
        subject_list = all_subjects
    else:
        # remove sub- prefix, get unique
        subject_list = [subj[4:] if subj.startswith('sub-') else subj
                        for subj in participant_label]
        subject_list = sorted(list(set(subject_list)))

        if list(set(subject_list) - set(all_subjects)):
            non_exist = list(set(subject_list) - set(all_subjects))
            raise RuntimeError('Participant label(s) not found in the '
                               'BIDS root directory: {}'.format(' '.join(non_exist)))

    if not no_randomize:
        shuffle(subject_list)

    return subject_list


def get_task_list(bids_dir, app_name, subject_list, group_size=1, *args):
    groups = [sorted(subject_list[i:i+group_size])
              for i in range(0, len(subject_list), group_size)]

    log_arg = '>> log/sjob-{:04d}.log'.format
    output_dir = op.abspath('out/')
    task_list = []
    for i, part_group in enumerate(groups):
        task_list.append(
            '{0} {1} {2} participant --participant_label {3} {4} {6} {5}'.format(
            app_name, bids_dir, output_dir, ' '.join(part_group),
            '-w work/sjob-{:04d}'.format(i), log_arg(i), ' '.join(args)))
    return task_list

def get_execution_system():
    import socket
    fqdns = list(set([socket.getfqdn(i[4][0])
                 for i in socket.getaddrinfo(socket.gethostname(), None)]))

    if not fqdns:
        raise RuntimeError('Could not identify execution system')

    if fqdns[0].endswith('ls5.tacc.utexas.edu'):
        return 'ls5'
    elif fqdns[0].endswith('stanford.edu'):
        return 'sherlock'
    elif fqdns[0].endswith('stampede.tacc.utexas.edu'):
        return 'stampede'
    else:
        raise RuntimeError('Could not identify {} as execution system'.format(
            fqdns))


def main():
    """Entry point"""
    parser = ArgumentParser(formatter_class=RawTextHelpFormatter, description=dedent(
'''The Agave wrapper in python
---------------------------


'''))

    parser.add_argument('-v', '--version', action='version',
                        version='mriqc v{}'.format(__version__))

    parser.add_argument('bids_dir', action='store',
                        help='The directory with the input dataset '
                             'formatted according to the BIDS standard.')
    parser.add_argument('output_dir', action='store',
                        help='The directory where the output files '
                             'should be stored. If you are running group level analysis '
                             'this folder should be prepopulated with the results of the'
                             'participant level analysis.')
    parser.add_argument('--participant_label', '--subject_list', '-S', action='store',
                        help='The label(s) of the participant(s) that should be analyzed. '
                             'The label corresponds to sub-<participant_label> from the '
                             'BIDS spec (so it does not include "sub-"). If this parameter '
                             'is not provided all subjects should be analyzed. Multiple '
                             'participants can be specified with a space separated list.',
                        nargs="*")
    parser.add_argument('--group-size', default=1, action='store', type=int,
                        help='parallelize participants in groups')
    parser.add_argument('--no-randomize', default=False, action='store_true',
                        help='do not randomize participants list before grouping')
    parser.add_argument('--log-groups', default=False, action='store_true',
                        help='append logging output')
    parser.add_argument('--bids-app-name', default='mriqc', action='store',
                        help='BIDS app to call')
    parser.add_argument('--args', default='', action='store', help='append arguments')

    args = parser.parse_args()



if __name__ == '__main__':
    main()
