#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: oesteban
# @Date:   2016-03-16 11:28:27
# @Last Modified by:   oesteban
# @Last Modified time: 2016-09-29 09:49:11

"""
Agave app generator

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os.path as op
import pkg_resources as pkgr
from argparse import ArgumentParser, RawTextHelpFormatter
import json
import mriqc
from cappat.tpl import Template

APP_TEMPLATE = pkgr.resource_filename('cappat.tpl', 'app_desc.jnj2')

APP_FIELDS = {
    'app_name': mriqc.__name__,
    'version': mriqc.__versionbase__,
    'url': 'http://mriqc.readthedocs.io',
    'label': 'MRIQC (lonestar5)',
    'short_desc': mriqc.__description__,
    'long_desc': json.dumps(mriqc.__longdesc__)[1:-1],
    'tags': ['QA', 'sMRI', 'fMRI'],
}

APP_INPUTS = [{
    'id': 'bidsFolder',
    'details': {
        'label': 'folder',
        'description': 'input root folder of a BIDS-compliant tree',
        'argument': None,
        'showArgument': False
    },
    'value': {
        'visible': True,
        'required': True,
        'type': 'string',
        'default': 'agave://openfmri-storage/data/ds003_downsampled'
    },
    'semantics': {
        'ontology': ['xsd:string']
    }
}]

APP_PARAMS = [{
    'id': 'subjectList',
    'value': {
        'type': 'string',
        'visible': True,
        'required': False,
        'argument': None,
        'showArgument': False
    },
    'details': {
        'label': 'list of subjects to be processed',
        'description': 'corresponds to mriqc -S',
    },
    'semantics': {
        'ontology': ['xsd:string'],
        'minCardinality': 1,
        'maxCardinality': -1
    }
}]

def main():
    """Entry point"""

    parser = ArgumentParser(description='ABIDE2BIDS downloader',
                            formatter_class=RawTextHelpFormatter)
    g_inputs = parser.add_argument_group('Inputs')
    g_inputs.add_argument('--help-uri', action='store')
    g_inputs.add_argument('--input-name', action='store')
    g_inputs.add_argument('-E', '--exec-system', action='store')
    g_inputs.add_argument('-D', '--docker', action='store_true',
                          default=False)

    g_outputs = parser.add_argument_group('Outputs')
    g_outputs.add_argument('-o', '--output', action='store',
                           default='app.json')

    opts = parser.parse_args()

    app_desc = APP_FIELDS
    inputs = APP_INPUTS

    if opts.input_name is not None:
        inputs[0]['id'] = opts.input_name

    if opts.exec_system:
        app_desc['execution_system'] = opts.exec_system

    if opts.docker:
        app_desc.update({
            'app_name': 'mriqc-docker',
            'execution_system': 'docker.tacc.utexas.edu',
            'execution_type': 'CLI',
            'parallelism': 'SERIAL',
            'queue': 'debug',
            'memory': '1GB',
            'label': 'MRIQC (docker)'
        })
        app_desc['tags'] += ['docker']
    else:
        app_desc['modules'] = [
            'use /work/01329/poldrack/lonestar/software_lonestar5/modules',
            'load crnenv'
        ]

    app_desc['inputs'] = [json.dumps(i) for i in inputs]
    app_desc['parameters'] = [json.dumps(i) for i in APP_PARAMS]

    conf = Template(APP_TEMPLATE)
    conf.generate_conf(app_desc, opts.output)


if __name__ == '__main__':
    main()
