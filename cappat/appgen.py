#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: oesteban
# @Date:   2016-03-16 11:28:27
# @Last Modified by:   oesteban
# @Last Modified time: 2016-10-13 16:01:39

"""
Agave app generator

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import os.path as op
from argparse import ArgumentParser, RawTextHelpFormatter
import logging
import json
from pkg_resources import resource_filename as pkgrf
from io import open

logging.basicConfig()
logger = logging.getLogger('appgen')
logger.setLevel(logging.INFO)

AGAVE_APP_OPTIONAL = [
    ('helpURI', None),
    ('label', None),
    ('shortDescription', None),
    ('longDescription', None),
    ('executionType', 'HPC'),
    ('parallelism', 'PARALLEL'),
    ('defaultQueue', 'normal'),
    ('defaultNodeCount', 1),
    ('defaultMaxRunTime', "04:00:00"),
    ('defaultMemoryPerNode', "4GB"),
    ('defaultProcessorsPerNode', 1),
    ('deploymentPath', 'apps/cappat'),
    ('deploymentSystem', 'openfmri-storage'),
    ('templatePath', 'wrapper.sh'),
    ('testPath', 'wrapper.sh'),
    ('checkpointable', False),
    ('archiveSystem', 'openfmri-archive'),
    ('archive', True),
    ('tags', []),
    ('modules', []),
    ('inputs', []),
    ('parameters', [])
]
AGAVE_EXECUTION_SYSTEMS = [
    'cli-sherlock.stanford.edu',
    'slurm-sherlock.stanford.edu',
    'slurm-ls5.tacc.utexas.edu',
    'cli-stampede.tacc.utexas.edu',
    'slurm-stampede.tacc.utexas.edu'
]

class CappatAgaveClient(object):
    AGAVE_CAPPAT_CLIENT = 'cappat-client'
    AGAVE_SESSION_FILE = op.expanduser('~/.agave/current')

    def __init__(self, app_desc=None, auth=None):
        """Creates an Agave session object"""
        from agavepy.agave import Agave
        from cappat.info import __wrapperver__ as wversion

        self.app_desc = app_desc
        if app_desc is not None:
            if self.app_desc['deploymentPath'].endswith('/'):
                self.app_desc['deploymentPath'] = self.app_desc['deploymentPath'][:-1]
            self.app_desc['deploymentPath'] += '-{}'.format(wversion)

        with open(self.AGAVE_SESSION_FILE, 'r') as asf:
            session_data = json.load(asf)

        for key in ['access_token', 'refresh_token', 'baseurl',
                    'apikey', 'apisecret']:
            if not session_data.get(key):
                raise RuntimeError('{} required'.format())

        self.agave = Agave(
            api_server=session_data['baseurl'],
            api_key=session_data['apikey'],
            api_secret=session_data['apisecret'],
            token=session_data['access_token'],
            refresh_token=session_data['refresh_token'],
            client_name=self.AGAVE_CAPPAT_CLIENT,
            verify=session_data.get('verify', False)
        )

        try:
            clients = self.agave.clients.list()
            return
        except AttributeError:
            if auth is None or auth[0] is None or auth[1] is None:
                raise RuntimeError('Agave failed to authenticate')

        logger.warn('Agave token could not be reused, trying to '
                    'access using username and password.')
        self.agave = Agave(
            api_server=session_data['baseurl'],
            username=auth[0],
            password=auth[1],
            client_name=self.AGAVE_CAPPAT_CLIENT
        )

        try:
            clients = [c['name'] for c in self.agave.clients.list()]
            return
        except AttributeError:
            if auth[0] is None or auth[1] is None:
                raise RuntimeError('Agave failed to authenticate')

        logger.warn('Agave client could not be engaged, trying to '
                    'access using username and password.')
        self.agave = Agave(
            api_server=session_data['baseurl'],
            username=auth[0],
            password=auth[1]
        )
        if self.AGAVE_CAPPAT_CLIENT in clients:
            self.agave.clients.delete(self.AGAVE_CAPPAT_CLIENT)
        self.agave.clients.create(
            body={'clientName': self.AGAVE_CAPPAT_CLIENT})


    def _upload_file(self, fname, remote_path, remote_fname=None,
                     system='openfmri-storage', overwrite=True):
        """Upload file to an storage system"""

        if remote_fname is None:
            remote_fname = op.basename(fname)

        logger.info('Uploading "%s" to remote path "%s/%s" in %s',
                    fname, remote_path, remote_fname, system)

        head_path, tail_path = op.split(remote_path)
        if head_path and head_path != '/':
            while head_path.endswith('/'):
                head_path, tail = op.split(head_path)
                tail_path = op.join(tail, tail_path)

            # Make sure the folder exists
            self.agave.files.manage(
                systemId=system, filePath=head_path,
                body={'action':'mkdir', 'path': tail_path})
            logger.info('Created/checked that "%s/" folder exists under "%s/"',
                        tail_path, head_path)

        existing_files = [f.name for f in self.agave.files.list(
            systemId=system, filePath=remote_path)]
        file_exists = remote_fname in existing_files
        if not overwrite and file_exists:
            raise RuntimeError(
                'Remote path "{}/{}" exists in "{}"'.format(
                    remote_path, remote_fname, system))

        if file_exists:
            logger.warn('Overwriting remote path "%s/%s" in "%s"',
                        remote_path, remote_fname, system)

        with open(fname, 'r') as file_to_upload:
            result = self.agave.files.importData(
                systemId=system, filePath=remote_path,
                fileName=remote_fname, fileToUpload=file_to_upload
            )
        return result

    def upload_wrapper(self):
        """Upload the wrapper"""
        self._upload_file(
            pkgrf('cappat', 'data/wrapper.sh'),
            self.app_desc['deploymentPath'],
            remote_fname=self.app_desc['templatePath'],
            system=self.app_desc['deploymentSystem'])

    def singularity_image(self, image_file):
        """Upload singularity image"""
        image_storage = 'images-' + self.app_desc['executionSystem'].split('-')[1]

        self._upload_file(image_file, '',
                          remote_fname=op.basename(image_file),
                          system=image_storage,
                          overwrite=False)

        root_dir = self.agave.systems.get(
            systemId=image_storage)['storage']['rootDir']

        self.app_desc['modules'].append('load singularity')
        for i, param in enumerate(self.app_desc['parameters']):
            if param['id'] == 'execPath':
                self.app_desc['parameters'][i]['value']['default'] = op.join(
                    root_dir, op.basename(image_file))

    def add_app(self):
        apps = [app['id'] for app in self.agave.apps.list()]
        app_id = '{name}-{version}'.format(**self.app_desc)

        logger.info('Registering app "%s" with spec:\n--\n%s\n--',
                    app_id, json.dumps(self.app_desc, indent=4))

        if app_id in apps:
            raise RuntimeError('Trying to overwrite an existing '
                               'version of the app ({})'.format(app_id))
        return self.agave.apps.add(body=self.app_desc)

    def install(self):
        """Performs the prescribed actions in order"""
        self.upload_wrapper()
        self.add_app()


def main():
    """Entry point"""

    parser = ArgumentParser(description="""\
Generates an Agave app bundle to be deployed in the \
CRN-platform""", formatter_class=RawTextHelpFormatter)

    parser.add_argument(
        'app_name', action='store',
        help='The name that will be used to discover this app')
    parser.add_argument(
        'app_version', action='store',
        help='Version for this app')
    parser.add_argument(
        'execution_system', action='store', choices=AGAVE_EXECUTION_SYSTEMS,
        help='The execution system that will be associated to this app')
    parser.add_argument(
        'entry_point', action='store',
        help='The singularity image that will run this app')


    g_agave = parser.add_argument_group('Agave settings')
    g_agave.add_argument('-u', '--username', action='store',
                         default=os.getenv('AGAVE_USERNAME', None),
                         help='agave username')
    g_agave.add_argument('-p', '--password', action='store',
                         default=os.getenv('AGAVE_PASSWORD', None),
                         help='agave password')

    g_optional = parser.add_argument_group('Optional App fields')
    for field, default_value in AGAVE_APP_OPTIONAL:
        g_optional.add_argument(
            '--{}'.format(field), action='store', default=default_value)

    g_outputs = parser.add_argument_group('Outputs')
    g_outputs.add_argument('-o', '--output', action='store')

    opts = parser.parse_args()

    settings = {
        'label': '{} (cappat @ {})'.format(
            opts.app_name.upper(), opts.execution_system),
        'shortDescription': '{}-{} app automatically registered with cappat'.format(
            opts.app_name, opts.app_version),
        'longDescription': json.dumps({
            'support': 'Please add a link for support',
            'description': 'Please add a description',
            'acknowledgments': 'Please cite <citation-here>'
        })
    }

    for field, _ in AGAVE_APP_OPTIONAL:
        val = getattr(opts, field, None)
        if val is not None:
            settings[field] = val

    settings.update({
        'name': opts.app_name,
        'version': opts.app_version,
        'executionSystem': opts.execution_system
    })

    # Set default parameters
    with open(pkgrf('cappat', 'data/default_app_params.json')) as defp:
        settings['parameters'] = json.load(defp)
    settings['parameters'][0]['value']['default'] = opts.entry_point

    with open(pkgrf('cappat', 'data/default_app_inputs.json')) as defp:
        settings['inputs'] = json.load(defp)

    # 2. Connect agave and register app
    a_ses = CappatAgaveClient(settings, (opts.username, opts.password))
    if op.isfile(opts.entry_point):
        a_ses.singularity_image(opts.entry_point)
    a_ses.install()




if __name__ == '__main__':
    main()
