#!/usr/bin/env python
# -*- coding: utf-8 -*-

PACKAGE_NAME = 'cappat'

def main():
    """ Install entry-point """
    from os import path as op
    from glob import glob
    from inspect import getfile, currentframe
    from setuptools import setup, find_packages
    from io import open  # pylint: disable=W0622

    this_path = op.dirname(op.abspath(getfile(currentframe())))

    # Python 3: use a locals dictionary
    # http://stackoverflow.com/a/1463370/6820620
    ldict = locals()

    # Get version and release info, which is all stored in phantomas/info.py
    module_file = op.join(this_path, PACKAGE_NAME, 'info.py')
    with open(module_file) as infofile:
        pythoncode = [line for line in infofile.readlines() if not line.strip().startswith('#')]
        exec('\n'.join(pythoncode), globals(), ldict)

    setup(
        name=PACKAGE_NAME,
        version=ldict['__version__'],
        description=ldict['__description__'],
        long_description=ldict['__longdesc__'],
        author=ldict['__author__'],
        author_email=ldict['__email__'],
        maintainer=ldict['__maintainer__'],
        maintainer_email=ldict['__email__'],
        license=ldict['__license__'],
        url=ldict['URL'],
        download_url=ldict['DOWNLOAD_URL'],
        classifiers=ldict['CLASSIFIERS'],
        packages=find_packages(exclude=['build', 'doc', 'old-wrappers']),
        package_data={'cappt.tpl': ["*.jnj2"]},
        entry_points={'console_scripts': [
            'agave_wrapper=cappat.wrapper:main'
        ]},
        # scripts=glob('scripts/*'),
        zip_safe=False,
        # Dependencies handling
        setup_requires=ldict['SETUP_REQUIRES'],
        install_requires=ldict['REQUIRES'],
        dependency_links=ldict['LINKS_REQUIRES'],
        tests_require=ldict['TESTS_REQUIRES'],
        extras_require=ldict['EXTRA_REQUIRES'],
    )


if __name__ == '__main__':
    main()
