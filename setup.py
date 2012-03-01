#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import glob
import gearmang
from distutils.core import setup

# publish package
#if sys.argv[-1] == 'publish':
#    os.system('python setup.py sdist upload')
#    sys.exit()
#
## run tests
#if sys.argv[-1] == 'test':
#    os.system('python test_requests.py')
#    sys.exit()


setup(
    name='gearmang',
    version=gearmang.__version__,
    description='Tools for Gearman',
    long_description=open('README.md').read(),
    author='Rhett Garber',
    url='http://github.com/splaice/py-bootstrap',
    package_data={'': ['LICENSE', 'NOTICE']},
    license=gearmang.__license__,
    scripts=glob.glob("bin/*"),
    packages=['gearmang']
)
