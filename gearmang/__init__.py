# -*- coding: utf-8 -*-

"""
gearmang
~~~~~~~~

:copyright: (c) 2012 by Rhett Garber
:license: ISC, see LICENSE for more details.

"""

__title__ = 'gearmang'
__version__ = '0.0.1'
__build__ = 0
__author__ = 'Rhett Garber'
__license__ = 'ISC'
__copyright__ = 'Copyright 2012 Rhett Garber'

from .client import Client
Client = Client #pyflakes

from .worker import Worker
Worker = Worker #pyflakes
