# Copyright © 2015 STRG.AT GmbH, Vienna, Austria
#
# This file is part of the The SCORE Framework.
#
# The SCORE Framework and all its parts are free software: you can redistribute
# them and/or modify them under the terms of the GNU Lesser General Public
# License version 3 as published by the Free Software Foundation which is in the
# file named COPYING.LESSER.txt.
#
# The SCORE Framework and all its parts are distributed without any WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. For more details see the GNU Lesser General Public
# License.
#
# If you have not received a copy of the GNU Lesser General Public License see
# http://www.gnu.org/licenses/.
#
# The License-Agreement realised between you as Licensee and STRG.AT GmbH as
# Licenser including the issue of its valid conclusion and its pre- and
# post-contractual effects is governed by the laws of Austria. Any disputes
# concerning this License-Agreement including the issue of its valid conclusion
# and its pre- and post-contractual effects are exclusively decided by the
# competent court, in whose district STRG.AT GmbH has its registered seat, at
# the discretion of STRG.AT GmbH also the competent court, in whose district the
# Licensee has his registered seat, an establishment or assets.

from functools import wraps
import logging
from tornado.ioloop import IOLoop


class OperationMeta(type):

    def __init__(cls, name, bases, attrs):
        for attr in attrs:
            if callable(attrs[attr]):
                attrs[attr] = OperationMeta.wrap(attrs[attr])
                setattr(cls, attr, attrs[attr])
        return type.__init__(cls, name, bases, attrs)

    def wrap(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                self.frontend.terminate()
                raise e
        return wrapper


class Operation(metaclass=OperationMeta):

    def __init__(self, frontend, name):
        self.frontend = frontend
        loop = IOLoop.current()
        self.log = logging.LoggerAdapter(
            logging.getLogger('score.netfs.proxy.' + name),
            {'timestamp': loop.time()})
        self.log.debug('init')

    def read(self, *args, **kwargs):
        self.frontend.read(*args, **kwargs)

    def write(self, *args, **kwargs):
        self.frontend.write(*args, **kwargs)
