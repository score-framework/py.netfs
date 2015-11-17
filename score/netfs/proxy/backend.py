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

from datetime import timedelta
import logging
import socket
from tornado.iostream import IOStream, StreamClosedError
from tornado.ioloop import IOLoop


log = logging.getLogger('score.netfs.proxy')


class NotConnected(Exception):
    pass


class Backend:

    def __init__(self, host, port, *, autoconnect=True):
        self.host = host
        self.port = port
        self.stream = None
        self.close_callbacks = []
        self.autoconnect = autoconnect
        if autoconnect:
            self.connect()

    def send(self, data, callback=None):
        if not self.connected():
            raise NotConnected()
        try:
            self.stream.write(data, callback=callback)
        except StreamClosedError:
            raise NotConnected()

    def read(self, length, callback=None, streaming_callback=None):
        # log.debug('{}.read({})'.format(self, length))
        self.stream.read_bytes(length, callback,
                               streaming_callback=streaming_callback)

    def connect(self, success_callback=None, error_callback=None):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        stream = IOStream(s)
        def connected(future):
            if future.exception():
                if self.autoconnect:
                    return self.reconnect()
                elif error_callback:
                    error_callback(self)
            log.debug('connected to {}'.format(self))
            stream.set_close_callback(self._stream_closed)
            self.stream = stream
            if success_callback:
                success_callback(self)
        stream.connect((self.host, self.port)).\
            add_done_callback(connected)

    def close(self):
        pass

    def reconnect(self):
        loop = IOLoop.current()
        loop.add_timeout(timedelta(seconds=2), self.connect)

    def connected(self):
        return self.stream is not None

    def __str__(self):
        return 'Backend({}:{})'.format(self.host, self.port)

    __repr__ = __str__

    def _stream_closed(self):
        self.stream = None
        for callback in self.close_callbacks:
            callback(self)
        self.close_callbacks = []
        if self.autoconnect:
            log.warn('lost connection to {}'.format(self))
            self.reconnect()

    def add_close_callback(self, callback):
        if not self.stream:
            raise NotConnected(self)
        self.close_callbacks.append(callback)

    def remove_close_callback(self, callback):
        try:
            self.close_callbacks.remove(callback)
        except ValueError:
            pass

    def transaction(self, success_callback, error_callback):
        backend = Backend(self.host, self.port, autoconnect=False)
        backend.connect(success_callback, error_callback)
        return backend
