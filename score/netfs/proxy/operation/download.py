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

from .base import Operation
import random
from score.netfs.proxy.backend import NotConnected
from score.netfs.constants import Constants
import struct


class DownloadOperation(Operation):

    def __init__(self, frontend):
        super().__init__(frontend, 'download')
        self.path = None
        self.backend = None
        self.backends = self.frontend.backends[:]
        self.sent_bytes = 0
        self.read(4, self.read_request_name_length)

    def read_request_name_length(self, length_bytes):
        length = struct.unpack('!i', length_bytes)[0]
        self.read(length, self.read_request_name)

    def read_request_name(self, name_bytes):
        self.path = str(name_bytes, 'UTF-8')
        self.log.debug('path = {}'.format(self.path))
        self.response_attempt()

    def response_attempt(self):
        if not self.backends:
            data = struct.pack('!b', Constants.RESP_ERROR)
            self.write(data, self.frontend.read_op)
            if self.backend:
                self.backend.remove_close_callback(self._backend_closed)
            return
        backend = random.choice(self.backends)
        self.backends.remove(backend)
        try:
            backend.add_close_callback(self._backend_closed)
        except NotConnected:
            return self.response_attempt()
        self.log.debug('chosen: {}'.format(backend))
        self.backend = backend
        data = struct.pack('!b', Constants.REQ_DOWNLOAD)
        data += struct.pack('!i', len(self.path.encode('UTF-8')))
        data += self.path.encode('UTF-8')
        self.skipped_bytes = 0
        self.backend.send(data)
        self.backend.read(1, self.handle_response_status)

    def handle_response_status(self, status_bytes):
        status = struct.unpack('!b', status_bytes)[0]
        if status != Constants.RESP_OK:
            self.response_attempt()
            return
        self.write(status_bytes)
        self.backend.read(8, self.handle_response_size)

    def handle_response_size(self, size_bytes):
        self.write(size_bytes)
        size = struct.unpack('!q', size_bytes)[0]
        self.backend.read(size, self.read_response_hash,
                          streaming_callback=self.handle_response_chunk)

    def handle_response_chunk(self, chunk):
        self.write(chunk)

    def read_response_hash(self, _):
        self.backend.read(512 // 8 + 4, self.handle_response_hash)

    def handle_response_hash(self, hash_bytes):
        self.backend.remove_close_callback(self._backend_closed)
        self.write(hash_bytes)
        self.frontend.read_op()

    def write(self, data, *args, **kwargs):
        diff = self.sent_bytes - self.skipped_bytes
        if diff > 0:
            skip = min(diff, len(data))
            self.skipped_bytes += skip
            data = data[skip:]
        self.skipped_bytes += len(data)
        self.sent_bytes += len(data)
        return super().write(data, *args, **kwargs)

    def _backend_closed(self, backend):
        assert self.backend == backend
        backend.remove_close_callback(self._backend_closed)
        if self.sent_bytes > 0 and not self.backends:
            # Already startend sending data, but can't continue due to lack of
            # working backends. The only remaining option is to terminate the
            # frontend connection.
            self.log.debug('lost last backend connection, terminating')
            return self.frontend.terminate()
        self.log.debug('lost backend connection, retrying')
        self.response_attempt()
