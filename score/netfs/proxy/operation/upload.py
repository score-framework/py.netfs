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
import struct
from score.netfs.constants import Constants
from score.netfs.proxy.backend import NotConnected


class UploadOperation(Operation):

    def __init__(self, frontend):
        super().__init__(frontend, 'upload')
        self.frontend.init_transaction(self.created_transaction)

    def created_transaction(self, transaction):
        self.transaction = transaction
        for backend in transaction:
            backend.add_close_callback(self._backend_closed)
        self.backends = transaction[:]
        self.distribute(struct.pack('!b', Constants.REQ_UPLOAD))
        self.read(4, self.handle_name_length)

    def distribute(self, data):
        self.log.debug('delegating to {} backends: {}'.
                       format(len(self.backends), data))
        for backend in self.backends:
            try:
                backend.send(data)
            except NotConnected:
                pass

    def handle_name_length(self, length_bytes):
        self.distribute(length_bytes)
        length = struct.unpack('!i', length_bytes)[0]
        self.read(length, self.handle_name)

    def handle_name(self, name_bytes):
        self.distribute(name_bytes)
        self.read(8, self.handle_content_length)

    def handle_content_length(self, length_bytes):
        self.distribute(length_bytes)
        length = struct.unpack('!q', length_bytes)[0]
        self.read(length, self.read_hash, streaming_callback=self.handle_chunk)

    def handle_chunk(self, chunk):
        self.distribute(chunk)

    def read_hash(self, _):
        self.read(512 // 8, self.handle_hash)

    def handle_hash(self, hash_bytes):
        if not self.transaction:
            # not a single backend connection received the upload in full,
            # return error response
            result = struct.pack('!b', Constants.RESP_ERROR)
            self.write(result, self.frontend.read_op)
            return
        self.distribute(hash_bytes)
        for backend in self.transaction:
            backend.read(1, self.create_backend_handler(backend))

    def create_backend_handler(self, backend):
        def handler(b):
            self.handle_backend_response(backend, b)
        return handler

    def handle_backend_response(self, backend, status_bytes):
        backend.remove_close_callback(self._backend_closed)
        self.backends.remove(backend)
        status = struct.unpack('!b', status_bytes)[0]
        self.log.debug('{}: {}'.format(backend, status))
        if status != Constants.RESP_OK:
            self.frontend.remove_from_transaction(backend)
        if self.backends:
            # Not all backends have responded yet
            return
        if not self.transaction:
            result = struct.pack('!b', Constants.RESP_ERROR)
        else:
            result = struct.pack('!b', Constants.RESP_OK)
        self.write(result, self.frontend.read_op)

    def _backend_closed(self, backend):
        self.backends.remove(backend)
