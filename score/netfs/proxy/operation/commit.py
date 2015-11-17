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


class CommitOperation(Operation):

    def __init__(self, frontend):
        super().__init__(frontend, 'commit')
        if self.frontend.transaction_backends is None:
            self.log.debug('success (no operations)!')
            data = struct.pack('!b', Constants.RESP_OK)
            self.write(data, self.frontend.read_op)
            return
        if not self.frontend.transaction_backends:
            self.log.debug('error (no backends)!')
            data = struct.pack('!b', Constants.RESP_ERROR)
            self.write(data, self.frontend.read_op)
            return
        self.success = False
        data = struct.pack('!b', Constants.REQ_COMMIT)
        for backend in self.frontend.transaction_backends:
            backend.send(data)
            backend.read(1, self.create_backend_handler(backend))

    def create_backend_handler(self, backend):
        def handler(b):
            self.handle_backend_response(backend, b)
        return handler

    def handle_backend_response(self, backend, status_bytes):
        status = struct.unpack('!b', status_bytes)[0]
        self.log.debug('{}: {}'.format(backend, status))
        if status == Constants.RESP_OK:
            self.success = True
        self.frontend.transaction_backends.remove(backend)
        if self.frontend.transaction_backends:
            return
        self.frontend.transaction_backends = None
        if self.success:
            self.log.debug('success!')
            data = struct.pack('!b', Constants.RESP_OK)
        else:
            self.log.debug('error!')
            data = struct.pack('!b', Constants.RESP_ERROR)
        self.write(data, self.frontend.read_op)
