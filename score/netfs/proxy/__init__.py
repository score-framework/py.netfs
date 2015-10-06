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

import logging
from score.netfs.constants import Constants
import struct
from tornado.tcpserver import TCPServer
from .backend import Backend, NotConnected
from .operation import (DownloadOperation, CommitOperation, PrepareOperation,
                        UploadOperation)


log = logging.getLogger(__name__)


class FrontendCommunication:

    def __init__(self, server, stream):
        self.server = server
        self.stream = stream
        self.stream.set_close_callback(self._stream_closed)
        self.transaction_backends = None
        self.read_op()

    def init_transaction(self, callback):
        if self.transaction_backends is not None:
            callback(self.transaction_backends)
            return
        self.transaction_backends = []
        def check(backend):
            try:
                remaining.remove(backend)
                if not remaining:
                    callback(self.transaction_backends)
            except ValueError:
                pass
        def connected(backend):
            backend.add_close_callback(self._backend_closed)
            self.transaction_backends.append(backend)
            check(backend)
        def failed(backend):
            check(backend)
        remaining = []
        for backend in self.backends:
            remaining.append(backend.transaction(connected, failed))

    def remove_from_transaction(self, backend):
        try:
            backend.send(struct.pack('!b', Constants.REQ_ROLLBACK))
        except NotConnected:
            pass
        try:
            self.transaction_backends.remove(backend)
        except ValueError:
            pass

    def read_op(self):
        if self.stream:
            self.stream.read_bytes(1, self.handle_op)

    def handle_op(self, op_bytes):
        op = struct.unpack('!b', op_bytes)[0]
        if op == Constants.REQ_UPLOAD:
            return UploadOperation(self)
        elif op == Constants.REQ_ROLLBACK:
            return self.handle_rollback_request()
        elif op == Constants.REQ_PREPARE:
            return PrepareOperation(self)
        elif op == Constants.REQ_COMMIT:
            return CommitOperation(self)
        elif op == Constants.REQ_DOWNLOAD:
            return DownloadOperation(self)
        else:
            log.error('Received bogus request byte %d' % op)
            self.terminate()

    def terminate(self):
        if self.stream:
            log.debug('terminate')
            self.stream.close()
            self.stream = None

    def read(self, *args, **kwargs):
        if self.stream:
            self.stream.read_bytes(*args, **kwargs)

    def write(self, *args, **kwargs):
        if self.stream:
            self.stream.write(*args, **kwargs)

    @property
    def backends(self):
        return self.server.backends

    def handle_rollback_request(self):
        log.debug('rollback')
        if self.transaction_backends is None:
            self.read_op()
            return
        data = struct.pack('!b', Constants.REQ_ROLLBACK)
        for backend in self.transaction_backends:
            backend.remove_close_callback(self._backend_closed)
            try:
                backend.send(data)
            except NotConnected:
                pass
        self.transaction_backends = None
        self.read_op()

    def _backend_closed(self, backend):
        if self.transaction_backends is None:
            return
        try:
            self.transaction_backends.remove(backend)
        except ValueError:
            pass

    def _stream_closed(self):
        log.debug('connection closed')
        self.stream = None
        if not self.transaction_backends:
            return
        for backend in self.transaction_backends:
            backend.close()


class ProxyServer(TCPServer):

    def __init__(self, backends, **kwargs):
        self.backends = [Backend(b[0], b[1]) for b in backends]
        TCPServer.__init__(self, **kwargs)

    def handle_stream(self, stream, address):
        FrontendCommunication(self, stream)
