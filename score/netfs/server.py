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
import hashlib
import os
import shutil
import struct
import tempfile
from tornado.tcpserver import TCPServer
from .constants import Constants


log = logging.getLogger(__name__)


class UploadError(Exception):
    pass


class FileUpload:
    """
    An ongoing file upload process. After this object is created, it expects the
    following members to be set in the specified order:

    - ``upload.path = 'path/to/file'``
    - ``upload.write(chunk)``
    - ``upload.hash = sha512-hash``
    - ``upload.finish()``

    All errors will be silantly registered, up until the call to
    :meth:`.finish`. That last function call will either return `None` (in case
    of success), or raise an UploadError with a message describing the error
    that occurred in any of the previous steps.

    If any of the file chunks could not be written, for example, the upload will
    continue normally and will raise an :class:`.UploadError` when
    :meth:`.finish` is called with the message "ErrorWritingFile".
    """

    def __init__(self, communication):
        self.communication = communication
        self._path = None
        self.sha = hashlib.sha512()
        self._error = None
        self.committed = False

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        """
        Sets the path, makes sure the designated folder exists and creates a
        lock file to prevent parallell processing of the same file.
        """
        self._path = value
        os.makedirs(os.path.dirname(value), exist_ok=True)
        self.tmp = value + '.tmp'
        try:
            op = next(op for op in self.communication.transaction
                         if isinstance(op, FileUpload)
                         and op.tmp == self.tmp)
            op.abort()
            self.communication.transaction.remove(op)
        except StopIteration:
            pass
        try:
            self.file = open(self.tmp, 'xb')
            # TODO: file lock!
        except OSError as e:
            log.error(e)
            self.error = 'ErrorOpeningFile'

    def __del__(self):
        """
        Remove temporary file on destruction.
        """
        if self.tmp is None:
            return
        try:
            os.unlink(self.tmp)
        except:
            pass

    def abort(self):
        if self.committed:
            os.unlink(self.path)
            if self.tmp:
                shutil.move(self.tmp, self.path)
        self.error = 'Aborted'

    def prepare(self):
        """
        Prepares the operation to be committed and raises an Exception if a
        commit would fail.
        """
        # test if the target path is writable
        open(self.path, 'ab')

    def commit(self):
        """
        Commits this operation, i.e. moves the temporary file to its rightful
        path.
        """
        if os.path.exists(self.path):
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self.path))
            os.close(fd)
            shutil.move(self.path, tmp)
            shutil.move(self.tmp, self.path)
            self.tmp = tmp
        else:
            shutil.move(self.tmp, self.path)
            self.tmp = None
        self.committed = True

    def write(self, chunk):
        """
        Writes a part of the file. See class description for details.
        """
        if self.error:
            return
        try:
            self.file.write(chunk)
            self.sha.update(chunk)
        except OSError:
            self.error = 'ErrorWritingFile'

    def finish(self):
        """
        Wraps up the upload process. See class description for details.
        """
        if self.error is None:
            try:
                self.file.close()
            except OSError:
                self.error = 'ErrorClosingFile'
            else:
                if self.sha.digest() != self.hash:
                    self.error = 'HashMismatch'
        if self.error:
            raise UploadError(self.error)

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, error):
        """
        Removes temporary file on error.
        """
        self.file = None
        self._error = error
        try:
            os.unlink(self.tmp)
        except:
            pass


class Communication:
    """
    The conversation between a client and this server process. See
    :ref:`narrative documentation <netfs_protocol>` for the details of the whole
    communication.
    """

    CHUNK_SIZE = 1024 * 32

    def __init__(self, server, stream):
        self.server = server
        self.stream = stream
        self.transaction = []
        stream.set_close_callback(self.stream_closed)
        self.read_op()

    def stream_closed(self):
        """
        Abort all pending operations if the connection is closed prematurely.
        """
        for op in self.transaction:
            op.abort()

    def read_op(self):
        """
        Read the :ref:`job byte <netfs_protocol>` and call `.handle_op`.
        """
        self.stream.read_bytes(1, self.handle_op)

    def handle_op(self, op_bytes):
        """
        Starts the operation designated by given :ref:`job byte
        <netfs_protocol>`.
        """
        op = struct.unpack('!b', op_bytes)[0]
        if op == Constants.REQ_UPLOAD:
            return self.handle_upload()
        elif op == Constants.REQ_PREPARE:
            return self.handle_prepare()
        elif op == Constants.REQ_COMMIT:
            return self.handle_commit()
        elif op == Constants.REQ_ROLLBACK:
            return self.handle_rollback()
        elif op == Constants.REQ_DOWNLOAD:
            return self.handle_download()
        else:
            log.error('Received bogus request byte %d' % op)
            self.stream.close()

    def handle_prepare(self):
        """
        Handles a ``prepare`` operation. See :ref:`narrative documentation
        <netfs_protocol_prepare>` for details.
        """
        log.debug('prepare')
        try:
            for op in self.transaction:
                op.prepare()
            result = Constants.RESP_OK
        except:
            result = Constants.RESP_ERROR
        result = struct.pack('!b', result)
        self.stream.write(result, self.read_op)

    def handle_rollback(self):
        """
        Handles a ``rollback`` operation. See :ref:`narrative documentation
        <netfs_protocol_rollback>` for details.
        """
        log.debug('rollback')
        for op in self.transaction:
            op.abort()
        self.transaction = []

    def handle_commit(self):
        """
        Handles a ``commit`` operation. See :ref:`narrative documentation
        <netfs_protocol_commit>` for details.
        """
        log.debug('commit')
        for op in self.transaction:
            try:
                op.commit()
            except Exception as e:
                log.debug(e)
                for op in self.transaction:
                    op.abort()
                self.transaction = []
                result = struct.pack('!b', Constants.RESP_ERROR)
                self.stream.write(result, self.read_op)
                return
        self.transaction = []
        result = struct.pack('!b', Constants.RESP_OK)
        self.stream.write(result, self.read_op)

    def handle_upload(self):
        """
        Handles a ``upload`` operation. See :ref:`narrative documentation
        <netfs_protocol_upload>` for details.
        """
        upload = FileUpload(self)
        log.debug('upload')

        def read_name_length(length_bytes):
            length = struct.unpack('!i', length_bytes)[0]
            log.debug('  name length = {}'.format(length))
            self.stream.read_bytes(length, read_name)

        def read_name(name_bytes):
            upload.path = self.get_path(str(name_bytes, 'UTF-8'))
            log.debug('  name = {}'.format(upload.path))
            self.stream.read_bytes(8, read_content_length)

        def read_content_length(length_bytes):
            log.debug('  content length bytes = {}'.format(length_bytes))
            length = struct.unpack('!q', length_bytes)[0]
            log.debug('  content length = {}'.format(length))
            self.stream.read_bytes(length, finished,
                                   streaming_callback=handle_chunk)

        def handle_chunk(chunk):
            log.debug('  ... chunk ...')
            upload.write(chunk)

        def finished(_):
            log.debug('  ... done')
            self.stream.read_bytes(512 // 8, read_hash)

        def read_hash(hash_bytes):
            upload.hash = hash_bytes
            log.debug('  hash = {}'.format(upload.hash))
            try:
                upload.finish()
                log.debug('  all ok!')
                self.transaction.append(upload)
                result = struct.pack('!b', Constants.RESP_OK)
                self.stream.write(result, self.read_op)
            except UploadError as e:
                log.warn(e)
                result = struct.pack('!b', Constants.RESP_ERROR)
                self.stream.write(result, self.read_op)
        self.stream.read_bytes(4, read_name_length)

    def handle_download(self):
        """
        Handles a ``download`` operation. See :ref:`narrative documentation
        <netfs_protocol_upload>` for details.
        """
        log.debug('download')
        path = None
        file = None
        sha = hashlib.sha512()

        def read_name_length(length_bytes):
            length = struct.unpack('!i', length_bytes)[0]
            log.debug('  name length = {}'.format(length))
            self.stream.read_bytes(length, read_name)

        def read_name(name_bytes):
            nonlocal path, file, sha
            name = self.get_path(str(name_bytes, 'UTF-8'))
            log.debug('  name = {}'.format(name))
            path = self.get_path(name)
            if os.path.exists(path + '.tmp'):
                log.debug('  uploading')
                data = struct.pack('!b', Constants.RESP_UPLOADING)
                self.stream.write(data, self.read_op)
                return
            try:
                file = open(path, 'rb')
            except OSError:
                log.debug('  not found')
                data = struct.pack('!b', Constants.RESP_NOTFOUND)
                self.stream.write(data, self.read_op)
                return
            try:
                # seek to end, read file size, go back to beginning of file
                file.seek(0, 2)
                data = struct.pack('!b', Constants.RESP_OK)
                data += struct.pack('!q', file.tell())
                log.debug('  length = {}'.format(file.tell()))
                file.seek(0, 0)
                self.stream.write(data, write_chunk)
            except OSError as e:
                log.debug('  error')
                log.error(e)
                data = struct.pack('!b', Constants.RESP_ERROR)
                self.stream.write(data, self.read_op)

        def write_chunk():
            chunk = file.read(self.CHUNK_SIZE)
            if chunk:
                sha.update(chunk)
                self.stream.write(chunk, write_chunk)
                log.debug('  ... chunk ... ({})'.format(len(chunk)))
                return
            log.debug('  ... done')
            file.close()
            log.debug('  hash = {}'.format(sha.digest()))
            data = sha.digest()
            data += struct.pack('!i', int(os.path.getmtime(path)))
            self.stream.write(data, self.read_op)

        self.stream.read_bytes(4, read_name_length)

    def get_path(self, name):
        """
        Gets the real, absolute path to the file designated by *name*.
        """
        return self.server.get_path(name)


class StorageServer(TCPServer):
    """
    A :class:`tornado.tcpserver.TCPServer` that handles netfs clients.

    The first parameter must be the path to the folder where files should be
    managed. All other arguments are passed to the parent constructor.
    """

    def __init__(self, root, **kwargs):
        self.root = os.path.realpath(root)
        TCPServer.__init__(self, **kwargs)

    def handle_stream(self, stream, address):
        Communication(self, stream)

    def get_path(self, name):
        """
        Gets the real, absolute path to the file designated by *name*.
        """
        path = os.path.join(self.root, name)
        if os.path.commonprefix([path, self.root]) != self.root:
            raise ValueError('Invalid path "%s"' % name)
        return path
