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

import fcntl
import hashlib
import logging
import os
from .constants import Constants
from score.init import (
    init_cache_folder, ConfiguredModule, parse_host_port, parse_bool)
import shutil
import socket
import struct
import tempfile
from transaction.interfaces import IDataManager
from zope.interface import implementer


log = logging.getLogger(__name__)
defaults = {
    # note: the 'server' value *must* consist of host and port,
    # otherwise the code in init() might cause an error.
    'server': 'localhost:14000',
    'cachedir': None,
    'deltmpcache': True,
    'ctx.member': 'netfs',
}


def init(confdict, ctx_conf=None):
    """
    Initializes this module acoording to :ref:`our module initialization
    guidelines <module_initialization>` with the following configuration keys:

    :confkey:`server` :faint:`[default=localhost:14000]`
        The server to connect to for all remote operations. Read using the
        generic :func:`score.init.parse_host_port`.

        The special value ``None`` indicates that all remote operations will
        immediately raise an exception. It is still possible to use higher
        level functions like :meth:`put <.ConfiguredNetfsModule.put>` and
        :meth:`get <.ConfiguredNetfsModule.get>` (although :meth:`get
        <.ConfiguredNetfsModule.get>` will raise an exception if the requested
        file is not present in the local folder.)

    :confkey:`cachedir` :faint:`[default=None]`
        A local folder that will hold downloaded files. If this value is
        omitted, the module will create a new temporary folder on demand, that
        will be used as the local folder for this session.

    :confkey:`deltmpcache` :faint:`[default=True]`
        This option is only relevant if the configuration did not contain a
        ``cachedir``. If this value is `True`, any temporary folder that was
        created for this session—as described for the configuration value
        ``cachedir``, above—will be removed when the
        :class:`.ConfiguredNetfsModule` is freed.

        The only use case where you might need this configuration is when you
        want to operate on a temporary folder, but still keep its contents when
        you are done with this module.

    """
    conf = dict(defaults.items())
    conf.update(confdict)
    if conf['server'] in (None, 'None'):
        host, port = None, None
    else:
        host, port = parse_host_port(conf['server'], defaults['server'])
    cachedir = None
    delcache = False
    if conf['cachedir']:
        cachedir = init_cache_folder(conf, 'cachedir')
    else:
        delcache = parse_bool(conf['deltmpcache'])
    c = ConfiguredNetfsModule(host, port, cachedir, delcache)
    c.ctx_conf = ctx_conf
    if ctx_conf and conf['ctx.member'] not in ('None', None):
        ctx_conf.register(conf['ctx.member'], lambda _: c.connect())
    return c


class CommitFailed(Exception):
    pass


class UploadFailed(Exception):
    pass


class DownloadFailed(Exception):
    pass


class ConfiguredNetfsModule(ConfiguredModule):
    """
    This module's :class:`configuration class
    <score.init.ConfiguredModule>`.
    """

    def __init__(self, host, port, cachedir, delcache):
        super().__init__(__package__)
        self.host = host
        self.port = port
        self._cachedir = cachedir
        self.delcache = delcache

    def __del__(self):
        if self.delcache and self._cachedir:
            shutil.rmtree(self._cachedir)

    @property
    def cachedir(self):
        if not self._cachedir:
            self._cachedir = tempfile.mkdtemp(prefix='netfs-', suffix='.tmp')
        return self._cachedir

    def connect(self):
        """
        Connects to the configured server and returns a
        :class:`.NetfsConnection`.
        """
        return NetfsConnection(self)


class NetfsConnection:

    CHUNK_SIZE = 1024 * 1024

    def __init__(self, conf):
        self.conf = conf
        if self.conf.host:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.conf.host, self.conf.port))
            self.socket.settimeout(None)

    def put(self, path, file, *, move=True):
        """
        Uploads a file with given *path* to the server and moves it into the
        cache folder. The *file* is either a string (denoting a file system path
        to the file), or a :term:`file object`.

        In case the *file* parameter was a string, it is possible to keep that
        original file in place by specifying a falsy value for *move*.
        """
        realpath = os.path.realpath(os.path.join(self.conf.cachedir, path))
        path_prefix = os.path.commonprefix((self.conf.cachedir, realpath))
        if path_prefix != self.conf.cachedir:
            raise ValueError('Invalid path: ' + path)
        dirname = os.path.dirname(realpath)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        if isinstance(file, str):
            if move:
                shutil.move(file, realpath)
            else:
                shutil.copy2(file, realpath)
            file = open(realpath, 'rb')
        else:
            shutil.copyfileobj(file, open(realpath, 'wb'))
        if self.conf.host:
            self.upload(path, file)

    def get(self, path):
        """
        Returns the local path to a file, downloading it from the server, if it
        does not already exist in the local cache folder.
        """
        realpath = os.path.realpath(os.path.join(self.conf.cachedir, path))
        path_prefix = os.path.commonprefix((self.conf.cachedir, realpath))
        if path_prefix != self.conf.cachedir:
            raise ValueError('Invalid path: ' + path)
        if os.path.exists(realpath):
            return realpath
        tmpfile = realpath + '.tmp'
        dirname = os.path.dirname(realpath)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        file = open(tmpfile, 'wb')
        try:
            fcntl.flock(file, fcntl.LOCK_EX)
            if os.path.exists(realpath):
                # another process downloaded the file
                return realpath
            time = self.download(path, file)
            os.rename(tmpfile, realpath)
            os.utime(realpath, (time, time))
        finally:
            fcntl.flock(file, fcntl.LOCK_UN)
            file.close()
        return realpath

    def upload(self, path, file):
        """
        Puts the contents of given :term:`file object` *file* with given *path*
        onto the server. Do not forget to call :meth:`.commit` when you're done!
        """
        if self.conf.host is None:
            raise UploadFailed('No server configured')
        if not isinstance(path, bytes):
            path = path.encode('UTF-8')
        data = struct.pack('b', Constants.REQ_UPLOAD)
        data += struct.pack('!i', len(path))
        data += path
        file.seek(0, 2)
        data += struct.pack('!q', file.tell())
        file.seek(0, 0)
        sha = hashlib.sha512()
        self._send(data)
        chunk = file.read(self.CHUNK_SIZE)
        while chunk:
            self._send(chunk)
            sha.update(chunk)
            chunk = file.read(self.CHUNK_SIZE)
        self._send(sha.digest())
        response = struct.unpack('b', self._read(1))[0]
        if response != Constants.RESP_OK:
            raise UploadFailed()
        if self.conf.ctx_conf:
            _CtxDataManager.join(self, self.conf.ctx_conf.tx_manager.get())

    def prepare(self):
        """
        Prepares the current transaction. Raises *CommitFailed* if the server
        responded with an error code.
        """
        if self.conf.host is None:
            return
        self._send(struct.pack('b', Constants.REQ_PREPARE))
        response = struct.unpack('b', self._read(1))[0]
        if response != Constants.RESP_OK:
            raise CommitFailed()

    def commit(self):
        """
        Instructs the server to persist all uploaded files, so that other
        clients can find them.
        """
        if self.conf.host is None:
            return
        self._send(struct.pack('b', Constants.REQ_COMMIT))
        response = struct.unpack('b', self._read(1))[0]
        if response != Constants.RESP_OK:
            raise CommitFailed()

    def rollback(self):
        """
        Sends a rollback command to the server.
        """
        if self.conf.host is None:
            return
        self._send(struct.pack('b', Constants.REQ_ROLLBACK))

    def download(self, path, file, retry=1):
        """
        Downloads the file with given *path* from the server and writes it into
        the :term:`file object` *file*.
        """
        if self.conf.host is None:
            raise DownloadFailed('No server configured')
        if not isinstance(path, bytes):
            path = path.encode('UTF-8')
        data = struct.pack('b', Constants.REQ_DOWNLOAD)
        data += struct.pack('!i', len(path))
        data += path
        self._send(data)
        response = struct.unpack('b', self._read(1))[0]
        if response != Constants.RESP_OK:
            raise DownloadFailed(path)
        length = struct.unpack('!q', self._read(8))[0]
        sha = hashlib.sha512()
        while length:
            chunk_size = min(self.CHUNK_SIZE, length)
            chunk = self._read(chunk_size)
            sha.update(chunk)
            file.write(chunk)
            length -= chunk_size
        hash = self._read(512 // 8)
        if sha.digest() != hash:
            if retry > 0:
                return self.download(path, file, retry - 1)
            raise DownloadFailed(path)
        return struct.unpack('!i', self._read(4))[0]

    def _send(self, data):
        log.debug('sending: {}'.format(data))
        totalsent = 0
        while totalsent < len(data):
            sent = self.socket.send(data[totalsent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent

    def _read(self, length):
        chunks = []
        bytes_recd = 0
        while bytes_recd < length:
            chunk = self.socket.recv(min(length - bytes_recd, 2048))
            if chunk == b'':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        log.debug('receiving {}: {}'.format(length, b''.join(chunks)))
        return b''.join(chunks)


@implementer(IDataManager)
class _CtxDataManager:
    """
    An :interface:`IDataManager <transaction.interfaces.IDataManager>`, which
    will commit all uploaded files at the end of the transaction.
    """

    _instances = []

    @classmethod
    def join(cls, connection, tx):
        if (connection, tx) not in cls._instances:
            tx.join(cls(connection, tx))

    def __init__(self, connection, transaction):
        self.transaction_manager = connection.conf.ctx_conf.tx_manager
        self.connection = connection
        self.__class__._instances.append((self.connection, transaction))

    def abort(self, transaction):
        self.connection.rollback()
        self.__class__._instances.remove((self.connection, transaction))

    def tpc_begin(self, transaction):
        pass

    def commit(self, transaction):
        pass

    def tpc_vote(self, transaction):
        self.connection.prepare()

    def tpc_finish(self, transaction):
        self.connection.commit()
        self.__class__._instances.remove((self.connection, transaction))

    def tpc_abort(self, transaction):
        self.connection.rollback()
        self.__class__._instances.remove((self.connection, transaction))

    def sortKey(self):
        return 'score.netfs(%d)' % id(self)
