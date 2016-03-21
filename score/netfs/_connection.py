import fcntl
import hashlib
import socket
import logging
import os
import shutil
import struct
from ._exceptions import CommitFailed, UploadFailed, DownloadFailed
from .constants import Constants
from transaction.interfaces import IDataManager
from zope.interface import implementer

log = logging.getLogger('score.netfs')


class NetfsConnection:

    CHUNK_SIZE = 1024 * 1024

    def __init__(self, conf):
        self.conf = conf
        if self.conf.host:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.conf.host, self.conf.port))
            self.socket.settimeout(None)

    def put(self, path, file, ctx=None, *, move=True):
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
            self.upload(path, file, ctx)

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

    def upload(self, path, file, ctx=None):
        """
        Puts the contents of given :term:`file object` *file* with given *path*
        onto the server.

        You must call :meth:`.commit` to actually persist the upload. If you are
        using the ctx module, though, you should pass a :term:`context object`
        as *ctx*. This will automatically commit the upload if the transaction
        was successful.
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
        if ctx:
            _CtxDataManager.join(self, ctx.tx_manager)

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
    def join(cls, connection, tx_manager):
        tx = tx_manager.get()
        if (connection, tx) not in cls._instances:
            tx.join(cls(connection, tx_manager))

    def __init__(self, connection, tx_manager):
        self.transaction_manager = tx_manager
        self.connection = connection
        self.__class__._instances.append((self.connection, tx_manager.get()))

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
