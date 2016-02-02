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
import shutil
from ._connection import NetfsConnection
from score.init import (
    init_cache_folder, ConfiguredModule, parse_host_port, parse_bool)
import tempfile


log = logging.getLogger(__name__)
defaults = {
    # note: the 'server' value *must* consist of host and port,
    # otherwise the code in init() might cause an error.
    'server': 'localhost:14000',
    'cachedir': None,
    'deltmpcache': True,
    'ctx.member': 'netfs',
}


def init(confdict, ctx=None):
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
    c.ctx_conf = ctx
    if ctx and conf['ctx.member'] not in ('None', None):
        ctx.register(conf['ctx.member'], lambda _: c.connect())
    return c


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
