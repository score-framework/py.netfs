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

import click
import logging
import score.netfs as netfs


log = logging.getLogger(__name__)


@click.group()
def main():
    """
    Client and server for netfs.
    """


@main.command('serve')
@click.option('-h', '--host', default='0.0.0.0')
@click.option('-p', '--port', default=14000)
@click.option('-d', '--debug', is_flag=True, default=False)
@click.argument('root', type=click.Path(file_okay=False, dir_okay=True))
def serve(root, host, port, debug):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    from tornado.ioloop import IOLoop
    from .server import StorageServer
    server = StorageServer(root)
    server.listen(port, address=host)
    IOLoop.instance().start()
    IOLoop.instance().close()


@main.command('proxy')
@click.option('-h', '--host', default='0.0.0.0')
@click.option('-p', '--port', default=14000, type=int)
@click.option('-b', '--backend', multiple=True)
@click.option('-d', '--debug', is_flag=True, default=False)
def proxy(host, port, debug, backend):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    from tornado.ioloop import IOLoop
    from .proxy import ProxyServer
    backends = []
    for b in backend:
        b = b.split(':')
        b[1] = int(b[1])
        backends.append(b)
    server = ProxyServer(backends)
    server.listen(port, address=host)
    IOLoop.instance().start()
    IOLoop.instance().close()


@main.command('download')
@click.option('-h', '--host', default='127.0.0.1')
@click.option('-p', '--port', default=14000, type=int)
@click.option('-d', '--debug', is_flag=True, default=False)
@click.argument('path')
@click.argument('file', type=click.File(mode='wb'))
def download(host, port, path, file, debug):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    conf = netfs.init({'host': host, 'port': port, 'cachedir': '.'})
    conf.download(path, file)


@main.command('upload')
@click.option('-h', '--host', default='127.0.0.1')
@click.option('-p', '--port', default=14000, type=int)
@click.option('-d', '--debug', is_flag=True, default=False)
@click.argument('path')
@click.argument('file', type=click.Path(file_okay=True, dir_okay=False))
def upload(host, port, path, file, debug):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    conf = netfs.init({'server': '{}:{}'.format(host, port), 'cachedir': '.'})
    fp = open(file, 'rb')
    conf.upload(path, fp)
    conf.commit()

if __name__ == '__main__':
    main()
