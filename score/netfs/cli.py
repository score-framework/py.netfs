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
import logging.config
import score.netfs as netfs
import score.init
import os.path


log = logging.getLogger(__name__)


def init_logging(logconf):
    if logconf is None:
        return
    conf = score.init.config.parse_config_file(logconf)
    logging.config.fileConfig(conf, disable_existing_loggers=False)


@click.group()
def main():
    """
    Client and server for netfs.
    """


@main.command('serve')
@click.option('-h', '--host', default='0.0.0.0')
@click.option('-p', '--port', default=14000)
@click.option('-l', '--logconf',
              type=click.Path(file_okay=True, dir_okay=False))
@click.argument('folder', type=click.Path(file_okay=False, dir_okay=True))
def serve(folder, host, port, logconf=None):
    init_logging(logconf)
    from tornado.ioloop import IOLoop
    from .server import StorageServer
    try:
        server = StorageServer(folder)
        server.listen(port, address=host)
        IOLoop.instance().start()
        IOLoop.instance().close()
    except Exception as e:
        log.exception(e)
        raise


def read_server_conf(section):
    try:
        folder = section['folder']
    except KeyError:
        raise click.ClickException('No folder configuration found')
    try:
        host = section['host']
    except KeyError:
        host = '0.0.0.0'
    try:
        port = int(section['port'])
    except KeyError:
        port = 14000
    except ValueError:
        raise click.ClickException('Configured port could not be parsed')
    return folder, host, port


@main.command('serve-conf')
@click.argument('conf', type=click.Path(file_okay=True, dir_okay=False))
@click.argument('name', required=False)
def serve_conf(conf, name=None):
    # NOTE: the \b in the following string causes click to print a paragraph
    # verbatim, i.e. without rewrapping its content.
    """
    Start server with config file.

    The configuration file must either contain a section [server] with the
    following keys:

    \b
    - `host` (default: 0.0.0.0)
    - `port` (default: 14000)
    - `folder` (required, no default).

    If an optional NAME is provided, the application will instead look into the
    section [server.NAME], but the expected configuration format does not
    change.
    """
    conf = score.init.parse_config_file(conf)
    if 'loggers' in conf:
        logging.config.fileConfig(conf, disable_existing_loggers=False)
    section_name = 'server'
    if name:
        section_name = 'server-%s' % name
    try:
        section = conf[section_name]
    except KeyError:
        raise click.ClickException('Section "%s" not found in configuration'
                                   % section_name)
    folder, host, port = read_server_conf(section)
    if not os.path.isdir(folder):
        raise click.ClickException('Configured folder (%s) does not exist'
                                   % folder)
    from tornado.ioloop import IOLoop
    from .server import StorageServer
    try:
        server = StorageServer(folder)
        server.listen(port, address=host)
        IOLoop.instance().start()
        IOLoop.instance().close()
    except Exception as e:
        log.exception(e)
        raise


@main.command('proxy')
@click.option('-h', '--host', default='0.0.0.0')
@click.option('-p', '--port', default=14000, type=int)
@click.option('-b', '--backend', multiple=True)
@click.option('-l', '--logconf',
              type=click.Path(file_okay=True, dir_okay=False))
def proxy(host, port, backend, logconf=None):
    init_logging(logconf)
    from tornado.ioloop import IOLoop
    from .proxy import ProxyServer
    backends = []
    for b in backend:
        b = b.split(':')
        b[1] = int(b[1])
        backends.append(b)
    try:
        server = ProxyServer(backends)
        server.listen(port, address=host)
        IOLoop.instance().start()
        IOLoop.instance().close()
    except Exception as e:
        log.exception(e)
        raise


def read_proxy_conf(section):
    host = '0.0.0.0'
    port = 14000
    backends = None
    try:
        host = section['host']
    except KeyError:
        pass
    try:
        port = int(section['port'])
    except KeyError:
        port = 14000
    except ValueError:
        raise click.ClickException('Configured port (%s) could not be parsed'
                                   % section['port'])
    try:
        backend_list = score.init.parse_list(section['backends'])
    except KeyError:
        pass
    else:
        backends = []
        for b in backend_list:
            b = b.split(':')
            b[1] = int(b[1])
            backends.append(b)
    return host, port, backends


@main.command('proxy-conf')
@click.argument('conf', type=click.Path(file_okay=True, dir_okay=False))
def proxy_conf(conf, name=None):
    """
    Start proxy with config file.

    The configuration file may contain a section [proxy] with the following
    keys:

    \b
    - `host` (default: 0.0.0.0)
    - `port` (default: 14000)
    - `backends` (default: see below).

    If no such section is found or the section contains no backends definition,
    the configuration will be done using all other sections in the file, parsing
    all [server] and [server.NAME] sections as described in the command
    serve-conf.
    """
    conf = score.init.parse_config_file(conf)
    if 'loggers' in conf:
        logging.config.fileConfig(conf, disable_existing_loggers=False)
    if 'proxy' in conf:
        host, port, backends = read_proxy_conf(conf['proxy'])
        if backends is None:
            log.debug('No backends configured, browsing all sections')
    else:
        log.debug('No proxy config section, falling back to defaults')
        host = '0.0.0.0'
        port = 14000
        backends = None
    if backends is None:
        backends = []
        for section in conf:
            if section == 'server' or section.startswith('server-'):
                # calling the variables here h and p so they don't clash with
                # the proxy host/port
                _, h, p = read_server_conf(conf[section])
                if (h, p) in backends:
                    log.debug('Ignoring duplicate backend %s:%d' % (h, p))
                    continue
                log.debug('Adding backend %s:%d' % (h, p))
                backends.append((h, p))
    if not backends:
        raise click.ClickException('No backends configured')
    from tornado.ioloop import IOLoop
    from .proxy import ProxyServer
    try:
        log.debug('Starting proxy at %s:%d' % (host, port))
        server = ProxyServer(backends)
        server.listen(port, address=host)
        IOLoop.instance().start()
        IOLoop.instance().close()
    except Exception as e:
        log.exception(e)
        raise


@main.command('download')
@click.option('-h', '--host', default='127.0.0.1')
@click.option('-p', '--port', default=14000, type=int)
@click.option('-l', '--logconf',
              type=click.Path(file_okay=True, dir_okay=False))
@click.argument('path')
@click.argument('file', type=click.File(mode='wb'))
def download(host, port, path, file, logconf=None):
    init_logging(logconf)
    conf = netfs.init({'server': '{}:{}'.format(host, port), 'cachedir': '.'})
    conf.connect().download(path, file)


@main.command('upload')
@click.option('-h', '--host', default='127.0.0.1')
@click.option('-p', '--port', default=14000, type=int)
@click.option('-l', '--logconf',
              type=click.Path(file_okay=True, dir_okay=False))
@click.argument('path')
@click.argument('file', type=click.Path(file_okay=True, dir_okay=False))
def upload(host, port, path, file, logconf=None):
    init_logging(logconf)
    conf = netfs.init({'server': '{}:{}'.format(host, port), 'cachedir': '.'})
    fp = open(file, 'rb')
    conn = conf.connect()
    conn.upload(path, fp)
    conn.commit()

if __name__ == '__main__':
    main()
