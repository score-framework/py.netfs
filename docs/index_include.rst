.. module:: score.netfs
.. role:: faint
.. role:: confkey

***********
score.netfs
***********

Introduction
============

Aim of this module is to provide an infrastructure for sharing arbitrary files
between multiple hosts. The reason we built this module is that we have the
need to distribute uploaded files (like images, or PDF files) across an array
of web servers and were not content with any other technology for our use case.

Environment
===========

In order to discuss the need for this module, one must first understand the
usual server setup of a web application with multiple servers. The following
diagram shall provide an overview of a typical scenario:: 

               HTTP Request
                    |
  <clients>         |     HTTP Request
                    |           |
                    .           .
  <other layers>    .           .
                    .           .
                    |           |
                    V           V
               +---------+   +---------+
  application  |  web01  |   |  web02  |  ...
  layer        +---------+   +---------+
                    |
                    +-------------+--
                    V             V
               +--------+   +--------+
  database     |  db01  |   |  db02  |  ...
  layer        +--------+   +--------+

We have a multitude of web servers handling requests. In order to have a robust
setup, where web servers are interchangeable, any web server must be able to
handle any HTTP request by the client.

The Problem
===========

The definition of our environment dictates that web01 must be able to serve a
file that was uploaded on web02, for instance. This means that we need some
algorithm for getting the file, that was uploaded on web02 onto web01's file
system.

There are three different strategies to solving this problem:

- Distribute on arrival: whenever web02 receives a file, it sends the file to
  all other web servers. This would presume that every web server knows every
  other web server. But the acual drawback is that one would need another
  strategy for retrieving missing files: It is well possible that this web
  server was offline for some time and did not receive an uploaded file.

- Retrieve on demand from peers: Whenever web01 needs a certail file, it
  fetches that file from somewhere. That somewhere might be any of the other
  web servers, which would mean that web01 "broadcasts" to all others a query
  ("Who has file X?") and downloads from one of those responding to the query.
  The issue here is that web01 would have to define a timeout and wait until
  all web servers respond in a defined time period before giving up and
  deciding that the file does not exist. This can make for a pretty bad user
  experience, once a peer is getting slow. Another drawback, which will be
  eliminated by the next strategy, is that it is hard to migrate files: they
  are, by the nature of this approach, distributed on multiple machines.

- Retrieve on demand from central server: Whenever a web server receives a
  file, it immediately uploads it to a central server. Any other web server,
  that needs a file, can ask that central file server for the file and
  immediately knows, if it exists. There is a single point of failure in this
  system, the effects of which can be mitigated by components described at a
  later point.


Our choice is strategy number three, now we need to define the means of the
transport, i.e. how do we upload/download files? Here are some options:

- A network share (like ``nfs`` or ``smb``) is the first idea, that comes to
  mind: It provides the convenience of a local file system and shared nature of
  a centralized server. The problem with these solutions is that all network
  shares start behaving in a very unfavorable way whenever there are networking
  errors. We have seen cases, where machines hang for *minutes*, even if the
  network share had an outage of a few milliseconds. So, whereas the *idea* is
  a great one, the implementations of these shares have some erratic "quirks".

- The next idea could be ``ftp``, a rock-stable protocol, where all timeouts
  can be specified in almost all client libraries. In spite of that, it still
  has two distinct drawbacks for us:

  - performance: Although everything is running on a local network, on pretty
    good hardware, we want our downloads to be as fast as possible. We
    espacially want to reduce the time-to-deliver from the web server's point
    of view, and the CPU and memory footprint on the central server. Although
    the hardware is usually pretty good, it should not have to waste its
    resources on BASE64-encoding and -decoding files.

  - race conditions: We do not want to download half-uploaded files, or create
    a broken file on the server through concurrent uploads. Unfortunately,
    these are issues on FTP servers, due to lack of support in the protocol
    itself.

The Solution
============

After some research and a *lot* of time used on testing various protocols, we
decided to implement our own simple client/server protocol. The result was the
score module called ``netfs``.

Features
--------

Distributed
```````````

This modules provides a dedicated proxy component, which acts as a façade to
multiple standalone backends. This setup allows one to ignore all backend
errors, as long as at least one backend is still functional.

Transactions
````````````

Since web projects usually work within transactions that can succeed or fail,
the communication implements transactions at its core. Whenever a client
connect, an implicit transaction is started. If the client disconnects without
committing the transaction (i.e. the web application had an error), all pending
operations are thrown away. If the client was successfull and wants to persists
all uploads, it just needs to tell the server to do so.

Locking
```````

The protol prevents mulitple writes onto the same file, as well as downloads of
incomplete files.

Hash Comparison
```````````````

All uploaded data is additionally checked for consistency with its SHA512-hash.
This will not fix any errors, but detect most erros in the transmission. It
also offers a good compromise between transmission speed and data integrity.

.. _netfs_proxy:

.. _netfs_protocol:

On the Wire
-----------

The protocol is extremely simple, where the party initiating the TCP connection
(=client) also initiates the conversation. The first thing the client sends is
a job byte, followed by job-specific data. The server always responds to the
request and once it is finished responding, the client can send the next
request.

The following list contains all possible requests accepted by the server:

.. _netfs_protocol_upload:

upload
``````

The client sends a file to the server, that can be persisted with a ``commit``
request (below)::

  +----------+
  |  1 Byte  |  Job Byte: always "1" for upload requests.
  +----------+
  |  4 Bytes |  Signed integer: Length of file name. This is the
  |          |    byte length of the UTF-8 encoded file name.
  +----------+
  |  ? Bytes |  File name: The UTF-8 encoded file name.
  |    ...   |
  +----------+
  |  8 Bytes |  Signed long long: Length of the file content.
  |          |
  +----------+
  |  ? Bytes |  File content
  |    ...   |
  +----------+
  | 64 Bytes |  SHA512-Hash of the file content.
  |          |
  +----------+

The server responds with a single byte to the whole request, even if it
encountered errors earlier. The response is either 1 for success, or 2 for
error. If the file is already being uploaded by another client, it is also
considered an error.

.. _netfs_protocol_prepare:

prepare
```````

An optional step that can be performed before a *commit* command is issued. It
causes the server to verify all uploads and issue an error response in case
something went wrong. The request consists of a single job byte::

  +----------+
  |  1 Byte  |  Job Byte: "3" for prepare request.
  +----------+

The response is a single byte: 1 for success, anything else in case of an
error.

.. _netfs_protocol_commit:

commit
``````

All uploaded files are persisted. Any existing files, that were re-uploaded,
will be overwritten. As the request has no payload, it just consists of a job
byte::

  +----------+
  |  1 Byte  |  Job Byte: "4" for commit request.
  +----------+

The response is again a single byte: 1 for success, anything else in case of an
error.

.. _netfs_protocol_rollback:

rollback
````````

Discards all pending uploads in this session. Consists of a single job byte::

  +----------+
  |  1 Byte  |  Job Byte: "5" for commit request.
  +----------+

.. _netfs_protocol_download:

download
````````

Requests the contents of a file::

  +----------+
  |  1 Byte  |  Job Byte: "2" for download requests.
  +----------+
  |  4 Bytes |  Signed integer: Length of file name. This is the
  |          |    byte length of the UTF-8 encoded file name.
  +----------+
  |  ? Bytes |  File name: The UTF-8 encoded file name.
  |    ...   |
  +----------+

If the file is found, the server responds with the exact same byte sequence, as
a client would initiate an upload, including the "job byte", which in this case
is the equivalant "status byte" 1, which indicates success.

.. todo::
    Download operation now also returns last modification time of the file on
    the server. This needs to either be implemented for the upload operation,
    or documented here separately.

Starting the Server
===================

Upon installation, this module registers a :mod:`score.cli` command that can be
used to manage a netfs server:

.. code-block:: console

    $ score netfs serve path/to/folder

Configuration
=============

.. autofunction:: score.netfs.init

.. autoclass:: score.netfs.ConfiguredNetfsModule()

    .. automethod:: score.netfs.ConfiguredNetfsModule.connect

.. autoclass:: score.netfs.NetfsConnection()

    .. automethod:: score.netfs.NetfsConnection.put

    .. automethod:: score.netfs.NetfsConnection.get

    .. automethod:: score.netfs.NetfsConnection.upload

    .. automethod:: score.netfs.NetfsConnection.commit

    .. automethod:: score.netfs.NetfsConnection.download
