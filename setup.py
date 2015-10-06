import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

setup(
    name='score.netfs',
    version='0.3.4'
    description='Distributed file storage of The SCORE Framework',
    long_description=README,
    author='strg.at',
    author_email='score@strg.at',
    url='http://score-framework.org',
    keywords='score framework web fs ftp smb',
    packages=['score.netfs',
              'score.netfs.proxy',
              'score.netfs.proxy.operation'],
    install_requires=[
        'score.init >= 0.2.4'
    ],
    extras_require={
        'server': ['tornado']
    },
    entry_points={
        'score.cli': [
            'netfs = score.netfs.cli:main',
        ]
    },
)
