#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-03
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import logging
import os
import pathlib
import sys

import click
import nest_asyncio
from click_default_group import DefaultGroup

from sdsstools.daemonizer import DaemonGroup, cli_coro

from cerebro import Cerebro


# This allows to do loop.run_forever() while the loop is already running.
nest_asyncio.apply()


CWD = os.getcwd()


# This changes the root logger level to DEBUG so that asyncio silent
# exceptions are propagated. Since the sdsstools logger has its own level
# control this should not affect it.
# See https://docs.python.org/3/library/asyncio-dev.html#asyncio-debug-mode
# asyncio_log = logging.getLogger('asyncio')
if os.environ.get('PYTHONASYNCIODEBUG', '0') != '0':
    logging.basicConfig(level=logging.DEBUG)


if sys.platform in ['linux', 'linux2', 'darwin']:
    pidfile = '/var/tmp/cerebro.pid'
else:
    raise RuntimeError('Cannot run cerebro in Windows.')


@click.group(cls=DefaultGroup, default='daemon', default_if_no_args=True)
def cerebro():
    """Command Line Interface for cerebro."""

    pass


@cerebro.group(cls=DaemonGroup, prog='daemon', workdir=CWD, pidfile=pidfile)
@click.option('--config', type=click.Path(exists=True, dir_okay=False),
              help='Absolute path to config file. Defaults to internal config.')
@cli_coro(debug=True)
async def daemon(config, logfile, no_log_rotate):
    """Handle the daemon."""

    if not config:
        config = (pathlib.Path(__file__).parent / 'etc' / 'cerebro.yaml')
    else:
        config = os.path.realpath(config)

    cerebro = Cerebro(config=config)

    try:
        cerebro.loop.run_forever()
    except KeyboardInterrupt:
        cerebro.stop()
        cerebro.loop.stop()


def main():
    cerebro()


if __name__ == '__main__':
    main()
