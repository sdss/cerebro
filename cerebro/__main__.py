#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-03
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import asyncio
import os
import pathlib
import sys

import click
from click_default_group import DefaultGroup

from sdsstools.daemonizer import DaemonGroup, cli_coro

from cerebro import Cerebro


if sys.platform in ["linux", "linux2", "darwin"]:
    pidfile = "/var/tmp/cerebro.pid"
else:
    raise RuntimeError("Cannot run cerebro in Windows.")


@click.group(cls=DefaultGroup, default="daemon", default_if_no_args=True)
@click.option(
    "--sources",
    type=str,
    help="Comma-separated list of sources to start.",
)
@click.option(
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    help="Absolute path to config file. Defaults to internal config.",
)
@click.pass_context
def cerebro(ctx, sources, config):
    """Command Line Interface for cerebro."""

    if not config:
        config = pathlib.Path(__file__).parent / "etc" / "cerebro.yaml"
    else:
        config = os.path.realpath(config)

    sources = sources.split(",") if sources else []

    ctx.obj = {"sources": sources, "config": config}


@cerebro.group(
    cls=DaemonGroup,
    prog="cerebro_daemon",
    workdir=os.getcwd(),
    pidfile=pidfile,
)
@cli_coro()
@click.pass_context
async def daemon(ctx):
    """Handle the daemon."""

    cerebro = Cerebro(config=ctx.obj["config"], sources=ctx.obj["sources"])

    # Hacky way to run forever. nest_asyncio has some issues with CLU.
    while True:
        try:
            await asyncio.sleep(60)
        except KeyboardInterrupt:
            cerebro.stop()
            cerebro.loop.stop()
            return


def main():
    cerebro(obj={})


if __name__ == "__main__":
    main()
