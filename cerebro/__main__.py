#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-03
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import asyncio
import json
import os
import pathlib
import sys

import click
from click_default_group import DefaultGroup

from sdsstools._vendor.color_print import color_text
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
@click.option("--profile", "-p", type=str, help="The profile to use.")
@click.pass_context
def cerebro(ctx, sources, config, profile):
    """Command Line Interface for cerebro."""

    if sources and profile:
        raise click.UsageError("--sources and --profile are mutually exclusive.")

    if not config:
        config = pathlib.Path(__file__).parent / "etc" / "cerebro.yaml"
    else:
        config = os.path.realpath(config)

    sources = sources.split(",") if sources else []

    ctx.obj = {"sources": sources, "config": config, "profile": profile}


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

    cerebro = Cerebro(**ctx.obj)
    await cerebro.start()

    # Hacky way to run forever. nest_asyncio has some issues with CLU.
    while True:
        try:
            await asyncio.sleep(60)
        except KeyboardInterrupt:
            cerebro.stop()
            cerebro.loop.stop()
            return


@cerebro.command()
@cli_coro()
async def status():
    """Get the status of the sources."""

    r, w = await asyncio.open_unix_connection("/tmp/cerebro.sock")

    w.write(b"status\n")
    await w.drain()

    reply = await r.readline()
    data = json.loads(reply.decode())

    for source in data:
        value = data[source]
        value_str = color_text("OK" if value else "NO", "green" if value else "red")
        print(f"{source}: {value_str}")

    w.close()
    await w.wait_closed()


@cerebro.command()
@click.argument("SOURCE", type=str)
@cli_coro()
async def restart(source):
    """Restarts a source."""

    print("Restarting ... ", end="", flush=True)

    r, w = await asyncio.open_unix_connection("/tmp/cerebro.sock")

    w.write(b"restart " + source.encode() + b"\n")
    await w.drain()

    reply = await r.readline()
    if "true" in reply.decode():
        print(color_text("SUCCESS", "green"))
    else:
        print(color_text("FAILED", "red"))

    w.close()
    await w.wait_closed()


def main():
    cerebro(obj={})


if __name__ == "__main__":
    main()
