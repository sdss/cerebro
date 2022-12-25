#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2022-10-31
# @Filename: tpm.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
from contextlib import suppress

from cerebro import log

from .source import DataPoints, Source


try:
    import tpmdata  # type: ignore
except ImportError:
    tpmdata = None


__all__ = ["TPMSource"]


class TPMSource(Source):
    """Reads TPM data.

    Parameters
    ----------
    name
        The name of the data source.
    kwargs
        Other parameters to pass to `.Source`.

    """

    source_type = "tpm"

    def __init__(self, name: str, **kwargs):
        if tpmdata is None:
            raise RuntimeError("tpmdata cannot be imported.")

        tpmdata.tinit()

        super().__init__(name, **kwargs)

        self._running: asyncio.Task | None = None

    async def start(self):
        """Connects to the socket."""

        self._runner = asyncio.create_task(self.read())

        await super().start()

    async def stop(self):
        """Disconnects from socket."""

        log.debug(f"{self.name}: stopping connection.")

        if not self.running:
            raise RuntimeError(f"{self.name}: source is not running.")

        with suppress(asyncio.CancelledError):
            if self._runner:
                self._runner.cancel()
                await self._runner
            self._runner = None

        super().stop()

    async def read(self):
        """Reads a packet from the TPM."""

        assert tpmdata is not None

        while True:
            loop = asyncio.get_running_loop()
            dd = await loop.run_in_executor(None, tpmdata.packet, 0, 0)

            if len(dd) > 0:
                tags = self.tags.copy()
                data_points = DataPoints(
                    data=[{"measurement": "tpm", "fields": dd, "tags": tags}],
                    bucket=self.bucket,
                )

                self.on_next(data_points)

            await asyncio.sleep(1)
