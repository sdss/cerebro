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

from tpm_multicast_client import TPMClient

from cerebro import log

from .source import DataPoints, Source


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
        self.tpm_client = TPMClient()

        super().__init__(name, **kwargs)

        self._running: asyncio.Task | None = None
        self._listen_task: asyncio.Task | None = None

    async def start(self):
        """Connects to the socket."""

        self._listen_task = asyncio.create_task(self.tpm_client.listen())
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

            if self._listen_task:
                self._listen_task.cancel()
                await self._listen_task
            self._listen_task = None

        super().stop()

    async def read(self):
        """Reads a packet from the TPM."""

        while True:
            try:
                if self.tpm_client.data is not None:
                    data = self.tpm_client.data
                    if len(data) > 0:
                        tags = self.tags.copy()
                        data_points = DataPoints(
                            data=[{"measurement": "tpm", "fields": data, "tags": tags}],
                            bucket=self.bucket,
                        )

                        self.on_next(data_points)
            except Exception as e:
                log.error(f"{self.name}: error reading TPM data: {e}")

            await asyncio.sleep(1)
