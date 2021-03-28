#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2021-03-27
# @Filename: lvm.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import warnings
from contextlib import suppress
from datetime import datetime

from .source import DataPoints, Source


__all__ = ["GoveeSource"]


class GoveeSource(Source):

    source_type = "govee"

    def __init__(
        self,
        name: str,
        host: str = "localhost",
        port: int = 1111,
        devices: dict[str, str] = {},
        **kwargs,
    ):

        super().__init__(name, **kwargs)

        self.host = host
        self.port = port

        self._inverse_devices = {
            address.upper(): name for name, address in devices.items()
        }

        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

        self._task: asyncio.Task | None = None

    async def start(self):
        """Connects to the Govee server."""

        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self._task = asyncio.create_task(self.get_measurement())

    async def stop(self):
        """Disconnects from server."""

        if not self.writer.is_closing():
            self.writer.close()

        with suppress(asyncio.CancelledError):
            if self._task:
                self._task.cancel()
                await self._task
            self._task = None

    async def get_measurement(self):
        """Queries the Govee server and reports a new measurement."""

        while True:

            self.writer.write(b"status\n")

            try:
                data = await asyncio.wait_for(self.reader.readline(), timeout=5)

                address, temp, hum, _, isot = data.decode().strip().split()

                date = datetime.fromisoformat(isot)
                temp = float(temp)
                hum = float(hum)

                bucket = self.bucket or "sensors"

                tags = self.tags.copy()
                tags["address"] = address.upper()

                if address.upper() in self._inverse_devices:
                    tags["device"] = self._inverse_devices[address.upper()]
                else:
                    tags["device"] = address.upper()

                temp_point = {
                    "measurement": "temperature",
                    "fields": {"value": temp},
                    "tags": tags,
                    "time": date,
                }

                humid_point = {
                    "measurement": "humidity",
                    "fields": {"value": hum},
                    "tags": tags,
                    "time": date,
                }

                points = DataPoints(data=[temp_point, humid_point], bucket=bucket)

                self.on_next(points)

            except Exception as err:

                warnings.warn(f"Problem found in {self.name}: {err}", UserWarning)

            await asyncio.sleep(10)
