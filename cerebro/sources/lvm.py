#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2021-03-27
# @Filename: lvm.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import re
import warnings
from contextlib import suppress
from datetime import datetime

from .source import DataPoints, Source


__all__ = ["GoveeSource", "Sens4Source"]


class GoveeSource(Source):
    """Retrieves temperature and RH from a TCP server connected to a Govee BT device."""

    source_type = "govee"
    timeout = 10

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
            await self.writer.wait_closed()

        with suppress(asyncio.CancelledError):
            if self._task:
                self._task.cancel()
                await self._task
            self._task = None

    async def restart(self):
        """Restarts the server."""

        await self.stop()
        await self.start()

    async def get_measurement(self):
        """Queries the Govee server and reports a new measurement."""

        while True:

            try:
                self.writer.write(b"status\n")
                await self.writer.drain()

                data = await asyncio.wait_for(self.reader.readline(), timeout=5)

                address, temp, hum, _, isot = data.decode().strip().split()

                date = datetime.fromisoformat(isot)
                temp = float(temp)
                hum = float(hum)

                # Check the timestamp. If the data point is too old, skip.
                if (datetime.utcnow() - date).seconds > 2 * self.timeout:
                    await asyncio.sleep(self.timeout)
                    continue

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

                warnings.warn(f"Problem found in {self.name}: {str(err)}", UserWarning)

                if err.__class__ == ConnectionError or self.reader.at_eof:
                    await self.restart()
                    return

            await asyncio.sleep(self.timeout)


class Sens4Source(Source):
    """Reads pressure and temperature from multiple Sens4 transducers.

    The Sens4 provides this information over a serial interface but we assume there is
    a bidirectional byte stream between the serial device and a TCP socket. All device
    servers must run on the same host, each on on a different port. The ``devices``
    parameters is a mapping of device name to a dictionary that must include the
    ``port`` and ``device_id`` of the device. For example ::

        devices = {'z1': {'port': 1112, 'device_id': 253}}

    """

    source_type = "sens4"
    timeout = 10

    def __init__(
        self,
        name: str,
        host: str = "localhost",
        devices: dict[str, dict] = {},
        **kwargs,
    ):

        super().__init__(name, **kwargs)

        self.host = host
        self.devices = devices
        self.connections = {}

        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

        self._task: asyncio.Task | None = None

    async def start(self):
        """Connects to the Sens4 socket."""

        for device_name in self.devices:
            reader, writer = await asyncio.open_connection(
                self.host,
                self.devices[device_name]["port"],
            )
            self.connections[device_name] = {"reader": reader, "writer": writer}

        self._task = asyncio.create_task(self.read())

    async def stop(self):
        """Disconnects from socket."""

        for connection in self.connections.values():
            if not connection["writer"].is_closing():
                connection["writer"].close()
                await connection["writer"].wait_closed()

            with suppress(asyncio.CancelledError):
                if self._task:
                    self._task.cancel()
                    await self._task
                self._task = None

    async def restart(self):
        """Restarts the server."""

        await self.stop()
        await self.start()

    async def read(self):
        """Queries the Sens4 devices and reports a new measurement."""

        while True:

            for name in self.connections:
                reader = self.connections[name]["reader"]
                writer = self.connections[name]["writer"]
                device_id = self.devices[name]["device_id"]

                try:
                    writer.write((f"@{device_id:d}Q?\\").encode())
                    await writer.drain()

                    data = await asyncio.wait_for(reader.readuntil(b"\\"), timeout=5)
                    data = data.decode()

                    m = re.match(
                        r"^@[0-9]{1,3}ACKQ"
                        r"([0-9]+?.[0-9]+E[+-][0-9]+),"
                        r"([0-9]+?.[0-9]+E[+-][0-9]+),"
                        r"([0-9]+?.[0-9]+E[+-][0-9]+),"
                        r"([0-9]+\.[0-9]+),.+\\$",
                        data,
                    )
                    if not m:
                        raise ValueError("Reply from device cannot be parsed.")

                    pz, pir, cmb, temp = map(float, m.groups())

                    bucket = self.bucket or "sensors"

                    tags = self.tags.copy()
                    tags["ccd"] = name

                    point = {
                        "measurement": "pressure",
                        "fields": {"pz": pz, "pir": pir, "cmb": cmb, "temp": temp},
                        "tags": tags,
                    }

                    self.on_next(DataPoints(data=[point], bucket=bucket))

                except Exception as err:

                    warnings.warn(
                        f"Problem found in {self.name} {name}: {str(err)}",
                        UserWarning,
                    )

                    if err.__class__ == ConnectionError or reader.at_eof:
                        await self.restart()
                        return

            await asyncio.sleep(self.timeout)
