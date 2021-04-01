#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-08
# @Filename: ieb.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import warnings
from contextlib import suppress

from typing import List, Optional

from drift import Device, Drift

from .source import DataPoints, Source


__all__ = ["IEBSource"]


class IEBSource(Source):
    """A data source that connects to an Instrument Electronics Box.

    Uses `drift <https://sdss-drift.readthedocs.io/en/latest/>`__ to connect
    to a Modbus ethernet module and read a series of devices.

    Parameters
    ----------
    name
        The name of the data source.
    ieb
        An instance of :py:class:`~drift.drift.Drift` with the connection to
        the IEB Modbus controller to use.
    config
        The path to a configuration file that can be loaded using
        `~drift.drift.Drift.from_config`.
    devices
        A list of devices to monitor, with the format ``<module>.<device>``.
    delay
        The delay between measurements of the devices, in seconds.
    kwargs
        Other arguments to pass to `.Source`.

    """

    source_type = "ieb"

    def __init__(
        self,
        name: str,
        ieb: Optional[Drift] = None,
        config: Optional[str] = None,
        devices: Optional[List[str]] = None,
        delay: float = 5.0,
        **kwargs,
    ):

        super().__init__(name, **kwargs)

        if ieb and config:
            raise ValueError("Only one of ieb or config can be defined.")

        if ieb:
            self.ieb = ieb
        elif config:
            self.ieb = Drift.from_config(config)
        else:
            raise ValueError("Either ieb or config are needed.")

        self.devices: List[Device]
        if devices is None:
            self.devices = [
                device
                for module in self.ieb.modules
                for device in self.ieb[module].devices
            ]
        else:
            self.devices = [self.ieb.get_device(device) for device in devices]

        self.delay = delay

        self._task: asyncio.Task | None = None

    async def start(self):
        """Starts and tests the connection to the IEB and begins monitoring."""

        # Test connection
        async with self.ieb:
            pass

        self._task = asyncio.create_task(self._measure())

    async def stop(self):
        """Stops monitoring tasks."""

        if self._task:
            with suppress(asyncio.CancelledError):
                self._task.cancel()
                await self._task

    async def _measure(self):
        """Schedules measurements."""

        while True:
            try:
                await asyncio.wait_for(self.measure_devices(), timeout=5)
            except asyncio.TimeoutError:
                warnings.warn("IEB: timed out measuring devices.")
            await asyncio.sleep(self.delay)

    async def measure_devices(self):
        """Reads devices and passes the data to `.Cerebro`."""

        data = []

        async with self.ieb:
            for device in self.devices:
                category = device.category
                if category is None:
                    continue
                value, units = await device.read(adapt=True, connect=False)
                tags = self.tags.copy()
                if units:
                    tags.update({"units": units})
                data.append(
                    {
                        "measurement": category,
                        "fields": {device.name: value},
                        "tags": tags,
                    }
                )

        data_points = DataPoints(data=data, bucket=self.bucket)

        self.on_next(data_points)
