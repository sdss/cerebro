#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-08
# @Filename: ieb.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
from contextlib import suppress

from typing import List, Optional

from drift import Device, Drift

from .. import log
from .source import DataPoints, Source


__all__ = ["DriftSource"]


class DriftSource(Source):
    """A data source that uses a Drift configuration file.

    Uses `drift <https://sdss-drift.readthedocs.io/en/latest/>`__ to connect
    to a Modbus ethernet module and read a series of devices.

    Parameters
    ----------
    name
        The name of the data source.
    drift_instance
        An instance of :py:class:`~drift.drift.Drift` with the connection to
        the Modbus controller to use.
    config
        The path to a configuration file that can be loaded using
        `~drift.drift.Drift.from_config`.
    devices
        A list of devices to monitor, with the format ``<module>.<device>``.
    delay
        The delay between measurements of the devices, in seconds.
    measure_timeout:
        Timeout while measuring.
    kwargs
        Other arguments to pass to `.Source`.

    """

    source_type = "drift"

    def __init__(
        self,
        name: str,
        drift_instance: Optional[Drift] = None,
        config: Optional[str] = None,
        devices: Optional[List[str]] = None,
        delay: float = 5.0,
        measure_timeout: float = 5.0,
        **kwargs,
    ):

        super().__init__(name, **kwargs)

        if drift_instance and config:
            raise ValueError("Only one of drift_instance or config can be defined.")

        if drift_instance:
            self.drift_instance = drift_instance
        elif config:
            self.drift_instance = Drift.from_config(config)
        else:
            raise ValueError("Either drift_instance or config are needed.")

        self.devices: List[Device]
        if devices is None:
            self.devices = [
                device
                for module in self.drift_instance.modules
                for device in self.drift_instance[module].devices.values()
            ]
        else:
            self.devices = [
                self.drift_instance.get_device(device) for device in devices
            ]

        self.delay = delay
        self.measure_timeout = measure_timeout

        self._task: asyncio.Task | None = None

    async def start(self):
        """Starts and tests the connection to the device(s) and begins monitoring."""

        self.running = True
        self._task = asyncio.create_task(self._measure())

    async def stop(self):
        """Stops monitoring tasks."""

        if self._task:
            with suppress(asyncio.CancelledError):
                self._task.cancel()
                await self._task

        self.running = False

    async def _measure(self):
        """Schedules measurements."""

        report_new_errors = True
        timeout = self.measure_timeout

        while True:
            try:
                await asyncio.wait_for(self.measure_devices(), timeout=timeout)
                report_new_errors = True
            except asyncio.TimeoutError:
                if report_new_errors:
                    log.error(f"{self.name}: timed out measuring devices.")
                report_new_errors = False
            except Exception as err:
                if report_new_errors:
                    log.error(f"{self.name}: unknown exception: {err}")
                report_new_errors = False

            await asyncio.sleep(self.delay)

    async def measure_devices(self):
        """Reads devices and passes the data to `.Cerebro`."""

        data = []

        async with self.drift_instance:
            for device in self.devices:
                category = device.category
                if category is None:
                    continue
                value, units = await device.read(adapt=True, connect=False)
                if device.__type__ == "relay":
                    value = True if value == "closed" else False
                    units = None
                tags = self.tags.copy()
                if units:
                    tags.update({"units": units})
                tags.update({"offset": device.offset})
                data.append(
                    {
                        "measurement": category,
                        "fields": {device.name: value},
                        "tags": tags,
                    }
                )

        data_points = DataPoints(data=data, bucket=self.bucket)

        self.on_next(data_points)
