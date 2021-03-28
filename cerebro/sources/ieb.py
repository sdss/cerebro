#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-08
# @Filename: ieb.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio

from typing import Optional

from drift import Drift, Relay

from .. import log
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
        :py:method:`~drift.drift.Drift.from_config`.
    devices
        A list of devices to monitor, with the format ``<module>.<device>``.
    default_delay
        The default delay between measurements for a device, in seconds.
    delays
        A dictionary of ``{key: delay}`` where ``key`` is either the device
        name as ``<module>.<device>`` or a module name. In the latter case the
        delay applies to all the devices in that module. If the device or
        module is not specified, the ``default_delay`` will be used.
    kwargs
        Other arguments to pass to `.Source`.

    """

    source_type = "ieb"

    def __init__(
        self,
        name: str,
        ieb: Optional[Drift] = None,
        config: Optional[str] = None,
        devices: Optional[list[str]] = None,
        default_delay: float = 5.0,
        delays: dict[str, float] = {},
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

        if devices is None:
            devices = [
                f"{module}.{device}"
                for module in self.ieb.modules
                for device in self.ieb[module].devices
            ]

        self.devices = devices

        self.delays = delays
        self.defaul_delay = default_delay

        self._tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()

    async def start(self):
        """Starts and tests the connection to the IEB and begins monitoring."""

        # Test connection
        async with self.ieb:
            pass

        for device in self.devices:
            self._schedule_task(device)

    def stop(self):
        """Stops monitoring tasks."""

        for task in self._tasks:
            task.cancel()
        self._tasks = []

    def _schedule_task(self, device):
        """Schedule the monitor task for a given device."""

        task = asyncio.create_task(self.measure_device(device))
        task.add_done_callback(self._tasks.remove)
        self._tasks.append(task)

    async def measure_device(self, device):
        """Reads a device and passes the data to `.Cerebro`."""

        async with self._lock:
            try:
                dev = self.ieb.get_device(device)
                value, units = await dev.read(device)
                read = True
                if isinstance(dev, Relay):
                    value = True if value == "closed" else False
            except Exception as ee:
                log.error(f"{self.name} failed reading device {device}: {ee}.")
                value = units = None
                read = False

        if read is True:

            tags = self.tags.copy()
            if units:
                tags.update({"units": units})

            data_points = DataPoints(
                data=[
                    {
                        "measurement": self.name,
                        "tags": self.tags,
                        "fields": {device: value},
                    }
                ],
                bucket=self.bucket,
            )

            self.on_next(data_points)

        module_name, device_name = device.split(".")
        if isinstance(self.delays, dict):
            if device in self.delays:
                delay = self.delays[device]
            elif device_name in self.delays:
                delay = self.delays[device_name]
            elif module_name in self.delays:
                delay = self.delays[module_name]
            else:
                delay = self.defaul_delay
        else:
            delay = self.defaul_delay

        await asyncio.sleep(delay)

        self._schedule_task(device)
