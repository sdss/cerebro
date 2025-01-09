#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2021-03-27
# @Filename: lvm.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import copy
import os
import re
from contextlib import suppress
from datetime import datetime

from typing import Any, Dict, Optional

import asyncudp

from drift import Drift
from drift.convert import data_to_float32
from sdsstools import cancel_task, read_yaml_file

from cerebro import log

from .drift import DriftSource
from .source import DataPoints, Source, TCPSource


__all__ = ["GoveeSource", "Sens4Source", "LVMIEBSource", "ThermistorsSource"]


class GoveeSource(TCPSource):
    """Retrieves temperature and RH from a TCP server connected to a Govee BT device."""

    source_type = "govee"
    delay = 10

    def __init__(
        self,
        *args,
        address: str,
        device: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        assert device or address, "One of device or address are needed."

        self.device = device
        self.address = address.upper()

        self.bucket = self.bucket or "sensors"

    async def _read_internal(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> list[dict] | None:
        """Queries the TCP server and returns a list of points."""

        writer.write((f"status {self.address}\n").encode())
        await writer.drain()

        data = await asyncio.wait_for(reader.readline(), timeout=5)

        # Not found
        if data == b"?\n":
            return

        address, temp, hum, _, isot = data.decode().strip().split()

        date = datetime.fromisoformat(isot)
        temp = float(temp)
        hum = float(hum)

        # Check the timestamp. If the data point is too old, skip.
        if (datetime.utcnow() - date).seconds > 2 * self.delay:
            return None

        tags = self.tags.copy()
        tags["address"] = address.upper()
        tags["device"] = self.device

        if address != self.address:
            log.warning(
                f"{self.name}: mismatch between expected address "
                f"{self.address} and received {address}."
            )
            return None

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

        return [temp_point, humid_point]


class Sens4Source(TCPSource):
    """Reads pressure and temperature from multiple Sens4 transducers.

    The Sens4 provides this information over a serial interface but we assume there is
    a bidirectional byte stream between the serial device and a TCP socket.

    """

    source_type = "sens4"

    def __init__(
        self,
        *args,
        device_id: int,
        ccd: str = "NA",
        delay: float = 1,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.device_id = device_id
        self.ccd = ccd

        self.delay = delay

        self.bucket = self.bucket or "sensors"

    async def _read_internal(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> list[dict] | None:
        """Queries the TCP server and returns a list of points."""

        writer.write((f"@{self.device_id:d}Q?\\").encode())
        await writer.drain()

        data = await asyncio.wait_for(reader.readuntil(b"\\"), timeout=5)
        data = data.decode()

        m = re.match(
            r"^@[0-9]{1,3}ACKQ?"
            r"([0-9]+?.[0-9]+E[+-][0-9]+),"
            r"([0-9]+?.[0-9]+E[+-][0-9]+),"
            r"([0-9]+?.[0-9]+E[+-][0-9]+),"
            r"([0-9]+\.[0-9]+),.+\\$",
            data,
        )
        if not m:
            raise ValueError("Reply from device cannot be parsed.")

        pz, pir, cmb, temp = map(float, m.groups())

        tags = self.tags.copy()
        tags["ccd"] = self.ccd

        point = {
            "measurement": "pressure",
            "fields": {"pz": pz, "pir": pir, "cmb": cmb, "temp": temp},
            "tags": tags,
        }

        return [point]


class LVMIEBSource(DriftSource):
    """A source for the LVM IEB boxes that parses the Archon configuration file."""

    source_type = "lvm_ieb"

    def __init__(self, name: str, controller: str, config: str, **kwargs):
        config_dict = copy.deepcopy(read_yaml_file(config))

        config_data = {"modules": config_dict["wago_modules"]}
        config_data.update(config_dict["specs"][controller]["wago"])

        drift = Drift.from_config(config_data)

        tags = kwargs.pop("tags", {})
        tags.update({"controller": controller})

        super().__init__(name, drift_instance=drift, tags=tags, **kwargs)


class LN2Scale(TCPSource):
    """Check the measurement of the LN2 scale."""

    source_type = "ln2_scale"

    def __init__(
        self,
        *args,
        delay: float = 1,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.delay = delay
        self.bucket = self.bucket or "sensors"

    async def _read_internal(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> list[dict] | None:
        """Queries the TCP server and returns a list of points."""

        writer.write(("~*P*~\n").encode())
        await writer.drain()

        data = await asyncio.wait_for(reader.readline(), timeout=5)
        data = data.decode()

        m = re.search(r"\s([\-0-9.]+)\slb", data)
        if not m:
            raise ValueError("Reply from device cannot be parsed.")

        weigth = float(m.group(1))

        tags = self.tags.copy()
        tags["spectrograph"] = "sp1"

        point = {
            "measurement": "ln2_weigth",
            "fields": {"value": weigth},
            "tags": tags,
        }

        return [point]


class CheckFileExistsSource(Source):
    """Checks if a file exists."""

    source_type = "check_file_exists"
    delay = 60

    def __init__(
        self,
        name: str,
        file: str,
        bucket: str = "sensors",
        tags: Dict[str, Any] = {},
        delay: Optional[float] = None,
    ):
        super().__init__(name, bucket=bucket, tags=tags)

        self.file = file
        self.delay = delay or self.delay

        self._runner: asyncio.Task | None = None

    async def start(self):
        """Connects to the socket."""

        self._runner = asyncio.create_task(self.check_file())

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

    async def check_file(self):
        """Checks if the file exist."""

        while True:
            file_exists = os.path.exists(self.file)

            tags = self.tags.copy()
            tags["full_path"] = self.file

            self.on_next(
                DataPoints(
                    data=[
                        {
                            "measurement": "file_exists",
                            "fields": {os.path.basename(self.file): int(file_exists)},
                            "tags": tags,
                        }
                    ],
                    bucket=self.bucket,
                )
            )

            await asyncio.sleep(self.delay)


class ThermistorsSource(Source):
    """Reads the spectrograph thermistors.

    Reads the ADAM 6251-B module and outputs the status of each thermistor.

    Parameters
    ----------
    name
        The name of the data source.
    host
        The ADAM module IP.
    port
        The UDP port that serves the ASCII service.
    mapping
        A mapping of ``channelN`` to a channel name that will be stored as
        as tag.
    bucket
        The bucket to write to. If not set it will use the default bucket.
    tags
        A dictionary of tags to be associated with all measurements.
    interval
        How often to read the thermistors.

    """

    source_type = "lvm_thermistors"
    interval: float = 1

    def __init__(
        self,
        name: str,
        host: str,
        port: int = 1025,
        mapping: dict[str, str] = {},
        bucket: Optional[str] = None,
        tags: dict[str, Any] = {},
        interval: float | None = None,
    ):
        super().__init__(name, bucket, tags)

        self.interval = interval or self.interval

        self.host = host
        self.port = port
        self.mapping = mapping

        self._runner: asyncio.Task | None = None

    async def start(self):
        """Starts the runner."""

        self._runner = asyncio.create_task(self._run_tasks())

        await super().start()

    async def stop(self):
        """Stops the runner."""

        if self._runner and not self._runner.done():
            with suppress(asyncio.CancelledError):
                self._runner.cancel()
                await self._runner

        self.running = False

    async def _run_tasks(self):
        """Connects to the ASCII service and reads the thermistors."""

        while True:
            try:
                socket = await asyncio.wait_for(
                    asyncudp.create_socket(remote_addr=(self.host, self.port)),
                    timeout=10,
                )

                socket.sendto(b"$016\r\n")
                data, _ = await asyncio.wait_for(socket.recvfrom(), timeout=10)

                match = re.match(rb"!01([0-9A-F]+)\r", data)
                if match is None:
                    raise ValueError(
                        f"Invalid response from thermistor server at {self.host!r}."
                    )

                value = int(match.group(1), 16)

                channels: dict[int, int] = {}
                for channel in range(16):
                    channels[channel] = int((value & 1 << channel) > 0)

                for channel, value in channels.items():
                    channel_name = self.mapping.get(f"channel{channel}", "")
                    tags = self.tags.copy()
                    tags["channel_name"] = channel_name

                    self.on_next(
                        DataPoints(
                            data=[
                                {
                                    "measurement": "thermistors",
                                    "fields": {f"channel{channel}": value},
                                    "tags": tags,
                                }
                            ],
                            bucket=self.bucket,
                        )
                    )

            except asyncio.TimeoutError:
                log.error(f"Timed out connect or reading thermistors at {self.host!r}.")

            except Exception as err:
                log.error(err)

            await asyncio.sleep(self.interval)


class LVMIonPumpSource(Source):
    """A data source for a ModIcon ion pump controlled by a LabJack I/O device.

    Parameters
    ----------
    name
        The name of the data source.
    host
        The LabJack IP.
    port
        The port to the Modbus server (usually 502).
    addresses
        A mapping of LabJack addresses to read and the measurement and field
        where to store them. An example of a valid ``addresses`` entry is
        ``{'z2_pressure': {'address': 0, 'n_registers': 2 'type': 'float32',
        'cast': 'float', 'measurement': 'pressure', 'field': 'ion', 'ccd': 'z2'}}``
        where ``cast`` can be ``int``, ``float``, or ``bool``, in which case the
        value will be converted to 0 or 1. ``ccd`` will be set a tag. If the
        measurement is ``pressure``, the appropriate conversion from voltage to
        Torr will be applied.
    controller
        The CCD controller (spectrograph) to which this source is associated.
    bucket
        The bucket to write to. If not set it will use the default bucket.
    tags
        A dictionary of tags to be associated with all measurements.
    interval
        How often to read the thermistors.

    Examples
    --------
    An example of a valid configuration for this source is ::

        lvm_ion_pump_sp2:
            type: lvm_ion_pump
            bucket: spec
            interval: 5
            host: 10.8.38.155
            port: 502
            controller: sp2
            addresses:
            z2_pressure:
                address: 0
                n_registers: 2
                type: float32
                cast: float
                measurement: pressure
                field: ion
                ccd: z2
            z2_power:
                address: 2020
                n_registers: 1
                type: uint16
                cast: bool
                measurement: power
                field: ion
                ccd: z2

    """

    source_type: str = "lvm_ion_pump"
    interval: float = 5

    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        addresses: dict,
        controller: str | None = None,
        bucket: str | None = None,
        tags: dict = {},
        interval: float | None = None,
    ):
        super().__init__(name, bucket, tags)

        if controller is not None:
            self.tags["controller"] = controller

        self.drift = Drift(host, port)
        self.addresses = addresses

        self.interval = interval or self.interval

        self._runner: asyncio.Task | None = None

    async def start(self):
        """Starts the runner."""

        self._runner = asyncio.create_task(self._read_device())

        await super().start()

    async def stop(self):
        """Stops the runner."""

        await cancel_task(self._runner)
        self._runner = None

        self.running = False

    async def _read_device(self):
        """Reads the device and emits the data points."""

        while True:
            data: list = []
            try:
                async with self.drift:
                    for config in self.addresses.values():
                        try:
                            value = await self.drift.client.read_holding_registers(
                                config["address"],
                                count=config["n_registers"],
                            )

                            if config["type"] == "float32":
                                registers: Any = tuple(value.registers)
                                value = data_to_float32(registers)
                            elif config["type"] == "uint16":
                                value = value.registers[0]
                            else:
                                raise ValueError(f"Invalid type {config['type']}.")

                            if "cast" in config:
                                value = eval(f"{config['cast']}({value})")
                            if isinstance(value, bool):
                                value = int(value)

                            if config["measurement"] == "pressure":
                                value = self.convert_pressure(value)

                            tags = self.tags.copy()
                            tags["ccd"] = config["ccd"]

                            data.append(
                                {
                                    "measurement": config["measurement"],
                                    "fields": {config["field"]: value},
                                    "tags": tags,
                                }
                            )
                        except Exception as err:
                            addr = config["address"]
                            log.warning(f"{self.name}: error in address {addr}: {err}")

            except Exception as err:
                log.error(f"{self.name}: unexpected error: {err}")

            self.on_next(DataPoints(data=data, bucket=self.bucket))

            await asyncio.sleep(self.interval)

    def convert_pressure(self, volts: float):
        """Converts the voltage to pressure in Torr."""

        # The calibration is a linear fit of the form y = mx + b
        m = 2.04545
        b = -6.86373

        log10_pp0 = m * volts + b  # log10(PPa), pressure in Pascal

        torr = 10**log10_pp0 * 0.00750062

        return torr
