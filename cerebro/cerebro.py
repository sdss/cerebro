#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-03
# @Filename: cerebro.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import abc
import asyncio
import datetime
import json
import os
import pathlib
import socket
import time
import warnings

from typing import Any, Dict, List, Optional

import ntplib
from rx.scheduler.eventloop.asyncioscheduler import AsyncIOScheduler
from rx.subject.subject import Subject

from sdsstools import read_yaml_file

from . import log
from .observer import Observer, get_observer_subclass
from .sources import Source, get_source_subclass


class SourceList(list):
    """A list of `.Source` instances.

    Provides a thin wrapper around a `list` class, with methods to add and
    start a data source or stop it.

    """

    def __init__(self, loop: asyncio.AbstractEventLoop, on_next, sources=[]):
        super().__init__()

        self.on_next = on_next

        self.scheduler = AsyncIOScheduler(loop)

        self._name_to_source = {}
        for source in sources:
            self.add_source(source)

    def stop(self):
        for source in self:
            if asyncio.iscoroutinefunction(source.stop):
                close_task = asyncio.create_task(source.stop())
                while not close_task.done():
                    pass
            else:
                source.stop()

    def __del__(self):
        self.stop()

    def __add__(self, x):
        raise NotImplementedError("Addition is not implemented.")

    def insert(self, *args):
        raise NotImplementedError("Insert is not implemented.")

    def remove(self, source):
        source.dispose()
        return super().remove(source)

    def append(self, source):
        return self.add_source(source)

    def pop(self, index):
        return self.remove_source(self[index].name)

    def get(self, name):
        """Retrieves a data source by name."""

        if name not in self._name_to_source:
            raise ValueError(f"Data source {name!r} not found.")

        return self._name_to_source[name]

    def add_source(self, source: Source):
        """Adds a `.Source`."""

        if not isinstance(source, Source):
            raise NotImplementedError("Only instances of Source can be passed.")

        source.subscribe(on_next=self.on_next, scheduler=self.scheduler)

        assert asyncio.iscoroutinefunction(source.start)

        # We add the source even if it's not running.
        self._name_to_source[source.name] = source
        super().append(source)

        timeout = getattr(source, "timeout", None)
        asyncio.create_task(self._start_source(source, timeout=timeout))

    async def _start_source(self, source, timeout=None):
        """Starts the source."""

        try:
            await asyncio.wait_for(source.start(), timeout=timeout)
        except asyncio.TimeoutError:
            log.error(f"Timed out trying to start source {source.name}.")
            source.running = False
        except BaseException as exception:
            log.error(f"Failed starting source {source.name}: {exception!s}")
            source.running = False
        else:
            log.debug(f"Started source {source.name}.")

    def remove_source(self, source_name):
        """Removes a source."""

        source = self._name_to_source[source_name]
        self.remove(source)
        self._name_to_source.pop(source_name)


class Cerebellum(type):
    """Metaclass for Cerebro."""

    def __call__(cls, *args, **kwargs):
        args, kwargs = cls.__parse_config__(*args, **kwargs)
        obj = Cerebro.__new__(cls)
        obj.__init__(*args, **kwargs)
        return obj

    @staticmethod
    def __parse_config__(*args, **kwargs):
        """Overrides initialisation parameters from a configuration file."""

        if kwargs.get("config", None) is None:
            return args, kwargs

        if kwargs.get("sources", {}) and kwargs.get("profile", None):
            raise ValueError("profile and sources are incompatible.")

        # Remove input sources and observers.
        sources_kw = kwargs.pop("sources", {})
        kwargs.pop("observers", None)

        profile_name = kwargs.get("profile", None)

        config_file = kwargs.pop("config")
        if isinstance(config_file, (str, pathlib.Path)):
            config = read_yaml_file(config_file)
        elif isinstance(config_file, dict):
            config = config_file.copy()
        else:
            raise ValueError(f"Invalid type {type(config_file)} for config.")
        config.update(kwargs)

        profiles_data = config.pop("profiles", {})

        sources = []
        observers = []

        if profile_name:
            assert profile_name in profiles_data

            profile = profiles_data[profile_name]

            global_sources = config.get("sources", {})
            global_observers = config.get("observers", {})

            for source in profile["sources"]:
                if isinstance(source, str):
                    sources.append(
                        Cerebellum.__get_source(source, global_sources[source])
                    )
                elif isinstance(source, dict):
                    for source_name in source:
                        sources.append(
                            Cerebellum.__get_source(
                                source_name,
                                source[source_name],
                            )
                        )
                else:
                    raise TypeError("Profile sources must be strings or dicts.")

            for observer in profile["observers"]:
                if isinstance(observer, str):
                    observers.append(
                        Cerebellum.__get_observer(observer, global_observers[observer])
                    )
                elif isinstance(observer, dict):
                    for observer_name in observer:
                        observers.append(
                            Cerebellum.__get_observer(
                                observer_name,
                                observer[observer_name],
                            )
                        )
                else:
                    raise TypeError("Profile observers must be strings or dicts.")

        else:
            for source_name, params in config.pop("sources", {}).items():
                if len(sources_kw) != 0 and source_name not in sources_kw:
                    continue
                sources.append(Cerebellum.__get_source(source_name, params))

            for observer_name, params in config.pop("observers", {}).items():
                observers.append(Cerebellum.__get_observer(observer_name, params))

        config["sources"] = sources
        config["observers"] = observers

        return args, config

    @staticmethod
    def __get_source(source_name, params):
        type_ = params.pop("type")
        Subclass = get_source_subclass(type_)
        if Subclass is None:
            raise ValueError(f"Source type {type_} is not valid.")
        return Subclass(source_name, **params)

    @staticmethod
    def __get_observer(source_name, params):
        type_ = params.pop("type")
        Subclass = get_observer_subclass(type_)
        if Subclass is None:
            raise ValueError(f"Observer type {type_} is not valid.")
        return Subclass(source_name, **params)


# Combine ABCMeta and Cerebellum so that can add it as metaclass to Cerebro
# without getting the "the metaclass of a derived class must be a (non-strict)
# subclass of the metaclasses of all its bases" error.
class MetaCerebro(abc.ABCMeta, Cerebellum):
    pass


class Cerebro(Subject, metaclass=MetaCerebro):
    """Handles a list of data sources to which observers can subscribe.

    Creates an `RX <https://rxpy.readthedocs.io/en/latest/>`__ subject that
    monitors a list of data sources. `.Observer` instances can
    subscribe to the combined data stream, which is served as a uniformly
    formatted dictionary.

    Parameters
    ----------
    name
        The name of this Cerebro instance. This value is added as the
        ``cerebro`` tag to all measurements processed. The name can be used
        to identify the origin of the data when multiple instances of Cerebro
        are loading data to the database.
    tags
        A list of tags to add to all the measurements.
    sources
        A list of `.Source` instances to listen to.
    config
        A file or dictionary from which to load the configuration. The format
        exactly follows the signature of `.Cerebro` and the data sources it
        refers to. For example:

        .. code-block:: yaml

            logfile: /data/logs/cerebro/cerebro.log
            ntp_server: us.pool.ntp.org
            tags:
              observatory: ${OBSERVATORY}
            profiles:
              default:
                sources:
                  - tron
                observers:
                  - influxdb
            sources:
              tron:
                type: tron
                bucket: Actors
                host: localhost
                port: 6093
                actors:
                  - tcc
                  - apo
            observers:
              influxdb:
                type: influxdb
                url: http://localhost:9999
                token: null
                org: SDSS
                default_bucket: FPS

        Each source and observer must include a ``type`` key that correspond
        to the ``source_type`` and ``observer_type`` value of the `.Source`
        and `.Writer` subclass to be used, respectively. The profile section, if
        present, is a dictionary of profiles, each one defining a list of sources and
        observers. If the items in the sources or observers list are strings, the
        entries from the global ``sources`` and ``observers`` sections will be used.
        The profile sources and observers items can also be dictionaries with the same
        format as the global sources and observers. If ``config`` is defined, the
        keywords ``sources`` and ``observers`` are ignored.
    profile
        The name of the profile to use from the configuration file. If `None` and
        ``config`` is defined, uses all the global sources and observers.
    ntp_server
        The route to the NTP server to use. The server is queried every hour
        to determine the offset between the NTP pool and the local computer.
    logfile
        If set, the path where to write the file log. Otherwise logs only to
        stdout/stderr.
    log_rotate
        Whether to rotate the log file at midnight UTC.

    """

    def __init__(
        self,
        name: str = "cerebro",
        tags: Dict[str, Any] = {},
        sources: List[Source | str] = [],
        observers: List[Observer] = [],
        config: Optional[str | dict | pathlib.Path] = None,
        profile: Optional[str] = None,
        ntp_server: str = "us.pool.ntp.org",
        logfile: Optional[str] = None,
        log_rotate: bool = True,
    ):
        Subject.__init__(self)

        self.name = name

        host = socket.getfqdn()

        if logfile:
            if os.path.isdir(logfile) and os.path.exists(logfile):
                logfile = os.path.join(logfile, f"{name}.log")
            log.start_file_logger(logfile, rotating=log_rotate)

        start_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

        log.debug(f"Starting Cerebro at {start_time} on host {host}.")

        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            # "There is no current event loop in thread %r"
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        self.sources = SourceList(self.loop, self.on_next, sources)

        for observer in observers:
            observer.set_cerebro(self)
            log.debug(f"Added observer of type {observer.observer_type}.")

        # Add the name of the instance and the host to the default tags.
        self.tags = tags.copy()

        self._offset = 0
        self.loop.call_soon(self.update_time_offset, ntp_server)

        self.status_server = None

    async def start(self):
        """Starts the status file socket."""

        self.status_server = await asyncio.start_unix_server(
            self.status_server_cb,
            path="/tmp/cerebro.sock",
        )

    def stop(self):
        """Stops the InfluxDB client and all the sources."""

        self.sources.stop()

        if self.status_server is not None:
            self.status_server.close()

    def on_next(self, data):
        """Processes a list of measurements from a data source.

        Measurements is expected to be a namedtuple or other kind of
        namespace with at least a ``.data`` attribute which must be a list
        of dictionaries. Each dictionary must contain an individual
        ``measurement`` along with a series of associated ``values``, and
        optionally a set of ``tags``. The default tags are added to each
        measurement. If a measurement does not contain a ``time`` value, it
        will be added with the current UTC time, taking into account the
        offset determined by the NTP server.

        Additional parameters can be added to the namespace for any observable
        to interpret, but are not required.

        Once updated, the measurements are propagated to all the observers.

        """

        if data.data == [] or data.data is None:
            return

        meas_time = int((time.time() + self._offset / 1e3) * 1e9)
        for point in data.data:
            if "time" not in point:
                # Time is in nanoseconds since UNIX epoch.
                point["time"] = meas_time
            point["tags"].update(self.tags)

        # Propagate to all the observers.
        Subject.on_next(self, data)

    def update_time_offset(self, server: str):
        """Updates the internal offset with the NTP server."""

        try:
            ntp = ntplib.NTPClient()
            offset = ntp.request(server, version=3).delay
            if offset:
                self._offset = offset
        except Exception as ee:
            warnings.warn(f"Failed updating offset from NTP server: {ee}", UserWarning)

        self.loop.call_later(3600.0, self.update_time_offset, server)

    async def status_server_cb(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        """Handles new connections to the status server."""

        while True:
            command = await reader.readline()
            command = command.decode().strip()
            if reader.at_eof():
                return None

            if command == "status":
                status = {source.name: source.running for source in self.sources}
                writer.write(json.dumps(status, indent=None).encode() + b"\n")
                await writer.drain()

            elif "restart" in command:
                source_name = command.split()[1]
                try:
                    source = self.sources.get(source_name)
                    await source.restart()
                    writer.write(b"true\n")
                except BaseException:
                    writer.write(b"false\n")

                await writer.drain()

            elif command == "exit":
                break
