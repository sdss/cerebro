#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-03
# @Filename: source.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import abc
import asyncio
from contextlib import suppress

from typing import Any, Dict, List, NamedTuple, Optional, Type

import rx
from rx.disposable import Disposable
from rx.subject import Subject

from cerebro import log


__all__ = [
    "DataPoints",
    "wrap_async_observable",
    "Source",
    "get_source_subclass",
    "TCPSource",
]


class DataPoints(NamedTuple):
    bucket: str | None
    data: List[Dict[str, Any]]


def wrap_async_observable(observable, *args, **kwargs):
    """Wraps a coroutine and creates an observable."""

    if not asyncio.iscoroutinefunction(observable):
        raise TypeError("Observable must be a coroutine function.")

    def on_subscribe(observer, scheduler):
        task = asyncio.create_task(observable(observer, scheduler, *args, **kwargs))
        return Disposable(task.cancel)  # type: ignore

    return rx.create(on_subscribe)


class Source(Subject):
    """A base class for a data source.

    Data sources are standalone systems that periodically produces measurements
    for one or multiple fields. And example is a TCP server that outputs
    temperature and humidity values as plain ASCII. The data source handles the
    connection and parsing of the stream, and reports new measurements to
    `.Cerebro`.

    Internally a source is an :py:class:`~rx.subject.Subject`, which acts as
    both an observer and observable. When the source calls ``on_next(value)``
    the value is received by `.Cerebro`, which handles the write to the
    database.

    Parameters
    ----------
    name
        The name of the data source.
    bucket
        The bucket to write to. If not set it will use the default bucket.
    tags
        A dictionary of tags to be associated with all measurements.

    """

    #: str: The type of data source.
    source_type: str | None = None

    #: float: Seconds to wait for initialisation.
    timeout: float | None = None

    def __init__(
        self,
        name: str,
        bucket: Optional[str] = None,
        tags: Dict[str, Any] = {},
    ):

        if self.source_type is None:
            raise ValueError("Subclasses must override source_type.")

        super().__init__()

        self.name = name
        self.bucket = bucket

        self.tags = tags.copy()
        self.tags.update({"source": self.source_type})

        self.running = False

    async def start(self):
        """Initialises the source.

        This method is called by `.Cerebro` when the data source is added. It
        can be empty but more frequently it contains the code to initialise the
        connection to a TCP server or other data stream. It can be a coroutine,
        in which case `.Cerebro` will schedule it as a task.

        """

        self.running = True

    def stop(self):
        """Stops the data source parsing and closes any open connection."""

        self.running = False

    async def restart(self):
        """Restarts the source."""

        log.debug(f"{self.name}: restarting source.")

        if self.running is True:
            if asyncio.iscoroutinefunction(self.stop):
                await self.stop()  # type: ignore
            else:
                self.stop()

        await self.start()


class TCPSource(Source, metaclass=abc.ABCMeta):
    """A source for a TCP server with robust reconnection and error handling.

    Parameters
    ----------
    name
        The name of the data source.
    host
        The host to which to connect.
    port
        The port on which the TCP server runs.
    delay
        How long to wait between queries to the TCP server. If `None`, uses the class
        delay.
    kwargs
        Other parameters to pass to `.Source`.

    """

    delay: float = 1

    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        delay: Optional[float] = None,
        **kwargs,
    ):

        super().__init__(name, **kwargs)

        self.host = host
        self.port = port

        self.delay = delay or self.delay

        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

        self._runner: asyncio.Task | None = None

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

    @abc.abstractmethod
    async def _read_internal(self) -> list[dict] | None:
        """Queries the TCP server and returns a list of points."""

        pass

    async def read(self, delay=None):
        """Queries the TCP server, emits data points, and handles disconnections."""

        delay = delay or self.delay

        while True:

            # Connect to server
            try:
                if self.writer and not self.writer.is_closing():
                    self.writer.close()
                    await self.writer.wait_closed()

                self.reader, self.writer = await asyncio.open_connection(
                    self.host,
                    self.port,
                )

            except (
                OSError,
                ConnectionError,
                ConnectionResetError,
                ConnectionRefusedError,
            ) as err:
                log.warning(f"{self.name}: {err}. Reconnecting in {delay} seconds.")
                await asyncio.sleep(delay)
                continue

            except BaseException as err:
                log.warning(f"{self.name}: Stopping after unknown error {err}.")
                await self.stop()
                return

            # Communicate with server
            try:
                points = await self._read_internal()
                if not self.reader.at_eof() and points is not None:
                    self.on_next(DataPoints(data=points, bucket=self.bucket))

            except asyncio.TimeoutError:
                log.warning(f"{self.name}: timed out waiting for the server to reply.")

            except Exception as err:
                log.warning(f"{self.name}: {str(err)}")

            finally:
                self.writer.close()
                await asyncio.sleep(delay)


def get_source_subclass(type_: str) -> Type[Source] | None:
    """Returns a `.Source` subclass based on its ``data_type``."""

    def all_subclasses(cls):
        return set(cls.__subclasses__()).union(
            [s for c in cls.__subclasses__() for s in all_subclasses(c)]
        )

    for subclass in all_subclasses(Source):
        if subclass.source_type == type_:
            return subclass

    return None
