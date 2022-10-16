#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-05
# @Filename: tron.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import warnings
from contextlib import suppress

from typing import Any, Dict, List, Optional

import numpy

from clu.legacy import TronConnection
from clu.legacy.types.keys import KeysDictionary
from clu.legacy.types.messages import Keyword, Reply
from clu.legacy.types.parser import ActorReplyParser, ParseError

from cerebro import log
from cerebro.protocols import ClientProtocol

from .source import DataPoints, Source


def process_keyword(
    keyword: Keyword,
    actor: str,
    tags: dict = {},
    keyword_tags: dict = {},
    casts: dict = {},
):
    """Creates a series of data points out of a keyword."""

    name = keyword.name
    keyword_tag_value = None

    points = []

    ii = 0
    for idx, key_value in enumerate(keyword.values):

        if hasattr(key_value, "name") and key_value.name:
            key_name = f"_{key_value.name}"
        elif len(keyword.values) == 1:
            key_name = ""
        else:
            key_name = f"_{ii}"

        value_tags = tags.copy()
        if hasattr(key_value, "units"):
            value_tags.update({"units": key_value.units})

        native = key_value.native
        if isinstance(native, (list, tuple, numpy.ndarray)):
            if key_value.__class__.__name__ == "PVT":
                fields = {
                    f"{name}{key_name}_P": native[0],
                    f"{name}{key_name}_V": native[1],
                    f"{name}{key_name}_T": native[2],
                }
            else:
                warnings.warn(
                    f"Cannot parse {actor}.{name!r} of type {type(native)!r}.",
                    UserWarning,
                )
                continue

        else:
            parsed = native

            if f"{actor}.{name}{key_name}" in casts:
                cast = casts[f"{actor}.{name}{key_name}"]
                if cast == "int":
                    parsed = int(native)
                elif cast == "float":
                    parsed = float(native)
                elif cast == "bool":
                    parsed = bool(native)

            fields = {f"{name}{key_name}": parsed}

            if f"{actor}.{name}" in keyword_tags:
                if idx == keyword_tags[f"{actor}.{name}"]["index"]:
                    keyword_tag_value = parsed

        points.append({"measurement": actor, "tags": value_tags, "fields": fields})

        ii += 1

    if keyword_tag_value is not None:
        keyword_tag_name = keyword_tags[f"{actor}.{name}"]["name"]
        for point in points:
            point["tags"].update({keyword_tag_name: keyword_tag_value})

    return points


class TronSource(Source):
    """A data source that monitors a Tron connection.

    Connects to Tron as a TCP client and parses actor keywords. Data values
    are sent to cerebro with the actor name as ``measurement`` and the keyword
    name as ``field_key``. If the key contains multiple values, the name of
    each value is added to the ``field_key`` as ``keyword_keyname``. If the
    value does not have a name, the zero-indexed index of its key is used.

    Internally it uses `CLU <https://github.com/sdss/clu>`__ to establish the
    connection to Tron and parse the keywords. It requires ``actorkeys`` to be
    importable.

    Parameters
    ----------
    name
        The name of the data source.
    bucket
        The bucket to write to. If not set it will use the default bucket.
    tags
        A dictionary of tags to be associated with all measurements.
    actors
        A list of actor names to monitor.
    host
        The host on which to connect to Tron.
    port
        The port on which Tron is running.
    keywords
        A list of keywords to monitor for a given actor. If `None`, all
        keywords are monitored and recorded.
    commands
        A dictionary of command string to be sent to the actor on an interval.
        The value of each key-value pair is the interval, in seconds.
    casts
        A dictionary of ``actor.keyword.key_name`` to cast. E.g.,
        ``{boss.exposure_time.exptime: "float"}``.
    keyword_tags
        A dictionary with a keyword value to be added to the data-points as a tag.
        This is useful for keywords in which the values are not independent and
        one wants to add an additional tag from the keyword itself. For example
        in ``fliswarm.status`` contains several values that all relate to a
        camera which is the first value in the keyword. To add the camera name
        as a tag we can pass ``{"fliswarm.status": {"index": 0, "name": "camera}}``.

    """

    source_type = "tron"
    timeout = 60

    def __init__(
        self,
        name: str,
        bucket: Optional[str] = None,
        tags: Dict[str, Any] = {},
        actors: List[str] = [],
        host: str = "localhost",
        port: int = 6093,
        keywords: Optional[List[str]] = None,
        commands: dict[str, float] = {},
        casts: dict[str, str] = {},
        keyword_tags: dict[str, dict] = {},
    ):

        super().__init__(name, bucket=bucket, tags=tags)

        self.tron = TronConnection(f"cerebro.{name}", host, port, models=actors)
        self.keywords = keywords

        self.commands = commands
        self._command_tasks: list[asyncio.Task] = []

        self.casts = casts
        self.keyword_tags = keyword_tags

        for model_name in self.tron.models:
            model = self.tron.models[model_name]
            model.register_callback(self.process_keyword)  # type: ignore

    async def start(self):
        """Starts the connection to Tron."""

        await self.tron.start(get_keys=False)

        for command in self.commands:
            self._command_tasks.append(
                asyncio.create_task(
                    self.schedule_command(
                        command,
                        self.commands[command],
                    )
                )
            )

        self.running = True

    async def schedule_command(self, command: str, interval: float):
        """Schedules a command to be executed on an interval."""

        actor = command.split(" ")[0]
        cmd_str = " ".join(command.split(" ")[1:])

        while True:
            await (await self.tron.send_command(actor, cmd_str))
            await asyncio.sleep(interval)

    async def stop(self):
        """Closes the connection to Tron."""

        for task in self._command_tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._command_tasks = []

        if self.tron and self.tron.transport:
            self.tron.stop()

        self.running = False

    async def process_keyword(self, _, keyword):
        """Processes a keyword received from Tron."""

        key = keyword.keyword
        name = keyword.name

        actor = keyword.model.name

        if self.keywords:
            if actor in self.keywords and name in self.keywords[actor]:
                return

        if len(key.values) == 0:
            return

        points = process_keyword(
            key,
            actor,
            tags=self.tags,
            casts=self.casts,
            keyword_tags=self.keyword_tags,
        )
        data_points = DataPoints(data=points, bucket=self.bucket)

        self.on_next(data_points)


class ActorClientSource(Source):
    """A data source that connects directly to an actor and issues periodic commands.

    This source should be used to complement `.TronSource` when one wants a
    command to be issued periodically without flooding the feed in Tron.

    Parameters
    ----------
    name
        The name of the data source.
    actor
        The name of the actor.
    host
        The host on which the actors is running.
    port
        The port on which the actor is running.
    commands
        A list of commands to issue to the actor on a timer.
    interval
        The interval, in seconds, between commands.
    bucket
        The bucket to write to. If not set it will use the default bucket.
    tags
        A dictionary of tags to be associated with all measurements.
    casts
        A dictionary of ``actor.keyword.key_name`` to cast. E.g.,
        ``{boss.exposure_time.exptime: "float"}``.
    keyword_tags
        A dictionary with a keyword value to be added to the data-points as a tag.
        This is useful for keywords in which the values are not independent and
        one wants to add an additional tag from the keyword itself. For example
        in ``fliswarm.status`` contains several values that all relate to a
        camera which is the first value in the keyword. To add the camera name
        as a tag we can pass ``{"fliswarm.status": {"index": 0, "name": "camera}}``.
    store_broadcasts
        Whether to store broadcast messages that may not be in response to a command.

    """

    source_type = "actor_client"
    timeout = 60

    def __init__(
        self,
        name: str,
        actor: str,
        host: str,
        port: int,
        commands: list[str],
        interval: float = 60.0,
        bucket: Optional[str] = None,
        tags: Dict[str, Any] = {},
        casts: dict[str, str] = {},
        keyword_tags: dict[str, dict] = {},
        store_broadcasts: bool = False,
    ):

        super().__init__(name, bucket=bucket, tags=tags)

        self.transport: asyncio.Transport | None = None
        self.protocol: ClientProtocol | None = None

        self.actor = actor
        self.host = host
        self.port = port

        self.commands = commands
        self._command_tasks: list[asyncio.Task] = []
        self.interval = interval
        self.casts = casts
        self.store_broadcasts = store_broadcasts
        self.keyword_tags = keyword_tags

        self.buffer = b""

        self.keyword_dict = KeysDictionary.load(actor)
        self.rparser: Any = ActorReplyParser()

    async def start(self, get_keys=True):
        """Starts the connection to Tron.

        Parameters
        ----------
        get_keys : bool
            If `True`, gets all the keys in the models.
        """

        loop = asyncio.get_running_loop()
        self.transport, self.protocol = await loop.create_connection(  # type: ignore
            lambda: ClientProtocol(self._handle_reply),
            self.host,
            self.port,
        )

        for command in self.commands:
            self._command_tasks.append(
                asyncio.create_task(self.schedule_command(command))
            )

        self.running = True

        return self

    async def stop(self):
        """Closes the connection."""

        assert self.transport

        for task in self._command_tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._command_tasks = []

        self.transport.close()

        self.running = False
        self.buffer = b""

    def connected(self):
        """Checks whether the client is connected."""

        if self.transport is None:
            return False

        return not self.transport.is_closing()

    async def schedule_command(self, command: str, interval: float | None = None):
        """Schedules a command to be executed on an interval."""

        interval = interval or self.interval

        assert self.transport

        while True:
            self.transport.write(command.encode() + b"\n")
            await asyncio.sleep(interval)

    def _handle_reply(self, data: bytes):
        """Processes a keyword received from the actor.

        Mostly copied from CLU's ``TronConnection``.

        """

        self.buffer += data

        lines = self.buffer.splitlines()
        if not self.buffer.endswith(b"\n"):
            self.buffer = lines.pop()
        else:
            self.buffer = b""

        keys = []
        for line in lines:
            try:
                # Do not strip here or that will cause parsing problems.
                line = line.decode()
                reply: Reply = self.rparser.parse(line)
            except ParseError:
                log.warning(f"{self.name}: failed parsing reply '{line.strip()}'.")
                continue

            for reply_key in reply.keywords:

                key_name = reply_key.name.lower()
                if key_name not in self.keyword_dict:
                    log.warning(
                        f"{self.name}: cannot parse unknown keyword "
                        f"{self.actor}.{reply_key.name}.",
                    )
                    continue

                # When parsed the values in reply_key are string. After consuming
                # it with the Key, the values become typed values.
                result = self.keyword_dict.keys[key_name].consume(reply_key)

                if not result:
                    log.warning(
                        f"{self.name}: failed parsing keyword "
                        f"{self.actor}.{reply_key.name}.",
                    )
                    continue

                if reply.header.commandId == 0 and self.store_broadcasts is False:
                    continue

                keys.append(reply_key)

        points = []

        for key in keys:
            key_points = process_keyword(
                key,
                self.actor,
                tags=self.tags,
                casts=self.casts,
                keyword_tags=self.keyword_tags,
            )
            points += key_points

        data_points = DataPoints(data=points, bucket=self.bucket)

        self.on_next(data_points)
