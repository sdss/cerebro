#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2021-05-04
# @Filename: AMQP.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
from collections.abc import MutableMapping
from contextlib import suppress

from typing import TYPE_CHECKING, Any, Callable, Coroutine, Optional

from clu import AMQPClient, AMQPReply

from .source import DataPoints, Source


if TYPE_CHECKING:
    from aio_pika import IncomingMessage

__all__ = ["AMQPSource"]


def flatten_dict(
    d: MutableMapping,
    parent_key: str = "",
    sep: str = ".",
    groupers: list[str] = [],
) -> tuple[MutableMapping, dict]:
    """From https://bit.ly/3KN9v3G."""

    groupings = {}

    items = []
    for k, v in d.items():
        if isinstance(v, (tuple, list)):
            continue

        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            flattened, new_groupings = flatten_dict(
                v,
                new_key,
                groupers=groupers,
                sep=sep,
            )
            items.extend(flattened.items())
            groupings.update(new_groupings)
        else:
            items.append((new_key, v))
            if k in groupers:
                groupings[k] = v

    return dict(items), groupings


class ReplyClient(AMQPClient):
    """A client that monitors all the replies."""

    def __init__(
        self,
        *args,
        callback: Callable[[AMQPReply], Coroutine] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.callback = callback

    async def handle_reply(self, message: IncomingMessage):
        """Prints the formatted reply."""

        reply = await super().handle_reply(message)

        if self.callback:
            asyncio.create_task(self.callback(reply))

        return reply


class AMQPSource(Source):
    """A source for AMQP actors.

    Parameters
    ----------
    name
        The name of the data source.
    bucket
        The bucket to write to. If not set it will use the default bucket.
    tags
        A dictionary of tags to be associated with all measurements.
    host
        The host on which RabbitMQ is running.
    port
        The port on which RabbitMQ is running.
    user
        The username to use to use to connect to RabbitMQ.
    password
        The password to use to use to connect to RabbitMQ.
    keywords
        A list of keyword values to output. The format must be
        ``actor.keyword.subkeyword.subsubkeyword``. The final value extracted
        must be a scalar. If `None`, all keywords will be stored.
    groupers
        A list of subkeywords that, if found, will be added as tags to the
        measurement. These are useful when the same keyword can be output
        for different devices or controllers.
    commands
        A mapping of commands to be issued to the interval, in seconds. For
        example ``{"archon status": 5}``.

    """

    source_type = "amqp"
    timeout = 60

    def __init__(
        self,
        name: str,
        bucket: Optional[str] = None,
        tags: dict[str, Any] = {},
        host: str = "localhost",
        port: int = 5672,
        user: str = "guest",
        password: str = "guest",
        keywords: list[str] | None = None,
        groupers: list[str] = [],
        commands: dict[str, float] = {},
    ):
        super().__init__(name, bucket=bucket, tags=tags)

        self.client = ReplyClient(
            f"cerebro_client_{name}",
            user=user,
            password=password,
            host=host,
            port=port,
            callback=self.process_keyword,
        )

        self.keywords = keywords
        self.groupers = groupers

        self.commands = commands
        self._command_tasks: list[asyncio.Task] = []

    async def start(self):
        """Starts the connection to RabbitMQ."""

        await self.client.start()

        for model in self.client.models.values():
            model.register_callback(self.process_keyword)

        for command in self.commands:
            task = self.schedule_command(command, self.commands[command])
            self._command_tasks.append(asyncio.create_task(task))

        self.running = True

    async def stop(self):
        """Closes the connection to Tron."""

        for task in self._command_tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._command_tasks = []

        await self.client.stop()

        self.running = False

    async def schedule_command(self, command: str, interval: float):
        """Schedules a command to be executed on an interval."""

        actor = command.split(" ")[0]
        cmd_str = " ".join(command.split(" ")[1:])

        while True:
            await (await self.client.send_command(actor, cmd_str))
            await asyncio.sleep(interval)

    async def process_keyword(self, reply: AMQPReply):
        """Processes a keyword received from an actor."""

        actor = reply.sender
        body = reply.body

        fields, grouppings = flatten_dict(body, groupers=self.groupers)

        if self.keywords:
            fields = {k: fields[k] for k in fields if k in self.keywords}

        tags = self.tags.copy()
        tags.update(grouppings)

        points = [
            {
                "measurement": actor,
                "tags": tags,
                "fields": fields,
            }
        ]

        data_points = DataPoints(data=points, bucket=self.bucket)

        self.on_next(data_points)
