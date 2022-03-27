#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2021-05-04
# @Filename: AMQP.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
from contextlib import suppress

from typing import Any, Optional

from clu import AMQPClient

from cerebro import log

from .source import DataPoints, Source


__all__ = ["AMQPSource"]


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
        must be a scalar.
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
        keywords: list[str] = [],
        commands: dict[str, float] = {},
    ):

        super().__init__(name, bucket=bucket, tags=tags)

        key_actors = list(set([key.split(".")[0] for key in keywords]))

        self.client = AMQPClient(
            f"cerebro_client_{name}",
            user=user,
            password=password,
            host=host,
            port=port,
            models=key_actors,
        )

        self.keywords = keywords
        self._actor_keys = {
            actor: [key.split(".")[1] for key in keywords if key.startswith(actor)]
            for actor in key_actors
        }

        self.commands = commands
        self._command_tasks: list[asyncio.Task] = []

    async def start(self):
        """Starts the connection to RabbitMQ."""

        await self.client.start()

        for model in self.client.models.values():
            model.register_callback(self.process_keyword)

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

    async def process_keyword(self, model, keyword):
        """Processes a keyword received from Tron."""

        name = keyword.name
        actor = keyword.model.name

        if name not in self._actor_keys[actor]:
            return

        points = []

        for key in self.keywords:
            if not key.startswith(actor) or key.split(".")[1] != name:
                continue

            value = keyword.value
            try:
                subchunks = key.split(".")[2:]
                field_name = ".".join(subchunks)
                for chunk in subchunks:
                    value = value.get(chunk, None)
                    if value is None:
                        return
            except Exception as err:
                log.warning(f"{self.name}: {err}")
                return

            points.append(
                {
                    "measurement": actor,
                    "tags": self.tags,
                    "fields": {field_name: value},
                }
            )

        data_points = DataPoints(data=points, bucket=self.bucket)

        self.on_next(data_points)
