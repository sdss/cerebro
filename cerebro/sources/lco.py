#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2022-12-21
# @Filename: lco.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import warnings
from contextlib import suppress
from datetime import datetime, timedelta
from sqlite3 import OperationalError

from typing import Any, Awaitable, Callable, Optional

import peewee

from cerebro.sources.source import DataPoints

from .. import log
from .source import Source


warnings.filterwarnings(
    "ignore",
    message="Unable to determine MySQL version",
    category=UserWarning,
)


class LCOWeather(Source):
    """Connects to the LCO weather database and grabs some information.

    Connection parameters are hardcoded here because they are unlikely to change.

    Parameters
    ----------
    name
        The name of the data source.
    bucket
        The bucket to write to. If not set it will use the default bucket.
    tags
        A dictionary of tags to be associated with all measurements.
    interval
        How often to query for new data.

    """

    source_type = "lco_weather"
    interval: float = 90

    def __init__(
        self,
        name: str,
        bucket: Optional[str] = None,
        tags: dict[str, Any] = {},
        interval: float | None = None,
    ):
        super().__init__(name, bucket, tags)

        self.interval = interval or self.interval

        self.connection = peewee.MySQLDatabase(
            "weather",
            user="lcodata",
            host="clima.lco.cl",
            read_default_file="~/.my.cnf",
        )

        self.last_data: datetime = datetime.utcnow() - timedelta(minutes=10)

        self._runner: asyncio.Task | None = None
        self._tasks: list[Callable[[], Awaitable]] = [
            self.dimm_data,
            self.magellan_data,
        ]

    async def start(self):
        """Starts the runner."""

        if self._runner:
            await self.stop()

        self._runner = asyncio.create_task(self._run_tasks())

        await super().start()

    async def stop(self):
        """Stops the runner."""

        if self._runner:
            with suppress(asyncio.CancelledError):
                self._runner.cancel()
                await self._runner

        self.running = False

    async def _run_tasks(self):
        """Runs the defined tasks."""

        while True:
            data = []

            if self.connection.is_closed():
                conn_result = False
                try:
                    conn_result = self.connection.connect()
                except OperationalError as err:
                    log.error(f"{self.name}: failed connecting to database: {err!s}")

                if not conn_result:
                    log.error(f"{self.name}: failed connecting to database.")

            if not self.connection.is_closed():
                results = await asyncio.gather(
                    *[asyncio.wait_for(task(), 20) for task in self._tasks],
                    return_exceptions=True,
                )

                for ii, result in enumerate(results):
                    coro_name = self._tasks[ii].__name__
                    if isinstance(result, asyncio.CancelledError):
                        log.error(f"{self.name}: task {coro_name} timed out.")
                    elif isinstance(result, Exception):
                        log.error(f"{self.name}: task {coro_name} failed: {result!s}.")
                    else:
                        data += result

            if len(data) > 0:
                data_points = DataPoints(data=data, bucket=self.bucket)
                self.on_next(data_points)

            self.connection.close()
            self.last_data = datetime.utcnow()

            await asyncio.sleep(self.interval)

    async def dimm_data(self):
        """Gathers data from DIMM."""

        if self.connection.is_closed():
            raise RuntimeError("Database connection is not open.")

        time_str = self.last_data.strftime("'%Y-%m-%d %H:%M:%S'")
        qdata = self.connection.execute_sql(
            f"SELECT tm, se, el FROM dimm_data WHERE tm > {time_str} ORDER BY tm DESC"
        )

        points = [
            {
                "measurement": "dimm",
                "fields": {"seeing": vv[1], "altitude": vv[2]},
                "time": vv[0],
                "tags": self.tags.copy(),
            }
            for vv in qdata
        ]

        return points

    async def magellan_data(self):
        """Gathers data from the Magellans."""

        if self.connection.is_closed():
            raise RuntimeError("Database connection is not open.")

        time_str = self.last_data.strftime("'%Y-%m-%d %H:%M:%S'")
        qdata = self.connection.execute_sql(
            "SELECT tm, un, fw, az, el FROM magellan_data "
            f"WHERE fw IS NOT NULL AND fw > 0 AND tm > {time_str}"
            "ORDER BY tm DESC"
        )

        points = []
        tags = self.tags.copy()

        for vv in qdata:
            if vv[1] == 0:
                telescope = "baade"
            else:
                telescope = "clay"

            points.append(
                {
                    "measurement": "magellan",
                    "fields": {"seeing": vv[2], "azimuth": vv[3], "altitude": vv[4]},
                    "time": vv[0],
                    "tags": {**tags, "telescope": telescope},
                }
            )

        return points
