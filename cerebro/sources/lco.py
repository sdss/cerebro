#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2022-12-21
# @Filename: lco.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import re
import warnings
from datetime import datetime

from typing import Any, ClassVar, Optional

import httpx
from polars import date

from sdsstools.utils import cancel_task

from cerebro import log
from cerebro.sources.source import Source


warnings.filterwarnings(
    "ignore",
    message="Unable to determine MySQL version",
    category=UserWarning,
)


class DIMMSource(Source):
    """Retrieve DIMM data from the LCO ``clima.lco.cl`` API.

    Parameters
    ----------
    name
        The name of the data source.
    bucket
        The bucket to write to. If not set it will use the default bucket.
    route
        The route to the API endpoint.
    tags
        A dictionary of tags to be associated with all measurements.
    interval
        How often to query for new data.

    """

    source_type: ClassVar[str] = "lco_dimm_data"
    base_url: ClassVar[str] = "http://clima.lco.cl"

    interval: float = 90

    def __init__(
        self,
        name: str,
        bucket: Optional[str] = None,
        route: str = "dimm_state",
        tags: dict[str, Any] = {},
        interval: float | None = None,
    ):
        super().__init__(name, bucket, tags)

        self.interval = interval or self.interval
        self.route = route

        self._runner_task: asyncio.Task | None = None
        self._last_data = datetime.now(date.timezone.utc)

    async def start(self):
        """Starts the runner."""

        if self._runner_task:
            await self.stop()

        self._runner_task = asyncio.create_task(self._get_dimm_data())

        await super().start()

    async def stop(self):
        """Stops the runner."""

        self._runner_task = await cancel_task(self._runner_task)
        self.running = False

    async def _get_dimm_data(self):
        """Gets DIMM data from the API."""

        while True:
            try:
                async with httpx.AsyncClient(base_url=self.base_url) as client:
                    response = await client.get(self.route)
                    response.raise_for_status()
                    data = response.text

                match = re.match(
                    r"^time = (?P<time>.+)\naz = (?P<az>.+)\nel = (?P<el>.+)\n"
                    r"seeing = (?P<seeing>.+)\ncounts = (?P<counts>.+)\n$",
                    data,
                )

                if not match:
                    raise ValueError("Could not parse DIMM data API response.")

                match_dict = match.groupdict()
                time = datetime.fromisoformat(match_dict["time"] + "Z")
                alt = float(match_dict["el"])
                seeing = float(match_dict["seeing"])

                # Avoid adding the same point again and again during the day.
                if time <= self._last_data:
                    await asyncio.sleep(self.interval)
                    continue

                self._last_data = time

            except Exception as ee:
                log.error(f"Failed to get DIMM data: {ee}")
                await asyncio.sleep(self.interval)
                continue

            self.on_next(
                [
                    {
                        "measurement": "dimm",
                        "fields": {"seeing": seeing, "altitude": alt},
                        "time": time,
                        "tags": self.tags.copy(),
                    }
                ]
            )

            await asyncio.sleep(self.interval)
