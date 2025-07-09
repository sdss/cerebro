#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2022-12-21
# @Filename: lco.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
from datetime import datetime

from typing import Any, ClassVar, Optional

from sdsstools.utils import cancel_task

from cerebro import log
from cerebro.sources.source import Source
from cerebro.tools import get_lco_seeing_data

from .source import DataPoints


class LCOSeeingDataSource(Source):
    """Retrieve seeing data from DIMM and the Magellan telescopes.

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

    source_type: ClassVar[str] = "lco_seeing_data"
    interval: float = 60

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
        self._last_data: datetime | None = None

    async def start(self):
        """Starts the runner."""

        if self._runner_task:
            await self.stop()

        self._runner_task = asyncio.create_task(self._get_seeing_data())

        await super().start()

    async def stop(self):
        """Stops the runner."""

        self._runner_task = await cancel_task(self._runner_task)
        self.running = False

    async def _get_seeing_data(self):
        """Gets DIMM data from the API."""

        while True:
            for source in ["dimm", "clay", "baade"]:
                try:
                    data = await get_lco_seeing_data(
                        start_time=-120,
                        end_time=None,
                        source=source,  # type: ignore[arg-type]
                        verbose=False,
                    )

                    data = data.drop("source")
                    if source == "dimm":
                        data = data.rename({"elevation": "altitude"})

                    measurement = "dimm" if source == "dimm" else "magellan"
                    tags = self.tags.copy()
                    if source != "dimm":
                        tags["telescope"] = source

                    data_points: list[dict[str, Any]] = []
                    for row in data.iter_rows(named=True):
                        time = row.pop("ts")
                        data_points.append(
                            {
                                "measurement": measurement,
                                "fields": row,
                                "time": time,
                                "tags": tags,
                            }
                        )

                    self.on_next(DataPoints(data=data_points, bucket=self.bucket))

                except Exception as ee:
                    log.error(f"Failed to get {source} data.", exc_info=ee)
                    await asyncio.sleep(self.interval)
                    continue

            await asyncio.sleep(self.interval)
