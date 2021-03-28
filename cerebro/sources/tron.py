#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-05
# @Filename: tron.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import warnings

from typing import Any, Optional

import numpy

from clu.legacy import TronConnection

from .source import DataPoints, Source


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

    """

    source_type = "tron"
    timeout = 60

    def __init__(
        self,
        name: str,
        bucket: Optional[str] = None,
        tags: dict[str, Any] = {},
        actors: list[str] = [],
        host: str = "localhost",
        port: int = 6093,
        keywords: Optional[list[str]] = None,
    ):

        super().__init__(name, bucket=bucket, tags=tags)

        self.tron = TronConnection(host, port, models=actors)
        self.keywords = keywords

        for model in self.tron.models:
            self.tron.models[model].register_callback(
                self.process_keyword
            )  # type: ignore

    async def start(self):
        """Starts the connection to Tron."""

        await self.tron.start(get_keys=False)

    def stop(self):
        """Closes the connection to Tron."""

        if self.tron and self.tron._client:
            self.tron.stop()

    async def process_keyword(self, model, keyword):
        """Processes a keyword received from Tron."""

        key = keyword.keyword
        name = keyword.name

        actor = model.name

        if self.keywords:
            if actor in self.keywords and name in self.keywords[actor]:
                return

        if len(key.values) == 0:
            return

        points = []

        ii = 0
        for key_value in key.values:

            if hasattr(key_value, "name") and key_value.name:
                key_name = f"_{key_value.name}"
            elif len(key.values) == 1:
                key_name = ""
            else:
                key_name = f"_{ii}"

            tags = self.tags.copy()
            if hasattr(key_value, "units"):
                tags.update({"units": key_value.units})

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
                fields = {f"{name}{key_name}": native}

            points.append({"measurement": actor, "tags": self.tags, "fields": fields})

            ii += 1

        data_points = DataPoints(data=points, bucket=self.bucket)

        self.on_next(data_points)
