#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-11
# @Filename: observer.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import abc
import asyncio
import os

from typing import Optional, Type

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS
from influxdb_client.rest import ApiException
from rx.core.observer.observer import Observer as RXObserver
from rx.scheduler.eventloop.asyncioscheduler import AsyncIOScheduler

from . import log


class Observer(RXObserver, metaclass=abc.ABCMeta):
    """An observer class that subscribes to `.Cerebro` updates.

    Observers are subscribed to an instance of `.Cerebro` by calling
    `.set_cerebro`. The `.on_next` method must be overridden to perform
    an action with the received data.

    Parameters
    ----------
    name
        The name of the observer.

    """

    observer_type = None

    def __init__(self, name: str):
        if self.observer_type is None:
            raise ValueError(
                "observer_type is not defined for " f"class {self.__class__.__name__}."
            )

        super().__init__(on_next=self.on_next)

        self.cerebro = None
        self.name = name

        self.loop = asyncio.get_event_loop()
        self.scheduler = AsyncIOScheduler(self.loop)

    def set_cerebro(self, cerebro):
        """Sets the instance of `.Cerebro` and subscribes to it."""

        self.cerebro = cerebro
        self.cerebro.subscribe(self, scheduler=self.scheduler)

    @abc.abstractmethod
    def on_next(self, data):
        pass


class InfluxDB(Observer):
    """A observer that loads data into an InfluxDB database.

    Parameters
    ----------
    url
        The port-qualified URL of the InfluxDB v2 database. Defaults to
        ``http://localhost:9999``.
    org
        The InfluxDB organisation.
    token
        The token to be used to write to the database buckets. If `None`,
        uses the value from ``$INFLUXDB_V2_TOKEN``.
    default_bucket
        The default bucket where to write data. Can be overridden by each
        individual `data source <.Source>`.


    """

    observer_type = "influxdb"

    def __init__(
        self,
        name: str,
        org: str,
        url: str = "http://localhost:9999",
        token: Optional[str] = None,
        default_bucket: Optional[str] = None,
    ):
        super().__init__(name)

        self.default_bucket = default_bucket

        if token is None:
            if "INFLUXDB_V2_TOKEN" in os.environ:
                token = os.environ["INFLUXDB_V2_TOKEN"]
            else:
                raise ValueError("Token not provided or " "found in INFLUXDB_V2_TOKEN")

        # Establish connection to InfluxDB
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_client = self.client.write_api(write_options=ASYNCHRONOUS)

    def dispose(self):
        """Disposes of the observer and closes the connection to InfluxDB."""

        super().dispose()

        if hasattr(self, "client"):
            self.client.__del__()

    def on_next(self, data):
        """Loads data to InfluxDB."""

        bucket = data.bucket or self.default_bucket
        if not bucket:
            raise ValueError("bucket is not defined.")

        try:
            result = self.write_client.write(bucket=bucket, record=data.data)
            result.get()
        except ApiException as ee:
            log.error(f"Failed writing to bucket {bucket}: {ee}")


def get_observer_subclass(type_: str) -> Type[Observer] | None:
    """Returns a `.Observer` subclass based on its ``data_type``."""

    for subclass in Observer.__subclasses__():
        if subclass.observer_type == type_:
            return subclass

    return None
