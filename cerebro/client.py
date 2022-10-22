#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2022-10-22
# @Filename: client.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import os

from influxdb_client import InfluxDBClient


class CerebroClient:
    """A client connection to InfluxDB.

    Relatively shallow wrapper around ``InfluxDBClient``.

    Parameters
    ----------
    host
        The host on which InfluxDB is running.
    port
        The port on which InfluxDB is running.
    token
        The token to query InfluxDB. Not needed if ``$INFLUXDB_V2_TOKEN``
        is set.
    org
        The InfluxDB organisation.

    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8086,
        token: str | None = None,
        org: str = "sdss5",
    ):

        if token is None:
            if "INFLUXDB_V2_TOKEN" in os.environ:
                token = os.environ["INFLUXDB_V2_TOKEN"]
            else:
                raise ValueError("Token not provided and $INFLUXDB_V2_TOKEN not set.")

        self.client = InfluxDBClient(f"http://{host}:{port}", token=token, org=org)

        try:
            self.client.ready()
        except Exception as err:
            raise RuntimeError(f"Failed connecting to InfluxDB: {err}")

        self.api = self.client.query_api()

    def _build_query(
        self,
        bucket: str,
        measurement: str,
        field: str | None = None,
        start: str | None = None,
        end: str | None = None,
        pivot: bool = False,
    ):
        """Buelds a query string."""

        query = f'from(bucket:"{bucket}")\n'

        if start is not None or end is not None:
            query += "|> range("
            if start is not None:
                query += f"start: {start}"
            if end is not None:
                if start is not None:
                    query += ", "
                query += f"end: {end}"
            query += ")\n"

        query += f'|> filter(fn: (r) => r["_measurement"] == "{measurement}") \n'

        if field:
            query += f'|> filter(fn: (r) => r["_field"] == "{field}") \n'

        if pivot:
            query += (
                '|> pivot(rowKey:["_time"], columnKey: ["_field"], '
                'valueColumn: "_value") \n'
            )

        return query

    def query(
        self,
        bucket: str,
        measurement: str,
        field: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ):
        """Query InfluxDB for a measurement and returns a Pandas dataframe.

        Parameters
        ----------
        bucket
            The bucket in which the measurements are stored.
        measurement
            The name of the measurement.
        field
            The name of the field to query (optional).
        start
            The start date for the query. Can be a full ISOT date
            (``2022-10-01T00:05:00Z``) or a relative range (``-15m``).
        end
            The end date for the query.

        """

        query = self._build_query(
            bucket,
            measurement,
            field=field,
            start=start,
            end=end,
            pivot=True,
        )
        print(query)

        return self.api.query_data_frame(query)
