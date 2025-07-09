#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2025-07-08
# @Filename: tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import datetime
import os
import time

from typing import Literal

import httpx
import polars
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from rich.console import Console


__all__ = [
    "get_lco_weather_data",
    "get_lco_seeing_data",
    "ingest_dataframe_to_influxdb",
]


LCO_ENV_URL = "http://env-api.lco.cl/metrics"

LCO_WEATHER_SCHEMA = polars.Schema(
    {
        "ts": polars.String,
        "temperature": polars.Float32,
        "air_pressure": polars.Float32,
        "wind_dir_avg": polars.Float32,
        "wind_dir_max": polars.Float32,
        "wind_dir_min": polars.Float32,
        "rain_intensity": polars.Float32,
        "wind_speed_avg": polars.Float32,
        "wind_speed_max": polars.Float32,
        "wind_speed_min": polars.Float32,
        "relative_humidity": polars.Float32,
    }
)

LCO_SEEING_DIMM_SCHEMA = polars.Schema(
    {
        "ts": polars.String,
        "seeing": polars.Float32,
        "counts": polars.Float32,
        "azimuth": polars.Float32,
        "elevation": polars.Float32,
    }
)

LCO_SEEING_MAGELLAN_SCHEMA = polars.Schema(
    {
        "ts": polars.String,
        "seeing": polars.Float32,
        "counts": polars.Float32,
        "air_temp": polars.Float32,
    }
)

console = Console()


def format_time(time: str | float) -> str:
    """Formats a time string for the LCO weather API format"""

    if isinstance(time, (float, int)):
        time = (
            datetime.datetime.fromtimestamp(
                time,
                tz=datetime.timezone.utc,
            )
            .isoformat(timespec="seconds")
            .replace("+00:00", "")
        )

    if "." in time:
        time = time.split(".")[0]

    return time


async def get_from_lco_api(
    start_time: str,
    end_time: str,
    source: str,
    measurement: Literal["weather", "seeing"] = "weather",
    verbose: bool = False,
):
    """Queries the LCO API for weather or seeing data."""

    start_time_dt = datetime.datetime.strptime(
        start_time,
        "%Y-%m-%dT%H:%M:%S",
    )
    end_time_dt = datetime.datetime.strptime(
        end_time,
        "%Y-%m-%dT%H:%M:%S",
    )

    if measurement == "weather":
        schema = LCO_WEATHER_SCHEMA
    elif measurement == "seeing":
        if source.lower() in ["clay", "baade"]:
            schema = LCO_SEEING_MAGELLAN_SCHEMA
        else:
            schema = LCO_SEEING_DIMM_SCHEMA

    data_chunks: list[polars.DataFrame] = []

    async with httpx.AsyncClient() as client:
        dt0 = start_time_dt

        # The API will only return one hour of data so we loop over it and concatenate.
        while True:
            # Use chunks of 15 days to be completely sure the API will return
            # the data for that interval.
            dt1 = dt0 + datetime.timedelta(days=15)

            if verbose:
                console.print(
                    f"[bright_black]Querying LCO {measurement} data from "
                    f"{dt0} to {dt1} for source {source}[/]",
                    highlight=False,
                )

            response = await client.get(
                f"{LCO_ENV_URL}/{measurement}",
                params={
                    "start_ts": dt0.strftime("%Y-%m-%dT%H:%M:%S"),
                    "end_ts": dt1.strftime("%Y-%m-%dT%H:%M:%S"),
                    "source": source.lower(),
                },
                timeout=30,
            )

            if response.status_code != 200:
                raise ValueError(f"Failed to get weather data: {response.text}")

            data = response.json()

            if "Error" in data:
                raise ValueError(f"Failed to get {measurement} data: {data['Error']}")
            elif "results" not in data or data["results"] is None:
                raise ValueError(f"Failed to get {measurement} data: no results found.")
            elif len(data["results"]) == 0:
                data_df = polars.DataFrame([], schema=schema)
            else:
                data_df = polars.DataFrame(data["results"], schema=schema)

            data_chunks.append(data_df)

            if dt1 >= end_time_dt:
                break

            # Increase the time window by a bit less than 30 minutes to ensure that
            # we don't lose any data points.
            dt0 = dt0 + datetime.timedelta(days=14.9)

    data = polars.concat(data_chunks)

    # Remove duplicate timestamps.
    data = data.sort("ts").unique("ts")

    # Convert the timestamp to a datetime object in UTC.
    data = data.with_columns(
        ts=polars.col.ts.str.to_datetime(
            time_unit="ms",
            time_zone="UTC",
        )
    )

    # Filter the data to the requested time range.
    data = data.filter(
        polars.col.ts.dt.replace_time_zone(None) >= start_time_dt,
        polars.col.ts.dt.replace_time_zone(None) <= end_time_dt,
    )

    return data


async def get_lco_weather_data(
    start_time: str | float = -300,
    end_time: str | float | None = None,
    source: Literal["dupont", "swope", "magellan"] = "dupont",
    verbose: bool = False,
):
    """Returns a data frame with weather data from the LCO environment service.

    Parameters
    ----------
    start_time
        The start time of the query. Can be a UNIX timestamp or an ISO datetime string.
        If the float value is negative, it is interpreted as the number of seconds
        before the current time. Defaults to -300 seconds (5 minutes ago).
    end_time
        The end time of the query. Can be a UNIX timestamp or an ISO datetime string.
        Defaults to the current time.
    source
        The source to query. Must be one of 'dupont', 'swope', or 'magellan'.
    verbose
        If :obj:`True`, prints information about the query to the console.

    Returns
    -------
    weather_data
        A data frame with the weather data.

    """

    if source not in ["dupont", "swope", "magellan"]:
        raise ValueError("source must be one of 'dupont', 'swope', or 'magellan'.")

    if isinstance(start_time, (float, int)) and start_time < 0:
        start_time = time.time() + start_time

    start_time = format_time(start_time)
    end_time = format_time(end_time or time.time())

    df = await get_from_lco_api(
        start_time,
        end_time,
        source,
        measurement="weather",
        verbose=verbose,
    )
    df = df.with_columns(source=polars.lit(source, polars.String))

    # Temperature is in Fahrenheit, convert to Celsius.
    df = df.with_columns(temperature=(polars.col.temperature - 32) * 5 / 9)

    # Delete rows with all null values.
    df = df.filter(~polars.all_horizontal(polars.exclude("ts", "source").is_null()))

    # Sort by timestamp and keep only unique timestamps.
    df = (
        df.sort("ts")
        .unique("ts")
        .drop_nulls(["wind_speed_avg", "wind_speed_max", "wind_dir_avg"])
    )

    # Calculate rolling means for average wind speed and gusts every 5m, 10m, 30m
    window_sizes = ["5m", "10m", "30m"]
    df = df.with_columns(
        **{
            f"wind_speed_avg_{ws}": polars.col.wind_speed_avg.rolling_mean_by(
                by="ts",
                window_size=ws,
            )
            for ws in window_sizes
        },
        **{
            f"wind_gust_{ws}": polars.col.wind_speed_max.rolling_max_by(
                by="ts",
                window_size=ws,
            )
            for ws in window_sizes
        },
        **{
            f"wind_dir_avg_{ws}": polars.col.wind_dir_avg.rolling_mean_by(
                by="ts",
                window_size=ws,
            )
            for ws in window_sizes
        },
    )

    # Add simple dew point.
    df = df.with_columns(
        dew_point=polars.col.temperature
        - ((100 - polars.col.relative_humidity) / 5.0).round(2)
    )

    # Change float precision to f32
    df = df.with_columns(polars.selectors.float().cast(polars.Float32))

    return df


async def get_lco_seeing_data(
    start_time: str | float = -300,
    end_time: str | float | None = None,
    source: Literal["dimm", "clay", "baade"] = "dimm",
    verbose: bool = False,
):
    """Returns a data frame with seeing data from the LCO environment service.

    Parameters
    ----------
    start_time
        The start time of the query. Can be a UNIX timestamp or an ISO datetime string.
        If the float value is negative, it is interpreted as the number of seconds
        before the current time. Defaults to -300 seconds (5 minutes ago).
    end_time
        The end time of the query. Can be a UNIX timestamp or an ISO datetime string.
        Defaults to the current time.
    source
        The source to query. Must be one of 'dimm', 'clay', or 'baade'.
    verbose
        If :obj:`True`, prints information about the query to the console.

    Returns
    -------
    seeing_data
        A data frame with the seeing data.

    """

    if source not in ["dimm", "clay", "baade"]:
        raise ValueError("source must be one of 'dimm', 'clay', or 'baade'.")

    if isinstance(start_time, (float, int)) and start_time < 0:
        start_time = time.time() + start_time

    start_time = format_time(start_time)
    end_time = format_time(end_time or time.time())

    df = await get_from_lco_api(
        start_time,
        end_time,
        source,
        measurement="seeing",
        verbose=verbose,
    )
    df = df.with_columns(source=polars.lit(source, polars.String))

    # We don't care about the counts or air temperature
    df = df.drop(["counts", "air_temp"], strict=False)

    # Delete rows with all null values.
    df = df.filter(~polars.all_horizontal(polars.exclude("ts", "source").is_null()))

    # Change float precision to f32
    df = df.with_columns(polars.selectors.float().cast(polars.Float32))

    return df


async def ingest_dataframe_to_influxdb(
    df: polars.DataFrame,
    measurement: str,
    time_column: str,
    url: str,
    bucket: str,
    org: str,
    token: str | None = None,
    tags: dict[str, str] = {},
    batch_points: int = 1000,
):
    """Ingests a Polars DataFrame to InfluxDB.

    Parameters
    ----------
    df
        The Polars DataFrame to ingest.
    measurement
        The name of the measurement to write to InfluxDB.
    time_column
        The name of the column in the DataFrame that contains the time values.
    url
        The URL of the InfluxDB instance, e.g., ``"http://localhost:8086"``.
    bucket
        The InfluxDB bucket where the data will be written.
    org
        The InfluxDB organisation to which the bucket belongs.
    token
        The InfluxDB token to use for authentication. If :obj:`None`, uses the value
        from the environment variable ``INFLUXDB_V2_TOKEN``.
    tags
        A dictionary of tags to add to each data point. These will be added as tags
        to the measurement in InfluxDB.
    batch_points
        The number of points to batch together when writing to InfluxDB.

    """

    if token is None:
        token = os.getenv("INFLUXDB_V2_TOKEN")
        if not token:
            raise ValueError("Token not provided and $INFLUXDB_V2_TOKEN not set.")

    async with InfluxDBClientAsync(url=url, token=token, org=org) as client:
        write_api = client.write_api()

        data_points: list[dict] = []
        for nn, row in enumerate(df.iter_rows(named=True)):
            time = row.pop(time_column)
            point = {
                "measurement": measurement,
                "tags": tags,
                "time": time,
                "fields": row,
            }
            data_points.append(point)

            # Batch write every batch_points points or at the end of the DataFrame.
            if len(data_points) >= batch_points or nn == len(df) - 1:
                result = await write_api.write(
                    bucket=bucket,
                    record=data_points,
                )
                if not result:
                    raise ValueError("Failed to write data to InfluxDB.")

                data_points = []  # Reset the data points list
