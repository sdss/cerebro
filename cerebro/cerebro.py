#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-03
# @Filename: cerebro.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import asyncio
import datetime
import os
import pathlib
import socket
import time
import uuid
import warnings

import ntplib
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS, ApiException
from rx.scheduler.eventloop import AsyncIOScheduler

from sdsstools import read_yaml_file

from . import log
from .source import Source, get_source_subclass


class MetaCerebro(type):
    """Metaclass for Cerebro."""

    def __call__(cls, *args, **kwargs):
        args, kwargs = cls.__parse_config__(cls, *args, **kwargs)
        obj = cls.__new__(cls)
        obj.__init__(*args, **kwargs)
        return obj


class Cerebro(list, metaclass=MetaCerebro):
    """Handles a list of data sources and writes to an InfluxDB v2 server.

    Creates an `RX <https://rxpy.readthedocs.io/en/latest/>`__ observer that
    monitors a list of data sources.

    name : str
        The name of this Cerebro instance. This value is added as the
        ``cerebro`` tag to all measurements processed. The name can be used
        to identify the origin of the data when multiple instances of Cerebro
        are loading data to the database.
    url : str
        The port-qualified URL of the InfluxDB v2 database. Defaults to
        ``http://localhost:9999``.
    token : str
        The token to be used to write to the database buckets. If `None`,
        uses the value from ``$INFLUXDB_V2_TOKEN``.
    org : str
        The InfluxDB organisation.
    default_bucket : str
        The default bucket where to write data. Can be overridden by each
        individual `data source <.Source>`.
    tags : dictionary
        A list of tags to add to all the measurements.
    sources : list
        A list of `.Source` instances to listen to.
    config : dict or str
        A file or dictionary from which to load the configuration. The format
        exactly follows the signature of `.Cerebro` and the data sources it
        refers to. For example:

        .. code-block:: yaml

            url: http://localhost:9999
            token: null
            org: SDSS
            default_bucket: FPS
            ntp_server: us.pool.ntp.org

            tags:
                observatory: ${OBSERVATORY}

            sources:
                tron:
                    type: tron
                    bucket: Actors
                    host: localhost
                    port: 6093
                    actors:
                        - tcc
                        - apo

        Each source must include a ``type`` key that correspond to the
        ``source_type`` value of the `.Source` subclass to be used.
    ntp_server : str
        The route to the NTP server to use. The server is queried every hour
        to determine the offset between the NTP pool and the local computer.
    logfile : str
        If set, the path where to write the file log. Otherwise logs only to
        stdout/stderr.
    log_rotate : bool
        Whether to rotate the log file at midnight UTC.

    """

    def __parse_config__(cls, *args, **kwargs):
        """Overrides initialisation parameters from a configuration file."""

        if kwargs.get('config', None) is None:
            return args, kwargs

        kwargs.pop('sources', None)  # Remove input sources if defined.

        config_file = kwargs.pop('config')
        if isinstance(config_file, (str, pathlib.Path)):
            config = read_yaml_file(config_file)
        elif isinstance(config_file, dict):
            config = config_file.copy()
        else:
            raise ValueError(f'Invalid type {type(config_file)} for config.')
        config.update(kwargs)

        sources_config = config.pop('sources', [])

        sources = []
        for source_name, params in sources_config.items():
            type_ = params.pop('type')
            Subclass = get_source_subclass(type_)
            if Subclass is None:
                raise ValueError(f'Source type {type_} is not valid.')
            sources.append(Subclass(source_name, **params))

        config['sources'] = sources

        return args, config

    def __init__(self, name, url='http://localhost:9999', token=None, org=None,
                 default_bucket=None, tags={}, sources=[], config=None,
                 ntp_server='us.pool.ntp.org', logfile=None, log_rotate=True):

        self.name = name
        self.default_bucket = default_bucket

        self.run_id = str(uuid.uuid4())

        host = socket.getfqdn()

        if logfile:
            if os.path.isdir(logfile) and os.path.exists(logfile):
                logfile = os.path.join(logfile, f'{name}.log')
            log.start_file_logger(logfile, rotating=log_rotate)

        start_time = datetime.datetime.utcnow().isoformat()

        log.debug(f'Starting Cerebro at {start_time} on host {host} '
                  f'with run_id={self.run_id!r}')

        if token is None:
            if 'INFLUXDB_V2_TOKEN' in os.environ:
                token = os.environ['INFLUXDB_V2_TOKEN']
            else:
                raise ValueError('Token not provided or '
                                 'found in INFLUXDB_V2_TOKEN')

        # Add the name of the instance and the host to the default tags.
        tags = tags.copy()
        tags.update({'cerebro': self.name,
                     'cerebro_host': host,
                     'run_id': self.run_id})

        # Establish connection to InfluxDB
        self.client = InfluxDBClient(url=url, token=token, org=org,
                                     default_tags=tags)
        self.write_client = self.client.write_api(write_options=ASYNCHRONOUS)

        self.loop = asyncio.get_event_loop()
        self.scheduler = AsyncIOScheduler(self.loop)

        super().__init__()

        self._source_to_index = {}
        for source in sources:
            self.add_source(source)

        self._offset = 0
        self.loop.call_soon(self.update_time_offset, ntp_server)

    def stop(self):
        """Stops the InfluxDB client and all the sources."""

        if hasattr(self, 'client'):
            self.client.__del__()

        for source in self:
            if asyncio.iscoroutinefunction(source.stop):
                close_task = asyncio.create_task(source.stop())
                while not close_task.done():
                    pass
            else:
                source.stop()

    def __del__(self):
        self.stop()

    def __add__(self, x):
        raise NotImplementedError('Addition is not implemented in Cerebro.')

    def insert(self, *args):
        raise NotImplementedError('Insert is not implemented in Cerebro.')

    def remove(self, source):
        source.dispose()
        return super().remove(source)

    def append(self, source):
        return self.add_source(source)

    def pop(self, index):
        return self.remove_source(self[index].name)

    def get(self, name):
        """Retrieves a data source by name."""

        if name not in self._source_to_index:
            raise ValueError(f'Data source {name!r} not found.')

        return self[self._source_to_index[name]]

    def add_source(self, source):
        """Adds a `.Source`."""

        def check_start(task):
            if task.done():
                exception = task.exception()
                if exception:
                    log.error('Failed starting data source '
                              f'{source.name}: {exception!s}')
            else:
                log.error(f'Timed out trying to start source {source.name}.')

        if not isinstance(source, Source):
            raise NotImplementedError('Only instances of Source can '
                                      'be passed at this point.')

        source.subscribe(on_next=self._process_next,
                         scheduler=self.scheduler)

        if asyncio.iscoroutinefunction(source.start):
            task = self.loop.create_task(source.start())
            self.loop.call_later(5, check_start, task)
        else:
            self.loop.call_soon(source.start)

        self._source_to_index[source.name] = source
        super().append(source)

    def remove_source(self, source_name):
        """Removes a source."""

        source = self._source_to_index[source_name]
        self.remove(source)

    def _process_next(self, measurements):
        """Processes a list of measurements from a data source."""

        if measurements.data == [] or measurements.data is None:
            return

        for point in measurements.data:
            if 'time' not in point:
                # Time is in nanoseconds since UNIX epoch.
                point['time'] = int((time.time() + self._offset / 1e3) * 1e9)

        bucket = measurements.bucket or self.default_bucket
        if not bucket:
            raise ValueError('bucket is not defined.')

        try:
            result = self.write_client.write(bucket=bucket,
                                             record=measurements.data)
            result.get()
        except ApiException as ee:
            log.error(f'Failed writing to bucket {bucket}: {ee}')

    def update_time_offset(self, server):
        """Updates the internal offset with the NTP server."""

        try:
            ntp = ntplib.NTPClient()
            offset = ntp.request(server, version=3).delay
            if offset:
                self._offset = offset
        except Exception as ee:
            warnings.warn(f'Failed updating offset from NTP server: {ee}',
                          UserWarning)

        self.loop.call_later(3600., self.update_time_offset, server)
