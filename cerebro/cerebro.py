#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-03
# @Filename: cerebro.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import asyncio
import os
import time
import warnings

import ntplib
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS
from rx.scheduler.eventloop import AsyncIOScheduler

from sdsstools import read_yaml_file

from .source import Source, get_source_subclass


class MetaCerebro(type):

    def __call__(cls, *args, **kwargs):
        args, kwargs = cls.__parse_config__(cls, *args, **kwargs)
        obj = cls.__new__(cls)
        obj.__init__(*args, **kwargs)
        return obj


class Cerebro(list, metaclass=MetaCerebro):

    def __parse_config__(cls, *args, **kwargs):

        if kwargs.get('config', None) is None:
            return args, kwargs

        kwargs.pop('sources', None)  # Remove input sources if defined.

        config_file = kwargs.pop('config')
        config = read_yaml_file(config_file)
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

    def __init__(self, url='http://localhost:9999', token=None, org=None,
                 default_bucket=None, tags={}, sources=[], config=None,
                 ntp_server='us.pool.ntp.org'):

        self.default_bucket = default_bucket

        if token is None:
            if 'INFLUXDB_V2_TOKEN' in os.environ:
                token = os.environ['INFLUXDB_V2_TOKEN']
            else:
                raise ValueError('Token not provided or '
                                 'found in INFLUXDB_V2_TOKEN')

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

        if name not in self._source_to_index:
            raise ValueError(f'Data source {name!r} not found.')

        return self[self._source_to_index[name]]

    def add_source(self, source):

        if not isinstance(source, Source):
            raise NotImplementedError('Only instances of Source can '
                                      'be passed at this point.')

        source.subscribe(on_next=self._process_next,
                         scheduler=self.scheduler)

        if asyncio.iscoroutinefunction(source.start):
            self.loop.create_task(source.start())
        else:
            self.loop.call_soon(source.start)

        self._source_to_index[source.name] = source
        super().append(source)

    def remove_source(self, source_name):
        source = self._source_to_index[source_name]
        self.remove(source)

    def _process_next(self, measurements):

        if measurements.data == [] or measurements.data is None:
            return

        for point in measurements.data:
            if 'time' not in point:
                # Time is in nanoseconds since UNIX epoch.
                point['time'] = int((time.time() + self._offset / 1e3) * 1e9)

        bucket = measurements.bucket or self.default_bucket
        if not bucket:
            raise ValueError('bucket is not defined.')

        result = self.write_client.write(bucket=bucket,
                                         record=measurements.data)
        result.get()

    def update_time_offset(self, server):

        try:
            ntp = ntplib.NTPClient()
            offset = ntp.request(server, version=3).delay
            if offset:
                self._offset = offset
        except Exception as ee:
            warnings.warn(f'Failed updating offset from NTP server: {ee}',
                          UserWarning)

        self.loop.call_later(3600., self.update_time_offset, server)
