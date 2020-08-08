#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-03
# @Filename: source.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import asyncio
import collections

import rx
from rx.disposable import Disposable
from rx.subject import Subject


__all__ = ['DataPoints', 'wrap_async_observable', 'Source',
           'get_source_subclass']


DataPoints = collections.namedtuple('DataPoints', ('bucket', 'data'))


def wrap_async_observable(observable, *args, **kwargs):
    """Wraps a coroutine and creates an observable."""

    if not asyncio.iscoroutinefunction(observable):
        raise TypeError('Observable must be a coroutine function.')

    def on_subscribe(observer, scheduler):
        task = asyncio.create_task(observable(observer, scheduler,
                                              *args, **kwargs))
        return Disposable(task.cancel)

    return rx.create(on_subscribe)


class Source(Subject):
    """A base class for a data source.

    Data sources are standalone systems that periodically produces measurements
    for one or multiple fields. And example is a TCP server that outputs
    temperature and humidity values as plain ASCII. The data source handles the
    connection and parsing of the stream, and reports new measurements to
    `.Cerebro`.

    Internally a source is an :py:class:`~rx.subject.Subject`, which acts as
    both an observer and observable. When the source calls ``on_next(value)``
    the value is received by `.Cerebro`, which handles the write to the
    database.

    Parameters
    ----------
    name : str
        The name of the data source.
    bucket : str
        The bucket to write to. If not set it will use the default bucket.
    tags : dict
        A dictionary of tags to be associated with all measurements.

    """

    #: str: The type of data source.
    source_type = None

    def __init__(self, name, bucket=None, tags={}):

        if self.source_type is None:
            raise ValueError('Subclasses must override source_type.')

        super().__init__()

        self.name = name
        self.bucket = bucket

        self.tags = tags.copy()
        self.tags.update({'source': self.source_type})

        self.loop = asyncio.get_event_loop()

    def start(self):
        """Initialises the source.

        This method is called by `.Cerebro` when the data source is added. It
        can be empty but more frequently it contains the code to initialise the
        connection to a TCP server or other data stream. It can be a coroutine,
        in which case `.Cerebro` will schedule it as a task.

        """

        pass

    def stop(self):
        """Stops the data source parsing and closes any open connection."""

        pass


def get_source_subclass(type_):
    """Returns a `.Source` subclass based on its ``data_type``."""

    for subclass in Source.__subclasses__():
        if subclass.source_type == type_:
            return subclass

    return None
