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

    if not asyncio.iscoroutinefunction(observable):
        raise TypeError('Observable must be a coroutine function.')

    def on_subscribe(observer, scheduler):
        task = asyncio.create_task(observable(observer, scheduler,
                                              *args, **kwargs))
        return Disposable(task.cancel)

    return rx.create(on_subscribe)


class Source(Subject):

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
        pass

    def stop(self):
        pass


def get_source_subclass(type_):

    for subclass in Source.__subclasses__():
        if subclass.source_type == type_:
            return subclass

    return None
