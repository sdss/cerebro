#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-05
# @Filename: tron.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import warnings

import numpy

from clu.legacy import TronConnection

from .source import DataPoints, Source


class TronSource(Source):

    source_type = 'tron'

    def __init__(self, name, bucket=None, tags={},
                 actors=[], host='localhost', port=6093, keywords=None):

        super().__init__(name, bucket=bucket, tags=tags)

        self.tron = TronConnection(host, port, model_names=actors)
        self.keywords = keywords

        for model in self.tron.models:
            self.tron.models[model].register_callback(self.process_keyword)

    async def start(self):
        await self.tron.start(get_keys=False)

    def stop(self):
        self.tron.stop()

    def process_keyword(self, model, keyword):

        key = keyword.key
        name = keyword.name

        actor = model.name

        if len(key.values) == 0:
            return

        points = []

        ii = 0
        for key_value in key.values:

            if hasattr(key_value, 'name') and key_value.name:
                key_name = f'_{key_value.name}'
            elif len(key.values) == 1:
                key_name = ''
            else:
                key_name = f'_{ii}'

            tags = self.tags.copy()
            if hasattr(key_value, 'units'):
                tags.update({'units': key_value.units})

            native = key_value.native
            if isinstance(native, (list, tuple, numpy.ndarray)):
                if key_value.__class__.__name__ == 'PVT':
                    fields = {f'{name}{key_name}_P': native[0],
                              f'{name}{key_name}_V': native[1],
                              f'{name}{key_name}_T': native[2]}
                else:
                    warnings.warn(f'Cannot parse {actor}.{name!r} '
                                  f'of type {type(native)!r}.', UserWarning)
                    continue

            else:
                fields = {f'{name}{key_name}': native}

            points.append({'measurement': actor,
                           'tags': self.tags,
                           'fields': fields})

            ii += 1

        data_points = DataPoints(data=points, bucket=self.bucket)

        self.on_next(data_points)
