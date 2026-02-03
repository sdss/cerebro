#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2022-10-15
# @Filename: protocols.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import asyncio
import random


__all__ = ["ReconnectingTCPClientProtocol", "ClientProtocol"]


class ReconnectingTCPClientProtocol(asyncio.Protocol):
    """A reconnecting client modelled after Twisted ``ReconnectingClientFactory``.

    Taken from https://bit.ly/3yn6MWa.
    """

    max_delay = 3600
    initial_delay = 1.0
    factor = 2.7182818284590451
    jitter = 0.119626565582
    max_retries = None

    def __init__(self, *args, loop=None, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._retries = 0
        self._delay = self.initial_delay
        self._continue_trying = True
        self._call_handle = None
        self._connector = None
        self.connected = False

    def connection_lost(self, exc):
        self.connected = False
        if self._continue_trying:
            self.retry()

    def connection_failed(self, exc):
        self.connected = False
        if self._continue_trying:
            self.retry()

    def retry(self):
        if not self._continue_trying:
            return

        self._retries += 1
        if self.max_retries is not None and (self._retries > self.max_retries):
            return

        self._delay = min(self._delay * self.factor, self.max_delay)
        if self.jitter:
            self._delay = random.normalvariate(self._delay, self._delay * self.jitter)
        self._call_handle = asyncio.get_running_loop().call_later(
            self._delay, self.connect
        )

    def connect(self):
        if self._connector is None:
            self._connector = asyncio.create_task(self._connect())

    async def _connect(self):
        try:
            await asyncio.get_running_loop().create_connection(
                lambda: self,
                *self._args,
                **self._kwargs,
            )
            self.connected = True
        except Exception as exc:
            asyncio.get_running_loop().call_soon(self.connection_failed, exc)
        finally:
            self._connector = None

    def stop_trying(self):
        if self._call_handle:
            self._call_handle.cancel()
            self._call_handle = None
        self._continue_trying = False
        if self._connector is not None:
            self._connector.cancel()
            self._connector = None
        self.connected = False


class ClientProtocol(ReconnectingTCPClientProtocol):
    """A reconnecting protocol."""

    def __init__(self, on_received, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_received = on_received
        self.transport: asyncio.Transport | None = None

    def data_received(self, data):
        asyncio.get_running_loop().call_soon(self._on_received, data)

    def connection_made(self, transport: asyncio.Transport):
        self.transport = transport
