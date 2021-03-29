#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2021-03-29
# @Filename: test_cerebro.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import cerebro


def test_cerebro():

    assert isinstance(cerebro.Cerebro("cerebro"), cerebro.Cerebro)
