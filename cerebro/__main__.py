#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2020-08-03
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

import click


@click.command()
def cerebro():
    """Command Line Interface for cerebro."""

    return


def main():
    cerebro(obj={}, auto_envvar_prefix='CEREBRO')


if __name__ == '__main__':
    main()
