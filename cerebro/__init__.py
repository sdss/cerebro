# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2026-09-03
# @Filename: __init__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import logging

from sdsstools import get_logger, get_package_version


NAME = "sdss-cerebro"


log = get_logger(NAME)
log.sh.setLevel(logging.WARNING)


__version__ = get_package_version(__file__, "sdss-cerebro") or "dev"


from .cerebro import Cerebro
