# encoding: utf-8

import logging

from sdsstools import get_logger, get_package_version


NAME = "sdss-cerebro"


log = get_logger(NAME)
log.sh.setLevel(logging.WARNING)


__version__ = get_package_version(__file__, "sdss-cerebro") or "dev"


from .cerebro import Cerebro
