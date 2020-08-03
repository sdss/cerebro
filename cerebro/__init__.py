# encoding: utf-8

from sdsstools import get_logger, get_package_version


NAME = 'sdss-cerebro'

__version__ = get_package_version(__file__, 'sdss-cerebro') or 'dev'

# Get a logger, mostly for warning formatting.
log = get_logger(NAME)
