#!/usr/bin/env python
"""Top-level module for intake_siphon."""

import intake  # noqa: F401
from pkg_resources import DistributionNotFound, get_distribution

from .cat import ThreddsCatalog  # noqa: F401

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # noqa: F401
    __version__ = '0.0.0'  # noqa: F401
