#!/usr/bin/env python
"""Top-level module for intake_siphon."""
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from .cat import ThreddsCatalog