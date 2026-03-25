"""Top-level module for intake_siphon."""

import intake  # noqa: F401

from ._version import __version__
from .cat import ThreddsCatalog  # noqa: F401
from .source import THREDDSMergedSource  # noqa: F401

__all__ = ['ThreddsCatalog', 'THREDDSMergedSource', '__version__']
