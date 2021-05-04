"""
===========
__init__.py
===========

Convenience exports for the Harmony library
"""

# Automatically updated by `make build`
__version__ = "1.0.8-alpha.6"

from .adapter import BaseHarmonyAdapter
from .cli import setup_cli, is_harmony_cli, run_cli
from .message import Temporal
