"""
===========
__init__.py
===========

Convenience exports for the Harmony library
"""

# Automatically updated by `make build`
__version__ = "v1.0.21"

from .adapter import BaseHarmonyAdapter
from .cli import setup_cli, is_harmony_cli, run_cli
from .message import Temporal
from pystac.stac_io import STAC_IO
from .s3_stac_io import read, write

STAC_IO.read_text_method = read
STAC_IO.write_text_method = write
