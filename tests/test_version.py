import unittest

from unittest.mock import patch, mock_open
from harmony.version import get_version


class TestVersion(unittest.TestCase):
    def setUp(self):
        pass

    def test_version_is_parsed_correctly(self):
        read_data = r'''
        """
        ===========
        __init__.py
        ===========

        Convenience exports for the Harmony library
        """

        # Automatically updated by `make build`
        __version__ = "0.0.1"

        from .adapter import BaseHarmonyAdapter
        from .cli import setup_cli, is_harmony_cli, run_cli
        from .message import Temporal'''
        with patch('builtins.open', mock_open(read_data=read_data)):
            version = get_version()
            assert version == '0.0.1'