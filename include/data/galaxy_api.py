from __future__ import annotations

"""Compatibility wrapper exposing a `get_galaxy_data` function.

This module proxies the real implementation in
`include.custom_functions.galaxy_functions` so DAGs that import
`include.data.galaxy_api` keep working.
"""

from include.custom_functions.galaxy_functions import get_galaxy_data

__all__ = ["get_galaxy_data"]
