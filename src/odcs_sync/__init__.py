"""ODCS Sync - Synchronize Open Data Contract Standard YAML to Databricks Unity Catalog."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("odcs-sync")
except PackageNotFoundError:
    # Package not installed (running from source)
    __version__ = "0.0.0-dev"
