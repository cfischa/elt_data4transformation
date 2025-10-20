"""Adapter utilities for normalising heterogeneous metadata into canonical structures."""

from .metadata import (
    CanonicalDatasetMetadata,
    adapt_destatis_metadata,
    adapt_gesis_metadata,
    canonicalize_metadata,
    register_adapter,
)

__all__ = [
    "CanonicalDatasetMetadata",
    "adapt_destatis_metadata",
    "adapt_gesis_metadata",
    "canonicalize_metadata",
    "register_adapter",
]
