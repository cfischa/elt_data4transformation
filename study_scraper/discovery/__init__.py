"""Discovery layer: turn topics into study candidates from concrete sources.

Each source implements `DiscoverySource`. A `Candidate` is what comes out
of a source *before* the topic filter decides whether to keep it.
"""

from study_scraper.discovery.base import Candidate, DiscoverySource

__all__ = ["Candidate", "DiscoverySource"]
