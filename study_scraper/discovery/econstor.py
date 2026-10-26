"""EconStor (ZBW) discovery via OAI-PMH (Dublin Core).

EconStor is an open-access repository of economics working papers and
studies. Its OAI endpoint speaks the same `oai_dc` dialect as SSOAR, so
this reuses `SSOARSource`'s harvest loop and parser (live, `from=`
incremental window, resumption tokens, fixture mode). Metadata licence
is CC0 1.0 (OAI `Identify`, checked 2026-10-26; see DECISIONS.md A52).
"""

from __future__ import annotations

from study_scraper.discovery.ssoar import SSOARSource

DEFAULT_BASE_URL = "https://www.econstor.eu/oai/request"


class EconStorSource(SSOARSource):
    """OAI-PMH discovery against ZBW EconStor."""

    source_id = "econstor"

    def __init__(self, *, base_url: str = DEFAULT_BASE_URL, **kwargs) -> None:
        super().__init__(base_url=base_url, **kwargs)

    def __enter__(self) -> "EconStorSource":
        return self
