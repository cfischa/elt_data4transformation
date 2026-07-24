"""Domain-audit source discovery (Phase 5d, issue #38).

Coverage must not be bounded by the hard-coded source list. This walks
every kept study's URLs (`canonical_url`, plus the `landing_page_url` /
`pdf_url` captured into `provenance` — see `pipeline._candidate_to_study`
and `storage.list_study_urls`), groups them by an approximate registrable
domain, and surfaces the domains we have no dedicated source for, ranked
by frequency: candidates for the next scraper to build.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urlsplit

from study_scraper.storage import PostgresStorage

# Domains already served by a dedicated discovery/lake source, plus pure
# identifier/redirector domains. doi.org resolves to the *actual*
# publisher (which is what we want surfaced via landing_page_url/pdf_url
# instead) — a doi.org hit itself isn't an actionable "candidate source".
# See DECISIONS.md A30.
KNOWN_DOMAINS = {
    "openalex.org",
    "ssoar.info",
    "bundestag.de",
    "dawum.de",
    "gesis.org",
    "europa.eu",
    "govdata.de",
    "doi.org",
}


def normalize_domain(url: Optional[str]) -> Optional[str]:
    """Best-effort registrable-domain approximation for grouping.

    Strips scheme/path/port/credentials, drops a leading `www.`, and
    keeps the last two dot-separated labels — an eTLD+1 approximation
    without a public-suffix-list dependency (good enough for the mostly
    `*.de`/`*.org`/`*.com` hosts this project sees; multi-part ccTLDs
    like `co.uk` are not special-cased).
    """
    if not url:
        return None
    try:
        netloc = urlsplit(url).netloc
    except ValueError:
        return None
    host = netloc.rsplit("@", 1)[-1].split(":", 1)[0].lower().strip()
    if not host or host.replace(".", "").isdigit():
        return None
    labels = [label for label in host.split(".") if label]
    if not labels:
        return None
    if labels[0] == "www":
        labels = labels[1:]
    if not labels:
        return None
    return ".".join(labels[-2:]) if len(labels) > 1 else labels[0]


def _is_known(domain: str) -> bool:
    return domain in KNOWN_DOMAINS or any(
        domain.endswith(f".{known}") for known in KNOWN_DOMAINS
    )


@dataclass
class DomainStat:
    domain: str
    hits: int
    example_url: str


def audit_domains(storage: PostgresStorage, *, limit: int = 20) -> List[DomainStat]:
    """Unknown domains across stored study URLs, ordered by frequency."""
    counts: Dict[str, int] = {}
    examples: Dict[str, str] = {}
    for row in storage.list_study_urls():
        for url in (row.get("canonical_url"), row.get("landing_page_url"), row.get("pdf_url")):
            domain = normalize_domain(url)
            if domain is None or _is_known(domain):
                continue
            counts[domain] = counts.get(domain, 0) + 1
            examples.setdefault(domain, url)

    stats = [
        DomainStat(domain=domain, hits=hits, example_url=examples[domain])
        for domain, hits in counts.items()
    ]
    stats.sort(key=lambda s: (-s.hits, s.domain))
    return stats[:limit]
