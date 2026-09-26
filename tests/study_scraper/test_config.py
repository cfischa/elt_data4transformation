"""Tests for `study_scraper.config.Settings`."""

from __future__ import annotations

from study_scraper.config import Settings


class TestEurostatCodes:
    def test_default_codes(self) -> None:
        settings = Settings(_env_file=None)
        assert settings.eurostat_codes == ["env_air_gge", "nrg_bal_s"]

    def test_env_override_is_split_and_stripped(self) -> None:
        settings = Settings(
            _env_file=None, eurostat_default_codes=" nrg_cb_e, gov_10dd_edpt1 "
        )
        assert settings.eurostat_codes == ["nrg_cb_e", "gov_10dd_edpt1"]

    def test_blank_override_yields_empty_list(self) -> None:
        settings = Settings(_env_file=None, eurostat_default_codes="")
        assert settings.eurostat_codes == []


class TestOpenAlexMailto:
    def test_default_is_a_non_empty_contact_address(self) -> None:
        # A default contact lets every scheduled run join OpenAlex's
        # "polite pool" without needing a new repo secret (issue #184).
        settings = Settings(_env_file=None)
        assert settings.openalex_mailto
        assert "@" in settings.openalex_mailto

    def test_env_override(self) -> None:
        settings = Settings(_env_file=None, openalex_mailto="ops@example.com")
        assert settings.openalex_mailto == "ops@example.com"
