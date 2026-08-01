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
