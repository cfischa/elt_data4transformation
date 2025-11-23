# SOEP Connector Status and Maintenance

**Status**: ⚠️ Connector is currently offline. The previously used public API `monitor.soep.de` is unreachable.

## Current Situation
- The codebase implements a connector for the SOEP Monitor API (`connectors/soep_connector.py`).
- The endpoint `https://monitor.soep.de/api/v1` fails to resolve.
- The alternative endpoint `https://companion.soep.de/api/v1` returns 404, indicating no compatible API is exposed there.

## Required Actions
1. **Identify New Data Source**: Access to SOEP data likely now requires:
   - Manual download from DIW Berlin / SOEP Research Data Center.
   - Restricted access credential application.
   - Using the [SOEP-Core](https://www.diw.de/en/diw_01.c.615551.en/research_data_center_soep.html) file distribution (Stata/CSV/SPSS) instead of an API.

2. **Refactor Connector**: The current `SOEPConnector` assumes a JSON REST API. It may need to be rewritten as a file-based loader that ingests manually placed CSVs from the SOEP-Core distribution.

## Documentation
- [SOEP Research Data Center](https://www.diw.de/en/diw_01.c.615551.en/research_data_center_soep.html)
- [SOEP Companion](https://companion.soep.de/)

