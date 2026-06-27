# Accuracy report

_Generated offline_

Run: `python -m study_scraper eval`. Gold lives in `study_scraper/eval/gold/`. The shipped gold is a SAMPLE — replace it with a curated set for a meaningful number (see ACCURACY.md).

| Stage | Metric | Value | n |
| --- | --- | --- | --- |
| Topic filter | recall | 100.0% | 6 |
| Topic filter | precision | 100.0% | 6 |
| Claims | capture rate | 100.0% | 3 |
| Attribution | precision | 75.0% | 2 |
| Attribution | recall | 100.0% | 2 |
| Attribution | hallucination rate | 25.0% | 2 |
| Attribution | calibration @conf≥0.8 | 100.0% | 2 |

