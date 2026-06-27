"""Offline accuracy evaluation harness (see docs/study_scraper/ACCURACY.md).

Scores three pipeline stages against gold files, with no network and no
API key:

  - topics  : recall/precision of the keyword topic filter
  - claims  : capture rate of the regex percentage extractor
  - attributions : precision/recall + hallucination rate + confidence
                   calibration of the LLM parser, over *captured* model
                   responses (the offline/Cowork path — no live API).

Gold lives in `study_scraper/eval/gold/*.jsonl`. The shipped files are a
tiny SAMPLE so the harness runs today; the real accuracy number needs a
maintainer-curated gold set (ACCURACY.md "Needs the maintainer").
"""
