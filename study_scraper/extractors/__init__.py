"""LLM-based extractors (Option A, A21).

These turn regex claims + study text into structured
(question, position, percentage) triples — the answer shape an issue-
polling system needs. The regex extractors (study_scraper/claims.py)
find numbers with context; these say WHAT each number answers.
"""
