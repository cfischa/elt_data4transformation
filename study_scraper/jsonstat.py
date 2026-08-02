"""JSON-stat 2.0 decoding — flattens a lake payload's dimension/value
encoding into typed rows (ROADMAP.md P1 item 6, issue #86).

Per A14, `eurostat.py` stores each dataset's raw JSON-stat 2.0 payload
as-is in `source_records.payload` ("preserve the raw payload, per-table
typed projections happen later"). This module is that later projection.

JSON-stat 2.0 (https://json-stat.org/format/) encodes an N-dimensional
table with:

- ``id``:    ordered list of dimension names, e.g. ``["geo", "time"]``.
- ``size``:  parallel list of category counts per dimension.
- ``dimension[<name>].category.index``: category id -> position (0-based)
  within that dimension — either a ``{id: position}`` dict or a list
  where the list position *is* the index. A dimension missing from
  ``dimension`` (some Eurostat responses omit ones with a single
  category) is treated as one anonymous category per its declared size.
- ``dimension[<name>].category.label``: category id -> human label
  (optional; falls back to the raw id).
- ``value``: either a dense array of length ``prod(size)`` or a sparse
  object mapping the stringified linear index to a value, omitting
  nulls. Row-major order: the LAST dimension in ``id`` varies fastest.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional


def flatten_jsonstat(payload: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Yield one row per non-null data point.

    Each row has one ``<dim>`` key per dimension (the category's human
    label, falling back to its raw id) plus a ``value`` key. Rows whose
    linear index can't be resolved against the declared category counts
    (malformed payload) are skipped rather than guessed at.
    """
    ids: List[str] = list(payload.get("id") or [])
    sizes: List[int] = list(payload.get("size") or [])
    if not ids or len(ids) != len(sizes):
        return
    dimensions = payload.get("dimension") or {}

    categories: List[List[str]] = []
    labels: List[Dict[str, str]] = []
    for dim_id, size in zip(ids, sizes):
        dim = dimensions.get(dim_id) or {}
        category = dim.get("category") or {}
        index = category.get("index")
        if isinstance(index, dict):
            ordered = [cid for cid, _ in sorted(index.items(), key=lambda kv: kv[1])]
        elif isinstance(index, list):
            ordered = list(index)
        else:
            # Dimension not described (common for single-category dims
            # Eurostat elides) — one anonymous category per declared size.
            ordered = [dim_id] * size
        categories.append(ordered)
        labels.append(dict(category.get("label") or {}))

    # Row-major strides: the last dimension varies fastest.
    strides = [1] * len(sizes)
    for i in range(len(sizes) - 2, -1, -1):
        strides[i] = strides[i + 1] * sizes[i + 1]

    value = payload.get("value")
    if isinstance(value, dict):
        items: List[tuple] = []
        for k, v in value.items():
            if v is None:
                continue
            try:
                items.append((int(k), v))
            except (TypeError, ValueError):
                continue
    elif isinstance(value, list):
        items = [(i, v) for i, v in enumerate(value) if v is not None]
    else:
        return

    for linear_index, val in items:
        row = _decode_row(linear_index, val, ids, sizes, strides, categories, labels)
        if row is not None:
            yield row


def _decode_row(
    linear_index: int,
    value: Any,
    ids: List[str],
    sizes: List[int],
    strides: List[int],
    categories: List[List[str]],
    labels: List[Dict[str, str]],
) -> Optional[Dict[str, Any]]:
    if linear_index < 0:
        return None
    row: Dict[str, Any] = {}
    remainder = linear_index
    for dim_pos, dim_id in enumerate(ids):
        cat_pos = remainder // strides[dim_pos]
        remainder %= strides[dim_pos]
        cats = categories[dim_pos]
        if cat_pos >= sizes[dim_pos] or cat_pos >= len(cats):
            return None
        cat_id = cats[cat_pos]
        row[dim_id] = labels[dim_pos].get(cat_id, cat_id)
    row["value"] = value
    return row
