"""Helpers for dynamically loading project modules from Airflow DAGs."""

from __future__ import annotations

import importlib
import logging
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import Callable, Iterable, Iterator, Optional, Sequence, Tuple, Union, cast

LOGGER = logging.getLogger(__name__)

PathLike = Union[str, os.PathLike[str]]
MainCallable = Callable[[Optional[Sequence[str]]], int]


def _resolve_path(value: PathLike) -> Path:
    path = Path(value)
    try:
        return path.resolve(strict=False)
    except FileNotFoundError:
        return path.absolute()


def _gather_base_candidates(dag_file: PathLike) -> Sequence[Path]:
    dag_path = _resolve_path(dag_file)
    dag_dir = dag_path if dag_path.is_dir() else dag_path.parent

    candidates = [dag_dir]
    candidates.extend(parent for parent in dag_dir.parents)

    for env_name in ("AIRFLOW_HOME", "PIPELINE_HOME"):
        env_value = os.environ.get(env_name)
        if not env_value:
            continue
        env_path = _resolve_path(env_value)
        candidates.append(env_path)
        parent = env_path.parent
        if parent != env_path:
            candidates.append(parent)

    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        candidate_path = _resolve_path(candidate)
        if not candidate_path.exists() or not candidate_path.is_dir():
            continue
        key = str(candidate_path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate_path)
    return unique


def _iter_search_roots(dag_file: PathLike) -> Iterator[Path]:
    seen: set[str] = set()
    for base in _gather_base_candidates(dag_file):
        key = str(base)
        if key not in seen:
            seen.add(key)
            yield base
        try:
            for child in base.iterdir():
                if not child.is_dir():
                    continue
                child_key = str(child.resolve(strict=False))
                if child_key in seen:
                    continue
                seen.add(child_key)
                yield child.resolve(strict=False)
        except (OSError, PermissionError):
            LOGGER.debug("Skipping directory traversal for %s due to access error", base)
            continue


def _candidate_module_path(root: Path, module_parts: Sequence[str]) -> Optional[Tuple[Path, Path]]:
    """Return the module path and sys.path root if the module exists below ``root``."""

    candidate_parts: Iterable[Sequence[str]] = [module_parts]
    if module_parts and root.name == module_parts[0]:
        candidate_parts = [module_parts, module_parts[1:]]

    for parts in candidate_parts:
        if not parts:
            continue
        module_file = root.joinpath(*parts[:-1], f"{parts[-1]}.py")
        if module_file.is_file():
            sys_path_root = root
            prefix_length = len(module_parts) - len(parts)
            for _ in range(prefix_length):
                sys_path_root = sys_path_root.parent
            return module_file, sys_path_root
        package_init = root.joinpath(*parts, "__init__.py")
        if package_init.is_file():
            sys_path_root = root
            prefix_length = len(module_parts) - len(parts)
            for _ in range(prefix_length):
                sys_path_root = sys_path_root.parent
            return package_init, sys_path_root
    return None


def _import_module(module_name: str, dag_file: PathLike) -> ModuleType:
    module_parts = module_name.split(".")

    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name:
            raise

    for search_root in _iter_search_roots(dag_file):
        candidate = _candidate_module_path(search_root, module_parts)
        if not candidate:
            continue
        module_path, sys_path_root = candidate
        sys_path_entry = str(sys_path_root)
        if sys_path_entry not in sys.path:
            sys.path.insert(0, sys_path_entry)
            LOGGER.debug("Added %s to sys.path while loading %s", sys_path_entry, module_name)
        try:
            module = importlib.import_module(module_name)
            return module
        except ModuleNotFoundError as exc:
            if exc.name != module_name:
                raise
            LOGGER.debug("Module %s still not found after adding %s", module_name, sys_path_entry)
            continue
    raise ModuleNotFoundError(
        f"Cannot locate module '{module_name}' relative to DAG '{dag_file}'."
    )


def load_module_main(dag_file: PathLike, module_name: str, attribute: str = "main") -> MainCallable:
    """Return the callable ``attribute`` from ``module_name`` located near ``dag_file``.

    The loader inspects the DAG's directory, its parents, ``$AIRFLOW_HOME``,
    optional ``$PIPELINE_HOME``, and their immediate subdirectories to find the
    requested module. Once located, the module is imported and the callable
    attribute (``main`` by default) is returned.
    """

    module = _import_module(module_name, dag_file)

    try:
        main_callable = getattr(module, attribute)
    except AttributeError as exc:
        raise AttributeError(f"Module '{module_name}' does not define '{attribute}'") from exc

    if not callable(main_callable):
        raise TypeError(f"Attribute '{attribute}' on module '{module_name}' is not callable")

    return cast(MainCallable, main_callable)
