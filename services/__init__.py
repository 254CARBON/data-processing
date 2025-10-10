"\"\"\"Service implementations for the data processing domain.\"\"\""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parent
_NORMALIZE_TICKS_DIR = _PACKAGE_ROOT / "normalize-ticks"
_CACHE_UPDATER_DIR = _PACKAGE_ROOT / "cache-updater"

if (
    "services.normalize_ticks" not in sys.modules
    and _NORMALIZE_TICKS_DIR.is_dir()
    and (_NORMALIZE_TICKS_DIR / "__init__.py").is_file()
):
    spec = importlib.util.spec_from_file_location(
        "services.normalize_ticks",
        (_NORMALIZE_TICKS_DIR / "__init__.py").as_posix(),
        submodule_search_locations=[str(_NORMALIZE_TICKS_DIR)],
    )
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        setattr(sys.modules[__name__], "normalize_ticks", module)

if (
    "services.cache_updater" not in sys.modules
    and _CACHE_UPDATER_DIR.is_dir()
    and (_CACHE_UPDATER_DIR / "__init__.py").is_file()
):
    spec = importlib.util.spec_from_file_location(
        "services.cache_updater",
        (_CACHE_UPDATER_DIR / "__init__.py").as_posix(),
        submodule_search_locations=[str(_CACHE_UPDATER_DIR)],
    )
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        setattr(sys.modules[__name__], "cache_updater", module)
