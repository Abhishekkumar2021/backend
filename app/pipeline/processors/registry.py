"""
Universal ETL Processor Registry
--------------------------------

Responsibilities:
✓ Central mapping: processor_type → class
✓ Optional auto-discovery
✓ Plugin support
✓ Validation + metadata
✓ Thread-safe registration
"""

from __future__ import annotations
import importlib
import inspect
import pkgutil
import threading
from typing import Dict, Type

from app.pipeline.processors.base import BaseProcessor
from app.core.exceptions import ValidationError
from app.core.logging import get_logger

logger = get_logger(__name__)


# =============================================================
# INTERNAL STORAGE
# =============================================================
_PROCESSOR_REGISTRY: Dict[str, Type[BaseProcessor]] = {}
_REGISTRY_LOCK = threading.Lock()


def normalize(name: str) -> str:
    """Normalize processor type names."""
    return name.strip().lower().replace(" ", "_")


# =============================================================
# REGISTRATION
# =============================================================
def register_processor(name: str, cls: Type[BaseProcessor]):
    """Register a processor class."""
    if not issubclass(cls, BaseProcessor):
        raise ValidationError(f"{cls.__name__} must inherit BaseProcessor")

    key = normalize(name)

    with _REGISTRY_LOCK:
        _PROCESSOR_REGISTRY[key] = cls

    logger.debug("processor_registered", processor_type=key, class_name=cls.__name__)

def get_processor_class(name: str) -> Type[BaseProcessor]:
    key = normalize(name)
    cls = _PROCESSOR_REGISTRY.get(key)

    if not cls:
        raise ValidationError(f"Processor '{key}' is not registered")

    return cls


# =============================================================
# AUTO-DISCOVERY (OPTIONAL)
# =============================================================
def autodiscover_processors(package="app.pipeline.processors"):
    """Auto register all BaseProcessor subclasses in a package."""
    logger.info("processor_autodiscovery_started", package=package)

    try:
        pkg = importlib.import_module(package)
    except Exception as e:
        logger.error("processor_autodiscovery_failed", error=str(e), package=package)
        return

    for _, module_name, is_pkg in pkgutil.iter_modules(pkg.__path__):
        if is_pkg:
            continue

        module_path = f"{package}.{module_name}"
        try:
            module = importlib.import_module(module_path)
        except Exception:
            continue

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseProcessor) and obj is not BaseProcessor:
                processor_key = normalize(obj.__name__.replace("Processor", ""))
                register_processor(processor_key, obj)

    logger.info("processor_autodiscovery_completed", count=len(_PROCESSOR_REGISTRY))


# =============================================================
# LISTING
# =============================================================
def list_processor_types():
    return {
        key: (cls.__doc__.strip().split("\n")[0] if cls.__doc__ else "No description")
        for key, cls in _PROCESSOR_REGISTRY.items()
    }


def get_registry():
    return dict(_PROCESSOR_REGISTRY)


# =============================================================
# DEFAULT PROCESSORS
# =============================================================
from app.pipeline.processors.noop import NoOpProcessor
from app.pipeline.processors.sql_processor import DuckDBProcessor
from app.pipeline.processors.transform_processors import (
    DropFieldsProcessor,
    RenameFieldsProcessor,
    FilterProcessor,
    AddConstantProcessor,
    ComputeExpressionProcessor,
    TypeCastProcessor,
)

default_processors = {
    "noop": NoOpProcessor,
    "duckdb_sql_transformer": DuckDBProcessor,
    "drop_fields": DropFieldsProcessor,
    "rename_fields": RenameFieldsProcessor,
    "filter_rows": FilterProcessor,
    "add_constant": AddConstantProcessor,
    "compute_expression": ComputeExpressionProcessor,
    "type_cast": TypeCastProcessor,
}

for key, cls in default_processors.items():
    register_processor(key, cls)

logger.info("processor_registry_initialized", total=len(_PROCESSOR_REGISTRY))