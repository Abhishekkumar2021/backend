"""Registry for Data Processors.

Centralizes the mapping between processor type strings (stored in DB)
and the actual Python classes that implement them.
"""
from typing import Dict, Type, Optional
from app.pipeline.processors.base import BaseProcessor
from app.pipeline.processors.noop import NoOpProcessor
from app.pipeline.processors.sql import DuckDBProcessor
from app.pipeline.processors.transformer import ExampleTransformerProcessor

# Map of processor_type string -> Processor Class
_PROCESSOR_REGISTRY: Dict[str, Type[BaseProcessor]] = {
    "noop": NoOpProcessor,
    "example_transformer": ExampleTransformerProcessor,
    "duckdb_sql_transformer": DuckDBProcessor,
}

def get_processor_class(processor_type: str) -> Optional[Type[BaseProcessor]]:
    """Get processor class by type name."""
    return _PROCESSOR_REGISTRY.get(processor_type)

def register_processor(processor_type: str, processor_class: Type[BaseProcessor]):
    """Register a new processor type."""
    _PROCESSOR_REGISTRY[processor_type] = processor_class

def list_processor_types() -> Dict[str, str]:
    """List available processor types and their descriptions."""
    return {
        k: v.__doc__ or "No description" 
        for k, v in _PROCESSOR_REGISTRY.items()
    }
