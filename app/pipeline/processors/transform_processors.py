"""
Simple built-in ETL transformers.
All processors are kept in one file for simplicity.
"""

from typing import Any, List
from app.pipeline.processors.base import BaseProcessor
from app.connectors.base import Record


class DropFieldsProcessor(BaseProcessor):
    """Remove specific fields from each record."""

    def __init__(self, fields: List[str]):
        self.fields = fields

    def transform_record(self, record: Record):
        for f in self.fields:
            record.data.pop(f, None)
        return record


class RenameFieldsProcessor(BaseProcessor):
    """Rename fields using a mapping: {'old': 'new'}."""

    def __init__(self, mapping: dict[str, str]):
        self.mapping = mapping

    def transform_record(self, record: Record):
        for old, new in self.mapping.items():
            if old in record.data:
                record.data[new] = record.data.pop(old)
        return record


class FilterProcessor(BaseProcessor):
    """Filter records based on a lambda condition."""

    def __init__(self, predicate):
        self.predicate = predicate

    def transform_record(self, record: Record):
        return record if self.predicate(record.data) else None


class AddConstantProcessor(BaseProcessor):
    """Add a constant field to each record."""

    def __init__(self, field: str, value: Any):
        self.field = field
        self.value = value

    def transform_record(self, record: Record):
        record.data[self.field] = self.value
        return record


class ComputeExpressionProcessor(BaseProcessor):
    """Compute a new field using a Python expression."""

    def __init__(self, field: str, expression: str):
        self.field = field
        self.expression = expression

    def transform_record(self, record: Record):
        local = dict(record.data)
        try:
            result = eval(self.expression, {"__builtins__": {}}, local)
            record.data[self.field] = result
        except Exception:
            pass
        return record


class TypeCastProcessor(BaseProcessor):
    """Cast fields to types: int, float, bool, str."""

    def __init__(self, casts: dict[str, str]):
        self.casts = casts

    def transform_record(self, record: Record):
        for field, type_name in self.casts.items():
            val = record.data.get(field)
            if val is None:
                continue
            try:
                if type_name == "int":
                    record.data[field] = int(val)
                elif type_name == "float":
                    record.data[field] = float(val)
                elif type_name == "bool":
                    record.data[field] = bool(val)
                elif type_name == "str":
                    record.data[field] = str(val)
            except Exception:
                continue
        return record
