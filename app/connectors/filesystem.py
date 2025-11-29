"""
File System Destination Connector
Writes data to local filesystem in various formats: Parquet, JSON, CSV, Avro
"""
import os
import json
import csv
from pathlib import Path
from typing import Iterator, Dict, Any, List, Optional
from datetime import datetime
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import avro.schema
import avro.datafile
import avro.io

from app.connectors.base import (
    DestinationConnector,
    ConnectionTestResult,
    Column,
    Record,
    DataType
)

logger = logging.getLogger(__name__)


class FileSystemDestination(DestinationConnector):
    """
    File System destination connector
    Supports Parquet, JSON, CSV, Avro formats
    """
    
    SUPPORTED_FORMATS = ['parquet', 'json', 'csv', 'jsonl', 'avro']
    
    # DataType to Avro type mapping
    AVRO_TYPE_MAPPING = {
        DataType.INTEGER: 'long',
        DataType.FLOAT: 'double',
        DataType.STRING: 'string',
        DataType.BOOLEAN: 'boolean',
        DataType.DATE: 'string',
        DataType.DATETIME: 'string',
        DataType.JSON: 'string',
        DataType.BINARY: 'bytes',
        DataType.NULL: ['null', 'string']
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize FileSystem destination connector
        
        Config format:
        {
            "base_path": "/path/to/output/directory",
            "format": "parquet",  # Options: parquet, json, csv, jsonl, avro
            "partition_by": None,  # Optional: column name to partition by
            "compression": "snappy",  # For parquet: snappy, gzip, none
            "overwrite": false  # If true, overwrite existing files
        }
        """
        super().__init__(config)
        self.base_path = Path(config['base_path'])
        self.format = config.get('format', 'parquet').lower()
        self.partition_by = config.get('partition_by')
        self.compression = config.get('compression', 'snappy')
        self.overwrite = config.get('overwrite', False)
        
        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}. Must be one of {self.SUPPORTED_FORMATS}")
        
        # Ensure base path exists
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def test_connection(self) -> ConnectionTestResult:
        """Test file system write permissions"""
        try:
            # Check if base path exists and is writable
            if not self.base_path.exists():
                return ConnectionTestResult(
                    success=False,
                    message=f"Base path does not exist: {self.base_path}"
                )
            
            if not os.access(self.base_path, os.W_OK):
                return ConnectionTestResult(
                    success=False,
                    message=f"No write permission for: {self.base_path}"
                )
            
            # Try creating a test file
            test_file = self.base_path / ".test_write"
            try:
                test_file.write_text("test")
                test_file.unlink()
            except Exception as e:
                return ConnectionTestResult(
                    success=False,
                    message=f"Cannot write to directory: {str(e)}"
                )
            
            return ConnectionTestResult(
                success=True,
                message=f"Write access confirmed",
                metadata={
                    "base_path": str(self.base_path),
                    "format": self.format,
                    "compression": self.compression
                }
            )
            
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"Test failed: {str(e)}"
            )
    
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """
        Create directory for stream (table)
        
        Args:
            stream: Table/stream name
            schema: List of Column objects
        """
        stream_path = self.base_path / stream
        stream_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory for stream: {stream_path}")
    
    def write(self, records: Iterator[Record]) -> int:
        """
        Write records to file system
        
        Args:
            records: Iterator of Record objects
            
        Returns:
            Number of records written
        """
        total_written = 0
        
        # Group records by stream
        stream_records: Dict[str, List[Dict[str, Any]]] = {}
        
        for record in records:
            if record.stream not in stream_records:
                stream_records[record.stream] = []
            stream_records[record.stream].append(record.data)
        
        # Write each stream
        for stream, data in stream_records.items():
            if not data:
                continue
            
            count = self._write_stream(stream, data)
            total_written += count
            logger.info(f"Wrote {count} records to {stream}")
        
        return total_written
    
    def _write_stream(self, stream: str, data: List[Dict[str, Any]]) -> int:
        """Write data for a single stream"""
        stream_path = self.base_path / stream
        stream_path.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{stream}_{timestamp}.{self._get_file_extension()}"
        file_path = stream_path / filename
        
        # Check if file exists and overwrite is disabled
        if file_path.exists() and not self.overwrite:
            # Append counter to filename
            counter = 1
            while file_path.exists():
                filename = f"{stream}_{timestamp}_{counter}.{self._get_file_extension()}"
                file_path = stream_path / filename
                counter += 1
        
        # Write data based on format
        if self.format == 'parquet':
            return self._write_parquet(file_path, data)
        elif self.format == 'json':
            return self._write_json(file_path, data)
        elif self.format == 'jsonl':
            return self._write_jsonl(file_path, data)
        elif self.format == 'csv':
            return self._write_csv(file_path, data)
        elif self.format == 'avro':
            return self._write_avro(file_path, data)
        else:
            raise ValueError(f"Unsupported format: {self.format}")
    
    def _get_file_extension(self) -> str:
        """Get file extension for format"""
        extensions = {
            'parquet': 'parquet',
            'json': 'json',
            'jsonl': 'jsonl',
            'csv': 'csv',
            'avro': 'avro'
        }
        return extensions.get(self.format, 'dat')
    
    def _write_parquet(self, file_path: Path, data: List[Dict[str, Any]]) -> int:
        """Write data to Parquet format"""
        df = pd.DataFrame(data)
        
        # Convert to PyArrow Table for better control
        table = pa.Table.from_pandas(df)
        
        # Write with compression
        pq.write_table(
            table,
            file_path,
            compression=self.compression if self.compression != 'none' else None
        )
        
        logger.info(f"Wrote Parquet file: {file_path} ({len(data)} records)")
        return len(data)
    
    def _write_json(self, file_path: Path, data: List[Dict[str, Any]]) -> int:
        """Write data to JSON format (array of objects)"""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"Wrote JSON file: {file_path} ({len(data)} records)")
        return len(data)
    
    def _write_jsonl(self, file_path: Path, data: List[Dict[str, Any]]) -> int:
        """Write data to JSON Lines format (one JSON object per line)"""
        with open(file_path, 'w', encoding='utf-8') as f:
            for record in data:
                json.dump(record, f, default=str)
                f.write('\n')
        
        logger.info(f"Wrote JSONL file: {file_path} ({len(data)} records)")
        return len(data)
    
    def _write_csv(self, file_path: Path, data: List[Dict[str, Any]]) -> int:
        """Write data to CSV format"""
        if not data:
            return 0
        
        # Get all unique keys across all records
        fieldnames = set()
        for record in data:
            fieldnames.update(record.keys())
        fieldnames = sorted(fieldnames)
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        logger.info(f"Wrote CSV file: {file_path} ({len(data)} records)")
        return len(data)
    
    def _write_avro(self, file_path: Path, data: List[Dict[str, Any]]) -> int:
        """Write data to Avro format"""
        if not data:
            return 0
        
        # Infer schema from first record
        avro_schema = self._infer_avro_schema(data[0])
        schema = avro.schema.parse(json.dumps(avro_schema))
        
        with open(file_path, 'wb') as f:
            writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
            
            for record in data:
                # Convert values to Avro-compatible types
                avro_record = self._convert_to_avro(record)
                writer.append(avro_record)
            
            writer.close()
        
        logger.info(f"Wrote Avro file: {file_path} ({len(data)} records)")
        return len(data)
    
    def _infer_avro_schema(self, sample_record: Dict[str, Any]) -> Dict[str, Any]:
        """Infer Avro schema from a sample record"""
        fields = []
        
        for key, value in sample_record.items():
            avro_type = self._python_to_avro_type(value)
            fields.append({
                "name": key,
                "type": ["null", avro_type] if value is None else avro_type
            })
        
        return {
            "type": "record",
            "name": "Record",
            "fields": fields
        }
    
    def _python_to_avro_type(self, value: Any) -> str:
        """Convert Python value to Avro type"""
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "long"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, (str, datetime)):
            return "string"
        elif isinstance(value, bytes):
            return "bytes"
        elif isinstance(value, (list, dict)):
            return "string"  # Serialize complex types as JSON strings
        else:
            return "string"
    
    def _convert_to_avro(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert record to Avro-compatible format"""
        avro_record = {}
        
        for key, value in record.items():
            if value is None:
                avro_record[key] = None
            elif isinstance(value, datetime):
                avro_record[key] = value.isoformat()
            elif isinstance(value, (list, dict)):
                avro_record[key] = json.dumps(value, default=str)
            else:
                avro_record[key] = value
        
        return avro_record


class FileSystemSource(DestinationConnector):
    """
    File System source connector
    Reads data from local files: Parquet, JSON, CSV, JSON Lines
    """
    
    SUPPORTED_FORMATS = ['parquet', 'json', 'csv', 'jsonl']
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize FileSystem source connector
        
        Config format:
        {
            "file_path": "/path/to/file.parquet",  # or directory
            "format": "parquet",  # Auto-detect if not specified
            "glob_pattern": "*.parquet"  # Optional: for directory reading
        }
        """
        super().__init__(config)
        self.file_path = Path(config['file_path'])
        self.format = config.get('format', self._detect_format()).lower()
        self.glob_pattern = config.get('glob_pattern', '*')
        
        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}")
    
    def _detect_format(self) -> str:
        """Auto-detect file format from extension"""
        if self.file_path.is_file():
            extension = self.file_path.suffix.lower().lstrip('.')
            if extension in self.SUPPORTED_FORMATS:
                return extension
        return 'parquet'  # Default
    
    def test_connection(self) -> ConnectionTestResult:
        """Test file system read access"""
        try:
            if not self.file_path.exists():
                return ConnectionTestResult(
                    success=False,
                    message=f"Path does not exist: {self.file_path}"
                )
            
            if not os.access(self.file_path, os.R_OK):
                return ConnectionTestResult(
                    success=False,
                    message=f"No read permission for: {self.file_path}"
                )
            
            # Count files
            if self.file_path.is_dir():
                files = list(self.file_path.glob(self.glob_pattern))
                file_count = len(files)
            else:
                file_count = 1
            
            return ConnectionTestResult(
                success=True,
                message=f"Read access confirmed",
                metadata={
                    "path": str(self.file_path),
                    "format": self.format,
                    "file_count": file_count
                }
            )
            
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"Test failed: {str(e)}"
            )
    
    def read(self, stream: str, **kwargs) -> Iterator[Record]:
        """
        Read data from file(s)
        
        Args:
            stream: File name or pattern
            
        Yields:
            Record objects
        """
        files = self._get_files()
        
        for file_path in files:
            logger.info(f"Reading file: {file_path}")
            
            if self.format == 'parquet':
                yield from self._read_parquet(file_path, stream)
            elif self.format == 'json':
                yield from self._read_json(file_path, stream)
            elif self.format == 'jsonl':
                yield from self._read_jsonl(file_path, stream)
            elif self.format == 'csv':
                yield from self._read_csv(file_path, stream)
    
    def _get_files(self) -> List[Path]:
        """Get list of files to read"""
        if self.file_path.is_file():
            return [self.file_path]
        elif self.file_path.is_dir():
            return sorted(self.file_path.glob(self.glob_pattern))
        else:
            return []
    
    def _read_parquet(self, file_path: Path, stream: str) -> Iterator[Record]:
        """Read Parquet file"""
        table = pq.read_table(file_path)
        df = table.to_pandas()
        
        for _, row in df.iterrows():
            yield Record(
                stream=stream,
                data=row.to_dict()
            )
    
    def _read_json(self, file_path: Path, stream: str) -> Iterator[Record]:
        """Read JSON file (array of objects)"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            for item in data:
                yield Record(stream=stream, data=item)
        else:
            yield Record(stream=stream, data=data)
    
    def _read_jsonl(self, file_path: Path, stream: str) -> Iterator[Record]:
        """Read JSON Lines file"""
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    yield Record(stream=stream, data=data)
    
    def _read_csv(self, file_path: Path, stream: str) -> Iterator[Record]:
        """Read CSV file"""
        df = pd.read_csv(file_path)
        
        for _, row in df.iterrows():
            yield Record(
                stream=stream,
                data=row.to_dict()
            )
    
    def write(self, records: Iterator[Record]) -> int:
        """Not implemented for source connector"""
        raise NotImplementedError("FileSystemSource is a source connector, not a destination")
    
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """Not implemented for source connector"""
        raise NotImplementedError("FileSystemSource is a source connector, not a destination")