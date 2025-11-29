"""
AWS S3 Source and Destination Connectors
Handles data reading/writing to S3 in various formats
"""
import io
import json
from typing import Iterator, Dict, Any, List, Optional
from datetime import datetime
import logging

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.connectors.base import (
    SourceConnector,
    DestinationConnector,
    ConnectionTestResult,
    Column,
    Record,
    State,
    DataType
)

logger = logging.getLogger(__name__)


class S3Destination(DestinationConnector):
    """
    S3 destination connector
    Writes data to AWS S3 in Parquet, JSON, CSV formats
    """
    
    SUPPORTED_FORMATS = ['parquet', 'json', 'csv', 'jsonl']
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize S3 destination connector
        
        Config format:
        {
            "bucket": "my-bucket",
            "prefix": "data/",  # Optional: S3 key prefix
            "format": "parquet",
            "aws_access_key_id": "...",
            "aws_secret_access_key": "...",
            "region": "us-east-1",
            "compression": "snappy"  # For parquet
        }
        """
        super().__init__(config)
        self.bucket = config['bucket']
        self.prefix = config.get('prefix', '').rstrip('/')
        self.format = config.get('format', 'parquet').lower()
        self.compression = config.get('compression', 'snappy')
        
        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}")
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.get('aws_access_key_id'),
            aws_secret_access_key=config.get('aws_secret_access_key'),
            region_name=config.get('region', 'us-east-1')
        )
    
    def test_connection(self) -> ConnectionTestResult:
        """Test S3 connection and write permissions"""
        try:
            # Check if bucket exists
            try:
                self.s3_client.head_bucket(Bucket=self.bucket)
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    return ConnectionTestResult(
                        success=False,
                        message=f"Bucket '{self.bucket}' does not exist"
                    )
                elif error_code == '403':
                    return ConnectionTestResult(
                        success=False,
                        message=f"No access to bucket '{self.bucket}'"
                    )
                raise
            
            # Test write permission with a small test object
            test_key = f"{self.prefix}/.test_write" if self.prefix else ".test_write"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=test_key,
                Body=b"test"
            )
            
            # Clean up test object
            self.s3_client.delete_object(
                Bucket=self.bucket,
                Key=test_key
            )
            
            return ConnectionTestResult(
                success=True,
                message="S3 access confirmed",
                metadata={
                    "bucket": self.bucket,
                    "prefix": self.prefix,
                    "format": self.format
                }
            )
            
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"S3 connection failed: {str(e)}"
            )
    
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """
        No need to create "stream" in S3 (just a key prefix)
        """
        logger.info(f"Stream '{stream}' will be created as S3 key prefix")
    
    def write(self, records: Iterator[Record]) -> int:
        """
        Write records to S3
        
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
            logger.info(f"Wrote {count} records to s3://{self.bucket}/{self.prefix}/{stream}")
        
        return total_written
    
    def _write_stream(self, stream: str, data: List[Dict[str, Any]]) -> int:
        """Write data for a single stream to S3"""
        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extension = self._get_file_extension()
        
        if self.prefix:
            s3_key = f"{self.prefix}/{stream}/{stream}_{timestamp}.{extension}"
        else:
            s3_key = f"{stream}/{stream}_{timestamp}.{extension}"
        
        # Write data based on format
        if self.format == 'parquet':
            return self._write_parquet(s3_key, data)
        elif self.format == 'json':
            return self._write_json(s3_key, data)
        elif self.format == 'jsonl':
            return self._write_jsonl(s3_key, data)
        elif self.format == 'csv':
            return self._write_csv(s3_key, data)
        else:
            raise ValueError(f"Unsupported format: {self.format}")
    
    def _get_file_extension(self) -> str:
        """Get file extension for format"""
        return {
            'parquet': 'parquet',
            'json': 'json',
            'jsonl': 'jsonl',
            'csv': 'csv'
        }.get(self.format, 'dat')
    
    def _write_parquet(self, s3_key: str, data: List[Dict[str, Any]]) -> int:
        """Write data to S3 in Parquet format"""
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)
        
        # Write to buffer
        buffer = io.BytesIO()
        pq.write_table(
            table,
            buffer,
            compression=self.compression if self.compression != 'none' else None
        )
        
        # Upload to S3
        buffer.seek(0)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=buffer.getvalue()
        )
        
        logger.info(f"Wrote Parquet to s3://{self.bucket}/{s3_key} ({len(data)} records)")
        return len(data)
    
    def _write_json(self, s3_key: str, data: List[Dict[str, Any]]) -> int:
        """Write data to S3 in JSON format"""
        json_data = json.dumps(data, indent=2, default=str)
        
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=json_data.encode('utf-8')
        )
        
        logger.info(f"Wrote JSON to s3://{self.bucket}/{s3_key} ({len(data)} records)")
        return len(data)
    
    def _write_jsonl(self, s3_key: str, data: List[Dict[str, Any]]) -> int:
        """Write data to S3 in JSON Lines format"""
        jsonl_data = '\n'.join(json.dumps(record, default=str) for record in data)
        
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=jsonl_data.encode('utf-8')
        )
        
        logger.info(f"Wrote JSONL to s3://{self.bucket}/{s3_key} ({len(data)} records)")
        return len(data)
    
    def _write_csv(self, s3_key: str, data: List[Dict[str, Any]]) -> int:
        """Write data to S3 in CSV format"""
        if not data:
            return 0
        
        df = pd.DataFrame(data)
        csv_data = df.to_csv(index=False)
        
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=csv_data.encode('utf-8')
        )
        
        logger.info(f"Wrote CSV to s3://{self.bucket}/{s3_key} ({len(data)} records)")
        return len(data)


class S3Source(SourceConnector):
    """
    S3 source connector
    Reads data from S3 in Parquet, JSON, CSV formats
    """
    
    SUPPORTED_FORMATS = ['parquet', 'json', 'csv', 'jsonl']
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize S3 source connector
        
        Config format:
        {
            "bucket": "my-bucket",
            "prefix": "data/",  # Optional: S3 key prefix or specific file
            "format": "parquet",
            "aws_access_key_id": "...",
            "aws_secret_access_key": "...",
            "region": "us-east-1"
        }
        """
        super().__init__(config)
        self.bucket = config['bucket']
        self.prefix = config.get('prefix', '').rstrip('/')
        self.format = config.get('format', 'parquet').lower()
        
        if self.format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {self.format}")
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.get('aws_access_key_id'),
            aws_secret_access_key=config.get('aws_secret_access_key'),
            region_name=config.get('region', 'us-east-1')
        )
    
    def test_connection(self) -> ConnectionTestResult:
        """Test S3 connection and read permissions"""
        try:
            # Check if bucket exists
            try:
                self.s3_client.head_bucket(Bucket=self.bucket)
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    return ConnectionTestResult(
                        success=False,
                        message=f"Bucket '{self.bucket}' does not exist"
                    )
                elif error_code == '403':
                    return ConnectionTestResult(
                        success=False,
                        message=f"No access to bucket '{self.bucket}'"
                    )
                raise
            
            # List objects in prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix,
                MaxKeys=10
            )
            
            file_count = response.get('KeyCount', 0)
            
            return ConnectionTestResult(
                success=True,
                message="S3 access confirmed",
                metadata={
                    "bucket": self.bucket,
                    "prefix": self.prefix,
                    "file_count": file_count
                }
            )
            
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"S3 connection failed: {str(e)}"
            )
    
    def discover_schema(self) -> None:
        """Schema discovery not implemented for S3"""
        raise NotImplementedError("Schema discovery not available for S3 source")
    
    def read(
        self,
        stream: str,
        state: Optional[State] = None,
        query: Optional[str] = None
    ) -> Iterator[Record]:
        """
        Read data from S3
        
        Args:
            stream: S3 key pattern or specific file
            state: Not used for S3
            query: Not used for S3
            
        Yields:
            Record objects
        """
        # List all objects matching the prefix/stream
        objects = self._list_objects(stream)
        
        for obj_key in objects:
            logger.info(f"Reading s3://{self.bucket}/{obj_key}")
            yield from self._read_object(obj_key, stream)
    
    def _list_objects(self, stream: str) -> List[str]:
        """List S3 objects matching the stream pattern"""
        search_prefix = f"{self.prefix}/{stream}" if self.prefix else stream
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket, Prefix=search_prefix)
        
        objects = []
        for page in pages:
            for obj in page.get('Contents', []):
                objects.append(obj['Key'])
        
        return objects
    
    def _read_object(self, s3_key: str, stream: str) -> Iterator[Record]:
        """Read a single S3 object"""
        # Download object
        response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
        content = response['Body'].read()
        
        if self.format == 'parquet':
            yield from self._read_parquet(content, stream)
        elif self.format == 'json':
            yield from self._read_json(content, stream)
        elif self.format == 'jsonl':
            yield from self._read_jsonl(content, stream)
        elif self.format == 'csv':
            yield from self._read_csv(content, stream)
    
    def _read_parquet(self, content: bytes, stream: str) -> Iterator[Record]:
        """Read Parquet content"""
        buffer = io.BytesIO(content)
        table = pq.read_table(buffer)
        df = table.to_pandas()
        
        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())
    
    def _read_json(self, content: bytes, stream: str) -> Iterator[Record]:
        """Read JSON content"""
        data = json.loads(content.decode('utf-8'))
        
        if isinstance(data, list):
            for item in data:
                yield Record(stream=stream, data=item)
        else:
            yield Record(stream=stream, data=data)
    
    def _read_jsonl(self, content: bytes, stream: str) -> Iterator[Record]:
        """Read JSON Lines content"""
        lines = content.decode('utf-8').split('\n')
        
        for line in lines:
            if line.strip():
                data = json.loads(line)
                yield Record(stream=stream, data=data)
    
    def _read_csv(self, content: bytes, stream: str) -> Iterator[Record]:
        """Read CSV content"""
        buffer = io.BytesIO(content)
        df = pd.read_csv(buffer)
        
        for _, row in df.iterrows():
            yield Record(stream=stream, data=row.to_dict())
    
    def get_record_count(self, stream: str) -> int:
        """Get record count (not efficiently implemented for S3)"""
        logger.warning("get_record_count is not optimized for S3 - reading all data")
        count = sum(1 for _ in self.read(stream))
        return count