import json
import subprocess
from collections.abc import Iterator
from typing import Any, Dict, List

from app.connectors.base import Column, ConnectionTestResult, DataType, Record, Schema, SourceConnector, State, Table
from app.connectors.utils import map_singer_type_to_data_type


class SingerSource(SourceConnector):
    """
    Generic Singer.io Source Connector (Tap).
    Wraps any Singer.io tap to extract data.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.tap_executable = config["tap_executable"]  # e.g., "tap-postgres"
        self.tap_config = config.get("tap_config", {})
        self.tap_catalog = config.get("tap_catalog", {})  # Pre-generated catalog
        self.select_streams = config.get("select_streams", [])

    def connect(self) -> None:
        """Singer taps don't have a persistent connection in the same way.
        Connection is established implicitly during discovery/read.
        """
        pass

    def disconnect(self) -> None:
        """No persistent connection to close."""
        pass

    def _run_tap_command(self, args: List[str], input_json: Dict[str, Any] | None = None) -> Iterator[Dict[str, Any]]:
        """
        Helper to run the tap executable and yield parsed JSON lines.
        """
        process = subprocess.Popen(
            [self.tap_executable] + args,
            stdin=subprocess.PIPE if input_json else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if input_json:
            process.stdin.write(json.dumps(input_json) + "\n")
            process.stdin.flush()
            process.stdin.close()

        for line in process.stdout:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                self.logger.warning(f"Skipping malformed JSON line from tap: {line.strip()}")

        # Check for errors after process completes
        if process.wait() != 0:
            stderr_output = process.stderr.read()
            raise Exception(f"Tap '{self.tap_executable}' failed with exit code {process.returncode}: {stderr_output}")

    def test_connection(self) -> ConnectionTestResult:
        """Test the tap connection by running discovery."""
        try:
            # Run discovery mode to see if it connects without errors
            # Only read a few lines to confirm basic connectivity
            success = False
            for _ in self._run_tap_command(["--config", json.dumps(self.tap_config), "--discover"]):
                success = True
                break # Just need to see if discovery starts successfully
            
            if success:
                return ConnectionTestResult(success=True, message="Successfully tested Singer tap connection (discovery mode).")
            else:
                return ConnectionTestResult(success=False, message="Singer tap discovery produced no output.")
        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Failed to test Singer tap connection: {e}")

    def discover_schema(self) -> Schema:
        """Discover and return schema metadata using Singer's --discover mode."""
        schemas_by_stream = {}
        with self:
            for message in self._run_tap_command(["--config", json.dumps(self.tap_config), "--discover"]):
                if message["type"] == "SCHEMA":
                    stream_name = message["stream"]
                    properties = message["schema"]["properties"]
                    columns = []
                    for prop_name, prop_details in properties.items():
                        # Singer properties can be a list of types, take the first one
                        singer_type = prop_details.get("type")
                        if isinstance(singer_type, list):
                            singer_type = next((t for t in singer_type if t != "null"), "string") # Prefer non-null type
                        
                        data_type = map_singer_type_to_data_type(singer_type)
                        nullable = "null" in prop_details.get("type", []) if isinstance(prop_details.get("type"), list) else False
                        columns.append(Column(name=prop_name, data_type=data_type, nullable=nullable))
                    
                    schemas_by_stream[stream_name] = Table(name=stream_name, columns=columns)
            
            return Schema(tables=list(schemas_by_stream.values()))

    def read(self, stream: str, state: State | None = None, query: str | None = None) -> Iterator[Record]:
        """Read data from the Singer tap using its --catalog and optionally --state."""
        if not self.tap_catalog:
            raise ValueError("Singer tap_catalog must be provided for reading data.")

        # Filter catalog to selected streams if specified
        catalog_to_send = self.tap_catalog.copy()
        if self.select_streams:
            catalog_to_send["streams"] = [
                s for s in catalog_to_send["streams"] if s["stream"] in self.select_streams
            ]

        args = ["--config", json.dumps(self.tap_config), "--catalog", json.dumps(catalog_to_send)]
        if state:
            args.extend(["--state", json.dumps(state.model_dump())]) # Assuming State object can be dumped
        
        with self:
            for message in self._run_tap_command(args):
                if message["type"] == "RECORD":
                    if message["stream"] == stream: # Yield only records for the requested stream
                        yield Record(stream=message["stream"], data=message["record"])
                elif message["type"] == "STATE":
                    # Singer taps typically output STATE messages, but our worker handles state saving.
                    # We can optionally capture it here if we want to return it from read().
                    # For now, let the worker manage based on max cursor value or last record processed.
                    pass 

    def get_record_count(self, stream: str) -> int:
        """Cannot reliably get record count for generic Singer taps without full sync."""
        return -1 # Indicate that count is not available
