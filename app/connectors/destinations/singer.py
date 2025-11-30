import json
import subprocess
from collections.abc import Iterator
from typing import Any, Dict, List, TextIO

from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record


class SingerDestination(DestinationConnector):
    """
    Generic Singer.io Destination Connector (Target).
    Wraps any Singer.io target to load data.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.target_executable = config["target_executable"]  # e.g., "target-postgres"
        self.target_config = config.get("target_config", {})
        self.target_catalog = config.get("target_catalog", {}) # Schema of data being sent
        self._target_process: subprocess.Popen | None = None
        self._stdin: TextIO | None = None

    def connect(self) -> None:
        """Establish connection to the Singer target process."""
        try:
            # Start the target process
            self._target_process = subprocess.Popen(
                [self.target_executable, "--config", json.dumps(self.target_config)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self._stdin = self._target_process.stdin

            # Send SCHEMA messages for all streams in the catalog
            if self.target_catalog and "streams" in self.target_catalog:
                for stream_config in self.target_catalog["streams"]:
                    schema_message = {
                        "type": "SCHEMA",
                        "stream": stream_config["stream"],
                        "schema": stream_config["schema"],
                        "key_properties": stream_config.get("key_properties", []),
                    }
                    self._stdin.write(json.dumps(schema_message) + "\n")
                    self._stdin.flush()

        except Exception as e:
            self.disconnect() # Ensure cleanup
            raise Exception(f"Failed to start Singer target '{self.target_executable}': {e}")

    def disconnect(self) -> None:
        """Close connection to the Singer target process and flush any remaining data."""
        if self._stdin:
            self._stdin.close()
            self._stdin = None
        if self._target_process:
            # Send STATE message to indicate end of stream, if applicable
            state_message = {"type": "STATE", "value": {}} # Empty state or last known state
            if self._target_process.stdin: # Ensure stdin is still open
                try:
                    self._target_process.stdin.write(json.dumps(state_message) + "\n")
                    self._target_process.stdin.flush()
                except BrokenPipeError:
                    pass # Target might have already exited
            
            # Wait for the process to finish and check for errors
            stdout, stderr = self._target_process.communicate(timeout=10) # 10s timeout for graceful shutdown
            if self._target_process.returncode != 0:
                print(f"Singer target '{self.target_executable}' exited with error: {stderr}")
            self._target_process = None

    def test_connection(self) -> ConnectionTestResult:
        """Test the target connection by attempting to start it."""
        try:
            with self: # Use context manager to ensure connection/process is closed
                # If connect() succeeded, then the target started up okay.
                return ConnectionTestResult(success=True, message="Successfully tested Singer target connection.")
        except Exception as e:
            return ConnectionTestResult(success=False, message=f"Failed to test Singer target connection: {e}")

    def create_stream(self, stream: str, schema: List[Column]) -> None:
        """Singer targets typically handle stream creation implicitly when SCHEMA message is sent."""
        pass # Schema message is handled in connect()

    def write(self, records: Iterator[Record]) -> int:
        """Write records to the Singer target."""
        if not self._stdin:
            raise Exception("Singer target process not connected.")

        written_count = 0
        with self: # Ensure context manager is used for self._stdin.write
            for record in records:
                message = {
                    "type": "RECORD",
                    "stream": record.stream,
                    "record": record.data,
                }
                try:
                    self._stdin.write(json.dumps(message) + "\n")
                    written_count += 1
                except BrokenPipeError:
                    raise Exception("Singer target process pipe is broken, likely exited unexpectedly.")
            self._stdin.flush() # Ensure all records are sent before disconnecting
        return written_count
