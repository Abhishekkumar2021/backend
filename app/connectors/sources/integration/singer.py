import json
import subprocess
from collections.abc import Iterator
from typing import Any, Dict, List

from app.connectors.base import (
    Column, ConnectionTestResult, DataType,
    Record, Schema, SourceConnector, State, Table
)
from app.connectors.utils import map_singer_type_to_data_type
from app.core.logging import get_logger
from app.schemas.connector_configs import SingerSourceConfig

logger = get_logger(__name__)


class SingerSource(SourceConnector):
    """
    Generic Singer.io Source Connector (Tap).
    Wraps any Singer.io tap to extract data.
    """

    def __init__(self, config: SingerSourceConfig):
        super().__init__(config)

        self.tap_executable = config.tap_executable
        self.tap_config = config.tap_config
        self.tap_catalog = config.tap_catalog
        self.select_streams = config.select_streams or []

        # Give this connector its own logger reference
        self.logger = logger

        self.logger.debug(
            "singer_source_initialized",
            tap_executable=self.tap_executable,
            select_streams=len(self.select_streams),
            has_catalog=bool(self.tap_catalog),
        )

    # ===================================================================
    # Connection
    # ===================================================================
    def connect(self) -> None:
        """Singer taps don't maintain persistent connections."""
        self.logger.debug("singer_connect_noop")

    def disconnect(self) -> None:
        self.logger.debug("singer_disconnect_noop")

    # ===================================================================
    # Tap Command Execution
    # ===================================================================
    def _run_tap_command(
        self,
        args: List[str],
        input_json: Dict[str, Any] | None = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Helper to run the tap executable and yield parsed JSON lines.
        """

        full_cmd = [self.tap_executable] + args

        self.logger.info(
            "singer_tap_command_started",
            tap=self.tap_executable,
            args_preview=str(full_cmd)[:200],
            has_input=bool(input_json),
        )

        process = subprocess.Popen(
            full_cmd,
            stdin=subprocess.PIPE if input_json else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Send input JSON if needed
        if input_json:
            try:
                process.stdin.write(json.dumps(input_json) + "\n")
                process.stdin.flush()
                process.stdin.close()
                self.logger.debug("singer_tap_input_sent")
            except Exception as e:
                self.logger.error(
                    "singer_tap_input_failed",
                    error=str(e),
                    exc_info=True,
                )
                raise

        # Process output lines
        for line in process.stdout:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                self.logger.warning(
                    "singer_tap_malformed_json",
                    line_preview=line.strip()[:200],
                )

        # Check for failures after completion
        exit_code = process.wait()
        if exit_code != 0:
            stderr_output = process.stderr.read()

            self.logger.error(
                "singer_tap_process_failed",
                tap=self.tap_executable,
                exit_code=exit_code,
                stderr_preview=stderr_output[:500],
            )

            raise Exception(
                f"Tap '{self.tap_executable}' failed with exit code {exit_code}: {stderr_output}"
            )

        self.logger.info(
            "singer_tap_command_completed",
            tap=self.tap_executable,
            exit_code=exit_code,
        )

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        self.logger.info(
            "singer_test_connection_requested",
            tap=self.tap_executable,
        )

        try:
            success = False

            for _ in self._run_tap_command(
                ["--config", json.dumps(self.tap_config), "--discover"]
            ):
                success = True
                break

            if success:
                self.logger.info(
                    "singer_test_connection_success",
                    tap=self.tap_executable,
                )
                return ConnectionTestResult(
                    success=True,
                    message="Successfully tested Singer tap connection (discovery mode).",
                )

            self.logger.warning(
                "singer_test_no_output",
                tap=self.tap_executable,
            )
            return ConnectionTestResult(
                success=False,
                message="Singer tap discovery produced no output.",
            )

        except Exception as e:
            self.logger.error(
                "singer_test_connection_failed",
                tap=self.tap_executable,
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(
                success=False,
                message=f"Failed to test Singer tap connection: {e}",
            )

    # ===================================================================
    # Schema Discovery
    # ===================================================================
    def discover_schema(self) -> Schema:
        self.logger.info(
            "singer_schema_discovery_requested",
            tap=self.tap_executable,
        )

        schemas_by_stream = {}

        with self:
            try:
                for message in self._run_tap_command(
                    ["--config", json.dumps(self.tap_config), "--discover"]
                ):
                    if message.get("type") != "SCHEMA":
                        continue

                    stream_name = message["stream"]
                    properties = message["schema"]["properties"]
                    columns = []

                    for prop_name, prop_details in properties.items():
                        type_value = prop_details.get("type")

                        # Singer can present type as list
                        if isinstance(type_value, list):
                            primary_type = next(
                                (t for t in type_value if t != "null"),
                                "string",
                            )
                            nullable = "null" in type_value
                        else:
                            primary_type = type_value
                            nullable = False

                        mapped_type = map_singer_type_to_data_type(primary_type)

                        columns.append(
                            Column(
                                name=prop_name,
                                data_type=mapped_type,
                                nullable=nullable,
                            )
                        )

                    schemas_by_stream[stream_name] = Table(
                        name=stream_name,
                        columns=columns,
                    )

                self.logger.info(
                    "singer_schema_discovery_completed",
                    stream_count=len(schemas_by_stream),
                )

                return Schema(tables=list(schemas_by_stream.values()))

            except Exception as e:
                self.logger.error(
                    "singer_schema_discovery_failed",
                    error=str(e),
                    exc_info=True,
                )
                return Schema(tables=[])

    # ===================================================================
    # Data Read
    # ===================================================================
    def read(
        self,
        stream: str,
        state: State | None = None,
        query: str | None = None,
    ) -> Iterator[Record]:
        self.logger.info(
            "singer_read_requested",
            tap=self.tap_executable,
            stream=stream,
            state_provided=bool(state),
        )

        if not self.tap_catalog:
            self.logger.error(
                "singer_read_missing_catalog",
                tap=self.tap_executable,
            )
            raise ValueError("Singer tap_catalog must be provided for reading data.")

        # Build catalog for stream filtering
        catalog_to_send = self.tap_catalog.copy()
        if self.select_streams:
            filtered = [
                s for s in catalog_to_send.get("streams", [])
                if s["stream"] in self.select_streams
            ]
            catalog_to_send["streams"] = filtered

            self.logger.debug(
                "singer_catalog_filtered",
                stream_count=len(filtered),
                selected_streams_count=len(self.select_streams),
            )

        args = ["--config", json.dumps(self.tap_config),
                "--catalog", json.dumps(catalog_to_send)]

        if state:
            args.extend(["--state", json.dumps(state.model_dump())])

        with self:
            try:
                for message in self._run_tap_command(args):
                    msg_type = message.get("type")

                    if msg_type == "RECORD" and message.get("stream") == stream:
                        self.logger.debug(
                            "singer_record_received",
                            stream=stream,
                        )
                        yield Record(stream=stream, data=message["record"])

                    elif msg_type == "STATE":
                        # Worker manages state externally
                        self.logger.debug("singer_state_message_received")

            except Exception as e:
                self.logger.error(
                    "singer_read_failed",
                    tap=self.tap_executable,
                    stream=stream,
                    error=str(e),
                    exc_info=True,
                )
                raise

    # ===================================================================
    # Record Count
    # ===================================================================
    def get_record_count(self, stream: str) -> int:
        self.logger.info(
            "singer_record_count_unavailable",
            stream=stream,
        )
        return -1
