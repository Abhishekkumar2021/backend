"""
Singer Source Connector — Unified ETL Framework Version
Compatible with any Singer.io Tap
"""

from __future__ import annotations
import json
import subprocess
from collections.abc import Iterator
from typing import Any, Dict, List

from app.connectors.base import (
    Column,
    ConnectionTestResult,
    DataType,
    Record,
    Schema,
    SourceConnector,
    State,
    Table,
)
from app.connectors.utils import map_singer_type_to_data_type
from app.core.logging import get_logger
from app.schemas.connector_configs import SingerSourceConfig

logger = get_logger(__name__)


class SingerSource(SourceConnector):
    """
    Generic Singer.io Source Connector (Tap)
    ---------------------------------------
    Supports:
    ✓ Running any Singer tap binary
    ✓ --discover for schema introspection
    ✓ --catalog + --state for extraction
    ✓ Inline JSON config or file-based config
    ✓ Structured logging for all subprocess I/O
    """

    def __init__(self, config: SingerSourceConfig):
        super().__init__(config)

        self.tap_executable = config.tap_executable
        self.tap_config = config.tap_config
        self.tap_catalog = config.tap_catalog
        self.select_streams = config.select_streams or []

        logger.debug(
            "singer_source_initialized",
            tap_executable=self.tap_executable,
            has_catalog=bool(self.tap_catalog),
            select_stream_count=len(self.select_streams),
        )

    # ===================================================================
    # No-op connection lifecycle
    # ===================================================================
    def connect(self) -> None:
        logger.debug("singer_connect_noop")

    def disconnect(self) -> None:
        logger.debug("singer_disconnect_noop")

    # ===================================================================
    # Subprocess Execution Helper
    # ===================================================================
    def _run_tap(
        self,
        args: List[str],
        input_payload: Dict[str, Any] | None = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Execute the tap as a subprocess.
        Yield parsed JSON messages line by line.
        """

        cmd = [self.tap_executable] + args

        logger.info(
            "singer_tap_started",
            command_preview=" ".join(cmd)[:300],
            has_input=bool(input_payload),
        )

        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE if input_payload else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        # Write stdin JSON if provided
        if input_payload:
            try:
                payload = json.dumps(input_payload)
                process.stdin.write(payload + "\n")
                process.stdin.flush()
                process.stdin.close()
                logger.debug("singer_tap_stdin_sent")
            except Exception as e:
                logger.error(
                    "singer_tap_stdin_failed",
                    error=str(e),
                    exc_info=True,
                )
                raise

        # Read output stream
        try:
            for line in process.stdout:
                line = line.strip()
                if not line:
                    continue

                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(
                        "singer_invalid_json_output",
                        line_preview=line[:200],
                    )

        finally:
            process.stdout.close()

        # Handle exit status
        exit_code = process.wait()
        stderr_output = process.stderr.read()

        if exit_code != 0:
            logger.error(
                "singer_tap_failed",
                exit_code=exit_code,
                stderr_preview=stderr_output[:400],
            )
            raise RuntimeError(
                f"Tap failed with exit code {exit_code}: {stderr_output}"
            )

        logger.info("singer_tap_completed", exit_code=exit_code)

    # ===================================================================
    # Test Connection
    # ===================================================================
    def test_connection(self) -> ConnectionTestResult:
        logger.info("singer_test_connection_requested")

        try:
            # Attempt discovery
            for _ in self._run_tap(["--config", json.dumps(self.tap_config), "--discover"]):
                break

            logger.info("singer_test_connection_success")
            return ConnectionTestResult(True, "Singer tap discovery successful")

        except Exception as e:
            logger.error(
                "singer_test_connection_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(False, f"Singer tap failed: {e}")

    # ===================================================================
    # Schema Discovery
    # ===================================================================
    def discover_schema(self) -> Schema:
        logger.info("singer_schema_discovery_requested")

        tables: Dict[str, Table] = {}

        try:
            for msg in self._run_tap(["--config", json.dumps(self.tap_config), "--discover"]):
                if msg.get("type") != "SCHEMA":
                    continue

                stream = msg["stream"]
                properties = msg["schema"]["properties"]
                cols: List[Column] = []

                for name, details in properties.items():
                    t = details.get("type", "string")

                    if isinstance(t, list):
                        primary = next((x for x in t if x != "null"), "string")
                        nullable = "null" in t
                    else:
                        primary = t
                        nullable = False

                    cols.append(
                        Column(
                            name=name,
                            data_type=map_singer_type_to_data_type(primary),
                            nullable=nullable,
                        )
                    )

                tables[stream] = Table(name=stream, columns=cols)

            logger.info(
                "singer_schema_discovery_completed",
                stream_count=len(tables),
            )
            return Schema(tables=list(tables.values()))

        except Exception as e:
            logger.error("singer_schema_discovery_failed", error=str(e), exc_info=True)
            return Schema(tables=[])

    # ===================================================================
    # Read (Extract)
    # ===================================================================
    def read(
        self,
        stream: str,
        state: State | None = None,
        query: str | None = None,  # ignored for Singer
    ) -> Iterator[Record]:
        logger.info(
            "singer_read_requested",
            stream=stream,
            has_state=bool(state),
        )

        if not self.tap_catalog:
            raise ValueError("Singer tap_catalog required for reading")

        # Filter catalog to only selected stream(s)
        catalog = self.tap_catalog.copy()

        if self.select_streams:
            catalog["streams"] = [
                s for s in catalog.get("streams", [])
                if s["stream"] in self.select_streams
            ]

            logger.debug(
                "singer_catalog_filtered",
                filtered_streams=len(catalog["streams"]),
            )

        args = [
            "--config", json.dumps(self.tap_config),
            "--catalog", json.dumps(catalog),
        ]

        if state:
            args.extend(["--state", json.dumps(state.model_dump())])

        try:
            for msg in self._run_tap(args):
                msg_type = msg.get("type")

                if msg_type == "RECORD" and msg.get("stream") == stream:
                    yield Record(stream=stream, data=msg["record"])

                elif msg_type == "STATE":
                    logger.debug("singer_state_received")
                    # PipelineEngine handles state externally

        except Exception as e:
            logger.error(
                "singer_read_failed",
                stream=stream,
                error=str(e),
                exc_info=True,
            )
            raise

    # ===================================================================
    # Record Count
    # ===================================================================
    def get_record_count(self, stream: str) -> int:
        """
        Singer taps do not support efficient COUNT.
        Returning -1 tells the UI & analytics to skip RECORD COUNT.
        """
        logger.info("singer_record_count_unavailable", stream=stream)
        return -1
