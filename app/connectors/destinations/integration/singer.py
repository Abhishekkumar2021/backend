import json
import subprocess
from collections.abc import Iterator
from typing import Any, Dict, List, TextIO

from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record
from app.core.logging import get_logger

logger = get_logger(__name__)


class SingerDestination(DestinationConnector):
    """
    Generic Singer.io Destination Connector (Target).
    Wraps any Singer.io target executable.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.target_executable = config["target_executable"]
        self.target_config = config.get("target_config", {})
        self.target_catalog = config.get("target_catalog", {})
        self._target_process: subprocess.Popen | None = None
        self._stdin: TextIO | None = None

        logger.debug(
            "singer_destination_initialized",
            target_executable=self.target_executable,
            has_catalog=bool(self.target_catalog),
        )

    # ------------------------------------------------------------------
    # Connect
    # ------------------------------------------------------------------
    def connect(self) -> None:
        try:
            logger.info("singer_target_process_starting", executable=self.target_executable)

            self._target_process = subprocess.Popen(
                [self.target_executable, "--config", json.dumps(self.target_config)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            self._stdin = self._target_process.stdin

            logger.info("singer_target_process_started")

            # Send SCHEMA messages from catalog
            if self.target_catalog and "streams" in self.target_catalog:
                for config in self.target_catalog["streams"]:
                    schema_message = {
                        "type": "SCHEMA",
                        "stream": config["stream"],
                        "schema": config["schema"],
                        "key_properties": config.get("key_properties", []),
                    }

                    try:
                        self._stdin.write(json.dumps(schema_message) + "\n")
                        self._stdin.flush()

                        logger.debug(
                            "singer_target_schema_sent",
                            stream=config["stream"],
                            key_properties=config.get("key_properties", []),
                        )

                    except Exception as e:
                        logger.error(
                            "singer_target_schema_write_failed",
                            stream=config["stream"],
                            error=str(e),
                            exc_info=True,
                        )
                        raise

        except Exception as e:
            logger.error(
                "singer_target_process_start_failed",
                error=str(e),
                exc_info=True,
            )
            self.disconnect()
            raise

    # ------------------------------------------------------------------
    # Disconnect
    # ------------------------------------------------------------------
    def disconnect(self) -> None:
        # Close stdin
        if self._stdin:
            try:
                self._stdin.close()
                logger.debug("singer_target_stdin_closed")
            except Exception as e:
                logger.warning("singer_target_stdin_close_failed", error=str(e))
            finally:
                self._stdin = None

        # Close process
        if self._target_process:
            try:
                # Send STATE message if stdin was still open earlier
                if self._target_process.stdin and not self._target_process.stdin.closed:
                    state_msg = {"type": "STATE", "value": {}}
                    try:
                        self._target_process.stdin.write(json.dumps(state_msg) + "\n")
                        self._target_process.stdin.flush()
                        logger.debug("singer_target_state_sent")
                    except Exception:
                        pass

                stdout, stderr = self._target_process.communicate(timeout=10)

                if self._target_process.returncode != 0:
                    logger.error(
                        "singer_target_process_exit_error",
                        exit_code=self._target_process.returncode,
                        stderr=stderr,
                    )
                else:
                    logger.info("singer_target_process_exited_cleanly")

            except Exception as e:
                logger.error("singer_target_process_termination_failed", error=str(e), exc_info=True)
            finally:
                self._target_process = None

    # ------------------------------------------------------------------
    # Test Connection
    # ------------------------------------------------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("singer_target_test_connection_started")

        try:
            with self:
                pass

            logger.info("singer_target_test_connection_success")
            return ConnectionTestResult(
                success=True,
                message="Successfully tested Singer target connection.",
            )

        except Exception as e:
            logger.error(
                "singer_target_test_connection_failed",
                error=str(e),
                exc_info=True,
            )
            return ConnectionTestResult(
                success=False,
                message=f"Failed to test Singer target connection: {e}",
            )

    # ------------------------------------------------------------------
    # Stream Creation
    # ------------------------------------------------------------------
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        logger.debug("singer_target_create_stream_noop", stream=stream)
        # No-op — Singer targets infer schema from SCHEMA messages

    # ------------------------------------------------------------------
    # Write records
    # ------------------------------------------------------------------
    def write(self, records: Iterator[Record]) -> int:
        if not self._stdin:
            raise Exception("Singer target process not connected.")

        logger.info("singer_target_write_started")

        written = 0

        with self:
            for record in records:
                message = {
                    "type": "RECORD",
                    "stream": record.stream,
                    "record": record.data,
                }

                try:
                    self._stdin.write(json.dumps(message) + "\n")
                    written += 1

                except BrokenPipeError as e:
                    logger.error(
                        "singer_target_write_broken_pipe",
                        stream=record.stream,
                        error=str(e),
                        exc_info=True,
                    )
                    raise Exception("Singer target process pipe broke (target likely exited).")

                except Exception as e:
                    logger.error(
                        "singer_target_write_record_failed",
                        stream=record.stream,
                        error=str(e),
                        exc_info=True,
                    )
                    raise

            try:
                self._stdin.flush()
                logger.debug("singer_target_stdin_flushed")

            except Exception as e:
                logger.error("singer_target_flush_failed", error=str(e), exc_info=True)
                raise

        logger.info("singer_target_write_completed", records_written=written)
        return written
