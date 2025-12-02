"""
Singer Destination Connector (Improved / Production-ready)

Wraps any Singer.io target executable. Provides:
- robust subprocess lifecycle
- structured logging
- safe SCHEMA sending from catalog
- batched RECORD writes (streaming to target stdin)
- context manager support for `with self:`
"""

from __future__ import annotations
import json
import shutil
import subprocess
import time
from collections.abc import Iterator
from typing import Any, Dict, List, Optional, TextIO

from app.connectors.base import Column, ConnectionTestResult, DestinationConnector, Record
from app.core.logging import get_logger
from app.schemas.connector_configs import SingerDestinationConfig

logger = get_logger(__name__)


class SingerDestination(DestinationConnector):
    """
    Singer.io Target Wrapper.
    """

    # How long to wait for process to exit on graceful shutdown before force-killing (seconds)
    _GRACEFUL_TERMINATION_TIMEOUT = 5
    _FORCE_KILL_TIMEOUT = 3

    def __init__(self, config: SingerDestinationConfig):
        super().__init__(config)
        self.target_executable: str = config.target_executable
        self.target_config: Dict[str, Any] = config.target_config or {}
        self.target_catalog: Optional[Dict[str, Any]] = config.target_catalog
        self._target_process: Optional[subprocess.Popen] = None
        self._stdin: Optional[TextIO] = None
        self._sent_schemas: set[str] = set()  # avoid re-sending same SCHEMA

        logger.debug(
            "singer_destination_initialized",
            target_executable=self.target_executable,
            has_catalog=bool(self.target_catalog),
        )

    # -------------------------
    # Context manager support
    # -------------------------
    def __enter__(self) -> "SingerDestination":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # ensure we attempt to send final STATE before shutdown
        try:
            self.disconnect()
        except Exception:
            # disconnection should never raise to callers of context manager
            logger.warning("singer_destination_disconnect_during_exit_failed", exc_info=True)

    # -------------------------
    # Connect / Start target
    # -------------------------
    def connect(self) -> None:
        """Start the Singer target subprocess and send SCHEMA messages (if catalog provided)."""
        if self._target_process:
            # already started
            return

        # ensure executable exists
        if not shutil.which(self.target_executable):
            msg = f"Singer target executable not found: {self.target_executable}"
            logger.error("singer_target_exec_missing", executable=self.target_executable)
            raise FileNotFoundError(msg)

        logger.info("singer_target_starting", executable=self.target_executable)

        try:
            # Start target process with text mode and line buffering for reliable writes
            self._target_process = subprocess.Popen(
                [self.target_executable, "--config", json.dumps(self.target_config)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,  # line buffered
                universal_newlines=True,
            )

            # stdin should be available if process started correctly
            if not self._target_process.stdin:
                raise RuntimeError("Singer target started but stdin is unavailable")

            self._stdin = self._target_process.stdin

            logger.info("singer_target_started", pid=getattr(self._target_process, "pid", None))

            # Send SCHEMA messages (if catalog present) — only once per stream
            if self.target_catalog and "streams" in self.target_catalog:
                for stream_cfg in self.target_catalog["streams"]:
                    stream_name = stream_cfg.get("stream")
                    if not stream_name or stream_name in self._sent_schemas:
                        continue

                    schema_message = {
                        "type": "SCHEMA",
                        "stream": stream_name,
                        "schema": stream_cfg.get("schema", {}),
                        "key_properties": stream_cfg.get("key_properties", []),
                    }

                    try:
                        self._write_line(json.dumps(schema_message))
                        self._sent_schemas.add(stream_name)
                        logger.debug("singer_schema_sent", stream=stream_name)
                    except Exception as e:
                        logger.error(
                            "singer_schema_send_failed",
                            stream=stream_name,
                            error=str(e),
                            exc_info=True,
                        )
                        # If schema sending fails, we tear down process and propagate
                        self.disconnect()
                        raise

        except Exception as e:
            logger.error("singer_target_start_failed", error=str(e), exc_info=True)
            # ensure process cleaned up
            self._force_cleanup_process()
            raise

    # -------------------------
    # Disconnect / Stop target
    # -------------------------
    def disconnect(self) -> None:
        """
        Try to cleanly close stdin, optionally send a STATE message, wait for process to exit.
        If process doesn't exit, attempt terminate -> kill.
        """
        # If stdin exists try send a final empty STATE message (best-effort)
        if self._stdin:
            try:
                state_msg = {"type": "STATE", "value": {}}
                self._write_line(json.dumps(state_msg))
                self._stdin.flush()
                logger.debug("singer_state_sent_before_shutdown")
            except Exception:
                # ignore failures sending final state
                logger.debug("singer_final_state_send_failed", exc_info=True)

            try:
                self._stdin.close()
                logger.debug("singer_target_stdin_closed")
            except Exception:
                logger.warning("singer_target_stdin_close_failed", exc_info=True)
            finally:
                self._stdin = None

        # If a process is running, try graceful shutdown
        if self._target_process:
            proc = self._target_process
            try:
                # Wait briefly for process to exit on its own
                waited = 0.0
                while proc.poll() is None and waited < self._GRACEFUL_TERMINATION_TIMEOUT:
                    time.sleep(0.1)
                    waited += 0.1

                if proc.poll() is None:
                    # still alive -> try terminate
                    proc.terminate()
                    logger.debug("singer_target_terminate_sent", pid=proc.pid)

                    waited = 0.0
                    while proc.poll() is None and waited < self._FORCE_KILL_TIMEOUT:
                        time.sleep(0.1)
                        waited += 0.1

                    if proc.poll() is None:
                        proc.kill()
                        logger.warning("singer_target_killed", pid=proc.pid)

                # Read any remaining stdout/stderr safely (non-blocking behavior is not strictly guaranteed,
                # but communicate with timeout is used to avoid hangs).
                try:
                    stdout, stderr = proc.communicate(timeout=2)
                except Exception:
                    # fallback to reading if communicate times out
                    try:
                        stdout = proc.stdout.read() if proc.stdout else ""
                        stderr = proc.stderr.read() if proc.stderr else ""
                    except Exception:
                        stdout, stderr = "", ""

                exit_code = proc.returncode
                if exit_code != 0:
                    logger.error(
                        "singer_target_exited_with_error",
                        pid=proc.pid,
                        exit_code=exit_code,
                        stderr_preview=(stderr or "")[:1000],
                    )
                else:
                    logger.info("singer_target_exited_cleanly", pid=proc.pid)

            except Exception:
                logger.exception("singer_target_shutdown_exception")
            finally:
                self._target_process = None

    # -------------------------
    # Test connection
    # -------------------------
    def test_connection(self) -> ConnectionTestResult:
        logger.info("singer_target_test_connection_started")
        # start and immediately stop to verify executable and config
        try:
            with self:
                # if connect succeeded we consider it successful
                pass
            return ConnectionTestResult(success=True, message="Singer target test succeeded")
        except Exception as e:
            logger.error("singer_target_test_connection_failed", error=str(e), exc_info=True)
            return ConnectionTestResult(success=False, message=f"Singer target test failed: {e}")

    # -------------------------
    # create_stream (no-op for Singer)
    # -------------------------
    def create_stream(self, stream: str, schema: List[Column]) -> None:
        logger.debug("singer_target_create_stream_noop", stream=stream)
        # Singer targets accept SCHEMA messages; creation is a no-op here.

    # -------------------------
    # Write records
    # -------------------------
    def write(self, records: Iterator[Record]) -> int:
        if not self._target_process or not self._stdin:
            raise RuntimeError("Singer target not connected. Call connect() before write().")

        logger.info("singer_target_write_started")

        written = 0

        # stream records sequentially — do not buffer entire dataset to memory
        try:
            # keep writing records; we don't batch here because we stream line by line
            for record in records:
                message = {"type": "RECORD", "stream": record.stream, "record": record.data}
                try:
                    self._write_line(json.dumps(message))
                    written += 1
                except BrokenPipeError as e:
                    logger.error("singer_target_broken_pipe", stream=record.stream, error=str(e), exc_info=True)
                    raise RuntimeError("Singer target stdin broken (target process likely exited).") from e
                except Exception as e:
                    logger.error("singer_target_write_record_failed", stream=record.stream, error=str(e), exc_info=True)
                    raise

            # ensure data flushed to the child process
            try:
                self._stdin.flush()
            except Exception:
                logger.debug("singer_target_flush_failed", exc_info=True)

            logger.info("singer_target_write_completed", records_written=written)
            return written

        finally:
            # we deliberately do not disconnect here; caller controls lifecycle
            pass

    # -------------------------
    # Internal helpers
    # -------------------------
    def _write_line(self, line: str) -> None:
        """Write a single newline-terminated line to target stdin."""
        if not self._stdin:
            raise RuntimeError("Target stdin is not available.")
        # ensure newline
        if not line.endswith("\n"):
            line = line + "\n"
        self._stdin.write(line)

    def _force_cleanup_process(self) -> None:
        """Attempt forceful cleanup of underlying process if something failed during startup."""
        if self._target_process:
            try:
                if self._target_process.poll() is None:
                    self._target_process.kill()
                    logger.debug("singer_target_force_killed")
            except Exception:
                logger.exception("singer_target_force_kill_failed")
            finally:
                self._target_process = None
                self._stdin = None
