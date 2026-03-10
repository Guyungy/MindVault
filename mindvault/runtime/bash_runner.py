"""Bash runner: execute commands with pid/timeout/stdout/stderr capture."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict
import subprocess


class BashRunner:
    """Thin wrapper for monitored command execution."""

    def __init__(self, stdout_dir: str | Path) -> None:
        self.stdout_dir = Path(stdout_dir)
        self.stdout_dir.mkdir(parents=True, exist_ok=True)

    def run(self, command: str, *, timeout_seconds: int = 120, cwd: str | Path | None = None) -> Dict[str, Any]:
        started_at = datetime.utcnow().isoformat()
        stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        stdout_path = self.stdout_dir / f"{stamp}.out"
        stderr_path = self.stdout_dir / f"{stamp}.err"

        with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
            process = subprocess.Popen(
                command,
                shell=True,
                cwd=str(cwd) if cwd else None,
                stdout=stdout_handle,
                stderr=stderr_handle,
                text=True,
            )
            pid = process.pid
            timed_out = False
            try:
                exit_code = process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                process.kill()
                exit_code = -1
                timed_out = True

        return {
            "command": command,
            "pid": pid,
            "timeout_seconds": timeout_seconds,
            "timed_out": timed_out,
            "exit_code": exit_code,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "started_at": started_at,
            "ended_at": datetime.utcnow().isoformat(),
        }
