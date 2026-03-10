"""Task monitor: derive task health from persisted task runtime state."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable


HEALTHY_STATUSES = {"completed", "failed", "blocked", "paused"}


def summarize_task(task: Dict[str, Any], recent_steps: Iterable[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    """Return a lightweight health summary for UI and monitoring."""
    recent_steps = list(recent_steps or [])
    now = datetime.utcnow()
    heartbeat_age_seconds = _heartbeat_age_seconds(task.get("last_heartbeat"), now)
    activity_age_seconds = _last_activity_age_seconds(task, recent_steps, now)
    stale = task.get("status") == "running" and activity_age_seconds is not None and activity_age_seconds > 300

    fallback_count = sum(1 for step in recent_steps if step.get("status") == "fallback")
    failed_count = sum(1 for step in recent_steps if step.get("status") == "failed")

    if stale:
        health = "stale"
    elif task.get("status") in HEALTHY_STATUSES:
        health = task.get("status")
    elif failed_count:
        health = "degraded"
    else:
        health = "healthy"

    return {
        "health": health,
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "activity_age_seconds": activity_age_seconds,
        "is_stale": stale,
        "recent_fallbacks": fallback_count,
        "recent_failures": failed_count,
        "step_count": len(recent_steps),
    }


def _heartbeat_age_seconds(value: str | None, now: datetime) -> int | None:
    if not value:
        return None
    try:
        heartbeat = datetime.fromisoformat(value)
    except ValueError:
        return None
    return max(0, int((now - heartbeat).total_seconds()))


def _last_activity_age_seconds(task: Dict[str, Any], recent_steps: Iterable[Dict[str, Any]], now: datetime) -> int | None:
    timestamps = []
    if task.get("last_heartbeat"):
        try:
            timestamps.append(datetime.fromisoformat(task["last_heartbeat"]))
        except ValueError:
            pass
    for step in recent_steps:
        value = step.get("timestamp")
        if not value:
            continue
        try:
            timestamps.append(datetime.fromisoformat(value))
        except ValueError:
            continue
    if not timestamps:
        return None
    return max(0, int((now - max(timestamps)).total_seconds()))
