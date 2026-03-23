"""
Google Chat webhook helpers for Airflow DAG notifications.

Uses services.config: GOOGLE_CHAT_WEBHOOK, ENABLE_CHAT_NOTIFICATIONS,
NOTIFY_ON_SUCCESS, NOTIFY_ON_FAILURE.
Sends plain text payloads compatible with standard Chat incoming webhooks.
"""
from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from typing import Any


def _post_json(webhook_url: str, payload: dict) -> bool:
    if not webhook_url or not str(webhook_url).strip():
        return False
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError):
        return False


def send_chat_text(text: str) -> bool:
    """POST a plain text message to the configured webhook (if enabled)."""
    from services.config import ENABLE_CHAT_NOTIFICATIONS, GOOGLE_CHAT_WEBHOOK

    if not ENABLE_CHAT_NOTIFICATIONS:
        return False
    return _post_json(GOOGLE_CHAT_WEBHOOK, {"text": text})


def _dag_run_bits(context: dict[str, Any]) -> tuple[str, str, str]:
    dag = context.get("dag")
    dr = context.get("dag_run")
    dag_id = dag.dag_id if dag else "unknown_dag"
    run_id = dr.run_id if dr else "unknown_run"
    when = context.get("logical_date") or context.get("data_interval_start") or ""
    return dag_id, run_id, str(when)


def notify_dag_success(context: dict[str, Any], title: str) -> None:
    """Airflow on_success_callback: send Chat when NOTIFY_ON_SUCCESS and enabled."""
    from services.config import (
        ENABLE_CHAT_NOTIFICATIONS,
        GOOGLE_CHAT_WEBHOOK,
        NOTIFY_ON_SUCCESS,
    )

    if not ENABLE_CHAT_NOTIFICATIONS or not NOTIFY_ON_SUCCESS or not GOOGLE_CHAT_WEBHOOK:
        return

    dag_id, run_id, when = _dag_run_bits(context)
    host = socket.gethostname()
    text = (
        f"*✅ {title}* — completed successfully\n"
        f"• DAG: `{dag_id}`\n"
        f"• Run ID: `{run_id}`\n"
        f"• Logical date: `{when}`\n"
        f"• Host: `{host}`"
    )
    _post_json(GOOGLE_CHAT_WEBHOOK, {"text": text})


def notify_dag_failure(context: dict[str, Any], title: str) -> None:
    """Airflow on_failure_callback: send Chat when NOTIFY_ON_FAILURE and enabled."""
    from airflow.utils.state import TaskInstanceState

    from services.config import (
        ENABLE_CHAT_NOTIFICATIONS,
        GOOGLE_CHAT_WEBHOOK,
        NOTIFY_ON_FAILURE,
    )

    if not ENABLE_CHAT_NOTIFICATIONS or not NOTIFY_ON_FAILURE or not GOOGLE_CHAT_WEBHOOK:
        return

    dag_id, run_id, when = _dag_run_bits(context)
    host = socket.gethostname()
    dr = context.get("dag_run")
    failed_tasks: list[str] = []
    if dr is not None:
        try:
            failed_tasks = [
                ti.task_id
                for ti in dr.get_task_instances(state=TaskInstanceState.FAILED)
            ]
        except Exception:
            failed_tasks = []

    ti = context.get("task_instance")
    if not failed_tasks and ti is not None:
        failed_tasks = [ti.task_id]

    exception = context.get("exception")
    err = f"• Exception: `{str(exception)[:500]}`\n" if exception else ""

    text = (
        f"*❌ {title}* — failed\n"
        f"• DAG: `{dag_id}`\n"
        f"• Run ID: `{run_id}`\n"
        f"• Logical date: `{when}`\n"
        f"• Failed task(s): `{', '.join(failed_tasks) or 'unknown'}`\n"
        f"• Host: `{host}`\n"
        f"{err}"
    )
    _post_json(GOOGLE_CHAT_WEBHOOK, {"text": text})


def extract_merge_chat_success(context: dict[str, Any]) -> None:
    notify_dag_success(context, "Athenaone Extract + Merge")


def extract_merge_chat_failure(context: dict[str, Any]) -> None:
    notify_dag_failure(context, "Athenaone Extract + Merge")
