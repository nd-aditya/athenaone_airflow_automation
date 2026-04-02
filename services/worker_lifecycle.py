"""
Worker lifecycle for Airflow deid phase: clear logs, start workers, stop workers.
Logs are under repo root as worker_1.log, worker_2.log, ... (and optionally worker.log).
"""
import glob
import os
import subprocess
import time


def _repo_root() -> str:
    """Project/repo root (where start_worker.sh and worker_*.log live)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def clear_worker_logs() -> dict:
    """
    Remove or truncate worker log files in repo root so the next run starts with fresh logs.
    Clears worker_*.log and worker.log.
    """
    root = _repo_root()
    cleared = []
    for pattern in ("worker_*.log", "worker.log"):
        for path in glob.glob(os.path.join(root, pattern)):
            try:
                with open(path, "w") as f:
                    f.write("")
                cleared.append(path)
            except Exception:
                try:
                    os.remove(path)
                    cleared.append(path)
                except Exception:
                    pass
    return {"cleared": cleared, "repo_root": root}


def start_workers(
    n: int = 2,
    conda_env: str | None = None,
) -> dict:
    """
    Start N deid workers in the background via start_worker.sh.
    Runs from repo root. Workers log to worker_1.log, worker_2.log, ...
    """
    root = _repo_root()
    script = os.path.join(root, "start_worker.sh")
    if not os.path.isfile(script):
        raise FileNotFoundError(f"start_worker.sh not found at {script}")
    env = os.environ.copy()
    if conda_env:
        env["CONDA_ENV"] = conda_env
    proc = subprocess.run(
        [script, str(n)],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"start_worker.sh failed (exit {proc.returncode}): stderr={proc.stderr!r} stdout={proc.stdout!r}"
        )
    return {"workers_started": n, "repo_root": root, "stdout": proc.stdout or ""}


_NULL_ENCOUNTERID_TABLES = [
    "CLINICALENCOUNTERDATA",
    "CLINICALENCOUNTERDIAGNOSIS",
    "CLINICALTEMPLATE",
    "VITALSIGN",
    "CLINICALSERVICE"
]


def _cleanup_null_encounterid(deid_schema: str) -> dict:
    """Delete rows with NULL clinicalencounterid from the deid schema."""
    from sqlalchemy import create_engine, text
    from services.config import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST

    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{deid_schema}",
        pool_pre_ping=True,
    )
    summary = {}
    with engine.connect() as conn:
        for table in _NULL_ENCOUNTERID_TABLES:
            try:
                result = conn.execute(text(
                    f"DELETE FROM `{deid_schema}`.`{table}` WHERE `clinicalencounterid` IS NULL"
                ))
                conn.commit()
                summary[table] = result.rowcount
            except Exception as e:
                summary[table] = f"SKIPPED: {e}"
    engine.dispose()
    return summary


def stop_workers(deid_schema: str | None = None) -> dict:
    """
    Stop all processes running manage.py start_worker (deid workers).
    Uses pkill -f "manage.py start_worker". Safe if no workers are running.
    If deid_schema is provided, also cleans up NULL clinicalencounterid rows.
    """
    proc = subprocess.run(
        ["pkill", "-f", "manage.py start_worker"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    # pkill returns 0 if at least one process was killed, 1 if no match
    if proc.returncode not in (0, 1):
        raise RuntimeError(f"pkill failed: returncode={proc.returncode} stderr={proc.stderr!r}")
    # Brief pause so processes exit before next run
    time.sleep(2)
    result = {"stopped": True}
    if deid_schema:
        result["cleanup"] = _cleanup_null_encounterid(deid_schema)
    return result
