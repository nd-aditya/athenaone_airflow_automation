import subprocess
import time
import signal
import os
import logging
import ast
import django
import sys
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone
from django.conf import settings
from deIdentification.nd_logger import nd_logger

# Don't import Django models at module level
# Child processes will import the module before Django is set up
# We'll import Django models inside functions where Django is guaranteed to be set up

# global list to store Terminal window information
# Format: {'window_id': window_id, 'process': process}
terminal_windows = []

_CPU_COUNT = os.cpu_count()
if not _CPU_COUNT:
    nd_logger.warning(f"unable to find cpu count. setting cpu count to 8")
    _CPU_COUNT = 8

MAX_WORKERS = _CPU_COUNT


def get_project_dir():
    """Get the project directory path."""
    # Try to get from settings or use manage.py location
    try:
        # Get the directory where manage.py is located
        manage_py_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'manage.py')
        if os.path.exists(manage_py_path):
            return os.path.dirname(manage_py_path)
    except:
        pass
    
    # Fallback to current working directory or use environment variable
    return os.getcwd()


def start_terminal_worker(worker_id, project_dir, venv_path=None):
    """Start a worker in a new Terminal window using osascript."""
    try:
        # Build the command - escape single quotes for AppleScript
        project_dir_escaped = project_dir.replace("'", "\\'")
        
        if venv_path:
            # Using conda - escape venv path too
            venv_path_escaped = venv_path.replace("'", "\\'")
            command = f"conda activate '{venv_path_escaped}' && cd '{project_dir_escaped}' && python manage.py start_worker"
        else:
            # Direct python command
            command = f"cd '{project_dir_escaped}' && python manage.py start_worker"
        
        # AppleScript to open Terminal and run command
        # Get the window ID after creating the window
        applescript = f'''
        tell application "Terminal"
            do script "{command}"
            set window_id to id of front window
            return window_id as string
        end tell
        '''
        
        # Run osascript
        result = subprocess.run(
            ['osascript', '-e', applescript],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            window_id = result.stdout.strip()
            nd_logger.info(f"[Manager] Started worker {worker_id} in Terminal window {window_id}")
            return window_id
        else:
            nd_logger.error(f"[Manager] Failed to start Terminal window: {result.stderr}")
            return None
            
    except Exception as e:
        nd_logger.error(f"[Manager] Error starting Terminal worker {worker_id}: {e}", exc_info=True)
        return None


def is_terminal_window_open(window_id):
    """Check if a Terminal window is still open."""
    if not window_id:
        return False
    try:
        applescript = f'''
        tell application "Terminal"
            try
                set targetWindow to window id {window_id}
                return true
            on error
                return false
            end try
        end tell
        '''
        result = subprocess.run(
            ['osascript', '-e', applescript],
            capture_output=True,
            text=True,
            timeout=2
        )
        return result.returncode == 0 and 'true' in result.stdout
    except:
        return False


def cleanup_workers():
    """Close all Terminal windows running workers forcefully without prompting."""
    global terminal_windows
    
    # First, kill all worker processes to prevent Terminal from prompting
    try:
        # Force kill all python manage.py start_worker processes
        subprocess.run(['pkill', '-9', '-f', 'python manage.py start_worker'], 
                      timeout=2, capture_output=True)
        time.sleep(0.5)  # Give processes time to die
    except:
        pass
    
    # Now close all Terminal windows without prompting
    for window_info in terminal_windows[:]:
        try:
            window_id = window_info.get('window_id')
            if window_id:
                # AppleScript to forcefully close the window without prompting
                # Since we've already killed the process, Terminal won't ask
                applescript = f'''
                tell application "Terminal"
                    try
                        set targetWindow to window id {window_id}
                        if exists targetWindow then
                            -- Force close without prompting (process already killed)
                            close targetWindow saving no
                            return true
                        else
                            return false
                        end if
                    on error
                        return false
                    end try
                end tell
                '''
                
                result = subprocess.run(
                    ['osascript', '-e', applescript],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                if result.returncode == 0:
                    nd_logger.info(f"[Manager] Forcefully closed Terminal window {window_id}")
                else:
                    nd_logger.debug(f"[Manager] Terminal window {window_id} might already be closed")
        except Exception as e:
            nd_logger.error(f"[Manager] Error closing Terminal window: {e}")
    
    terminal_windows.clear()


def has_ready_tasks():
    """Check if there are ready tasks available without locking them."""
    try:
        # Import Task here since it may not be available at module level in child processes
        from worker.models import Task
        from worker.models.helper import ComputationStatus
        
        _ready_tasks = Task.objects.filter(status=ComputationStatus.NOT_STARTED)
        # Use the same query logic as Task._get_ready_task but just check existence
        # performant_filter = Q(soft_delete=False) & Q(num_dependencies_pending=0)
        # _ready_tasks = (
        #     Task.objects.filter(
        #         (performant_filter & Q(status=ComputationStatus.NOT_STARTED))
        #         | (
        #             performant_filter
        #             & Q(status=ComputationStatus.PROCESSING)
        #             & Q(expires_at__lt=timezone.now())
        #         )
        #     )
        #     .filter(Q(failure_count__lt=1))
        #     .filter(back_off__lt=timezone.now())
        # )

        # # Apply custom query filters if enabled
        # custom_query_enabled = ast.literal_eval(
        #     os.getenv("WORKERS_CUSTOM_QUERY_ENABLED", "False")
        # )
        # custom_query_include_dict = ast.literal_eval(
        #     os.getenv("WORKERS_CUSTOM_QUERY_INCLUDE_DICT", "{}")
        # )
        # custom_query_exclude_dict = ast.literal_eval(
        #     os.getenv("WORKERS_CUSTOM_QUERY_EXCLUDE_DICT", "{}")
        # )

        # if custom_query_enabled:
        #     if custom_query_include_dict:
        #         _ready_tasks = _ready_tasks.filter(**custom_query_include_dict)
        #     if custom_query_exclude_dict:
        #         _ready_tasks = _ready_tasks.exclude(**custom_query_exclude_dict)

        # Just check existence without locking
        return _ready_tasks.exists()
    except Exception as e:
        nd_logger.error(f"[Manager] Error checking for ready tasks: {e}", exc_info=True)
        # Fallback: check for tasks with NOT_STARTED status
        try:
            from worker.models import Task
            from worker.models.helper import ComputationStatus
            return Task.objects.filter(
                status=ComputationStatus.NOT_STARTED,
                num_dependencies_pending=0
            ).exists()
        except Exception:
            return False


class Command(BaseCommand):
    help = "Spin up workers dynamically based on pending tasks."

    def handle(self, *args, **options):
        global terminal_windows
        nd_logger.info("[Manager] Starting dynamic worker manager...")
        nd_logger.info(f"[Manager] MAX_WORKERS: {MAX_WORKERS}")
        
        # Get project directory
        project_dir = get_project_dir()
        nd_logger.info(f"[Manager] Project directory: {project_dir}")
        
        # Get venv path from environment or settings
        venv_path = os.getenv('WORKER_VENV_PATH', os.getenv('CONDA_DEFAULT_ENV', None))
        
        def signal_handler(sig, frame):
            nd_logger.info("[Manager] Shutting down...")
            cleanup_workers()
            exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while True:
                # Check if Terminal windows are still open
                # Remove closed windows from tracking
                terminal_windows = [
                    w for w in terminal_windows 
                    if is_terminal_window_open(w.get('window_id'))
                ]
                
                # Check if tasks are present
                tasks_present = has_ready_tasks()
                
                if tasks_present:
                    # Case 1: Start workers if tasks exist and < MAX_WORKERS
                    if len(terminal_windows) < MAX_WORKERS:
                        new_workers_needed = MAX_WORKERS - len(terminal_windows)
                        for i in range(new_workers_needed):
                            worker_id = len(terminal_windows) + 1
                            window_id = start_terminal_worker(worker_id, project_dir, venv_path)
                            if window_id:
                                terminal_windows.append({
                                    'window_id': window_id,
                                    'worker_id': worker_id
                                })
                                time.sleep(1)  # Small delay between opening terminals
                            else:
                                nd_logger.warning(f"[Manager] Failed to start worker {worker_id}")
                else:
                    # Case 2: Stop workers if no tasks are present
                    if terminal_windows:
                        nd_logger.info("[Manager] No tasks present. Closing Terminal windows...")
                        cleanup_workers()

                time.sleep(5)

        except Exception as e:
            nd_logger.error(f"[Manager] Error: {e}", exc_info=True)
            cleanup_workers()
