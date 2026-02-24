from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from keycloakauth.utils import IsAuthenticated
import os
import json
import re
import threading
import time
import sys
from datetime import datetime
from pathlib import Path
from nd_api_v2.decorator import conditional_authentication
# Scheduler imports
try:
    import schedule
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False
    schedule = None
from nd_api_v2.models.scheduler_config import SchedulerConfig, default_run_config as scheduler_default_config
from nd_api_v2.services.ecw.find_incremental import find_incremental_diff, get_diff_database_connection_string
from nd_api_v2.services.ecw.merge_incremental_diff import merge_incremental_diff_into_main_database
from nd_api_v2.services.register_dump import register_dump_in_queue


# Global scheduler state
scheduler_state = {
    'enabled': False,
    'time': '02:00',
    'timezone': 'UTC',
    'last_run': None,
    'next_run': None,
    'thread': None
}

# Global pipeline state
pipeline_state = {
    'running': False,
    'thread': None,
    'process_id': None
}


# ============================================================================
# PIPELINE RUNNER FUNCTION
# ============================================================================

def run_pipeline_in_background():
    """Run the pipeline in a background thread"""
    def pipeline_worker():
        # Create log file for this run
        logs_dir = Path(settings.LOGS_DIR)
        logs_dir.mkdir(exist_ok=True, parents=True)
        log_file = logs_dir / f'ecw_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
        # Redirect stdout and stderr to log file
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        try:
            # Create log file and write initial message
            log_file_handle = open(log_file, 'w', buffering=1)  # Line buffered
            log_file_handle.write(f"ECW Pipeline started at {datetime.now().isoformat()}\n")
            log_file_handle.flush()
            
            # Store log file name in state BEFORE starting pipeline
            pipeline_state['running'] = True
            pipeline_state['log_file'] = log_file.name
            
            # Create a class that writes to both file and original stdout
            class TeeOutput:
                def __init__(self, file_handle, original_stream):
                    self.file = file_handle
                    self.original = original_stream
                
                def write(self, text):
                    if text:  # Only write non-empty text
                        self.file.write(text)
                        self.file.flush()
                        self.original.write(text)
                        self.original.flush()
                
                def flush(self):
                    self.file.flush()
                    self.original.flush()
                
                def fileno(self):
                    # Return original stream's fileno for compatibility
                    return self.original.fileno() if hasattr(self.original, 'fileno') else None
                
                def isatty(self):
                    return False  # Not a TTY
                
                def readable(self):
                    return False
                
                def writable(self):
                    return True
                
                def seekable(self):
                    return False
            
            # Redirect stdout and stderr
            sys.stdout = TeeOutput(log_file_handle, original_stdout)
            sys.stderr = TeeOutput(log_file_handle, original_stderr)
            
            run_pipeline()
        except Exception as e:
            print(f"❌ Pipeline execution failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Restore stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            # Close log file
            if 'log_file_handle' in locals():
                log_file_handle.close()
            
            pipeline_state['running'] = False
            pipeline_state['thread'] = None
            pipeline_state['process_id'] = None
            if 'log_file' in pipeline_state:
                del pipeline_state['log_file']
    
    if pipeline_state['running']:
        return False, "Pipeline is already running"
    
    thread = threading.Thread(target=pipeline_worker, daemon=True)
    thread.start()
    pipeline_state['thread'] = thread
    pipeline_state['process_id'] = thread.ident
    
    return True, f"Pipeline started in background thread (ID: {thread.ident})"


def run_pipeline():
    """Execute the complete ECW pipeline - runs within Django context"""
    # Get scheduler config
    scheduler_config = SchedulerConfig.objects.last()
    if scheduler_config is None:
        print("❌ Scheduler configuration not found, please define the scheduler type and configuration")
        return False
    
    if settings.INCREMENTAL_PROCESS_TYPE != "ecw_with_diff_script":
        print(f"❌ Invalid incremental process type: {settings.INCREMENTAL_PROCESS_TYPE}")
        return False
    
    config = scheduler_config.run_config
    start_time = datetime.now()
    
    def execute_step(step_num, total_steps, step_name, step_function):
        """Execute a pipeline step with error handling"""
        print(f"\n{'='*80}", flush=True)
        print(f"STEP {step_num}/{total_steps}: {step_name.upper()}", flush=True)
        print(f"{'='*80}", flush=True)
        
        try:
            step_function()
            print(f"✅ Step {step_num} completed successfully", flush=True)
            return True
        except Exception as e:
            print(f"❌ Step {step_num} failed: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False
    
    # Start pipeline
    print("\n" + "="*80, flush=True)
    print("🚀 STARTING ECW INCREMENTAL PIPELINE EXECUTION", flush=True)
    print("="*80 + "\n", flush=True)
    
    # Step 1: Find Incremental Diff
    def step1():
        print(f"🔍 Finding incremental differences...", flush=True)
        find_incremental_diff(config)
        print(f"✅ Find incremental diff completed", flush=True)
    
    if not execute_step(1, 2, "Find Incremental Diff", step1):
        return False
    
    # Step 2: Merge Incremental Diff
    def step2():
        print(f"🔄 Merging incremental diff into main database...", flush=True)
        merge_incremental_diff_into_main_database(config)
        print(f"✅ Merge incremental diff completed", flush=True)
    
    if not execute_step(2, 2, "Merge Incremental Diff", step2):
        return False
    
    # Calculate duration
    end_time = datetime.now()
    duration = end_time - start_time
    duration_str = str(duration).split('.')[0]
    
    # Register dump in queue (this will trigger deid)
    try:
        connection_string = get_diff_database_connection_string(config)
        dump_date = config['dump_date']
        
        print(f"📝 Registering dump in queue for deid processing...", flush=True)
        register_dump_in_queue(connection_string, dump_date)
        print(f"✅ Dump registered successfully", flush=True)
    except Exception as e:
        print(f"⚠️ Warning: Failed to register dump in queue: {e}", flush=True)
        import traceback
        traceback.print_exc()
    
    # Success message
    print("\n" + "="*80, flush=True)
    print("🎉 ECW INCREMENTAL PIPELINE EXECUTION COMPLETED SUCCESSFULLY!", flush=True)
    print(f"All 2 steps completed successfully.", flush=True)
    print(f"Total duration: {duration_str}", flush=True)
    print(f"Check logs/ folder for detailed logs", flush=True)
    print("="*80 + "\n", flush=True)
    
    return True


@conditional_authentication
class ECWPipelineConfigView(APIView):
    """Manage ECW pipeline configuration"""
    authentication_classes = [IsAuthenticated]

    def get(self, request):
        """Get current configuration"""
        try:
            scheduler_config = SchedulerConfig.objects.last()
            if scheduler_config is None:
                return Response({
                    "success": True,
                    "config": scheduler_default_config()
                }, status=status.HTTP_200_OK)
            
            if settings.INCREMENTAL_PROCESS_TYPE != "ecw_with_diff_script":
                return Response({
                    "success": False,
                    "error": f"Invalid incremental process type. Expected ecw_with_diff_script"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            return Response({
                "success": True,
                "config": scheduler_config.run_config
            }, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
                
    
    def post(self, request):
        """Update configuration"""
        try:
            data = request.data
            run_config_update = data.get("run_config", {})
            
            scheduler_config = SchedulerConfig.objects.last()
            if scheduler_config is None:
                # Create new config with defaults and update with provided values
                default_config = scheduler_default_config()
                # Deep merge the update into default config
                self._deep_update(default_config, run_config_update)
                scheduler_config = SchedulerConfig.objects.create(run_config=default_config)
            else:
                # Ensure process type is correct
                if settings.INCREMENTAL_PROCESS_TYPE != "ecw_with_diff_script":
                    return Response({
                        "success": False,
                        "error": f"Invalid incremental process type. Expected ecw_with_diff_script"
                    }, status=status.HTTP_400_BAD_REQUEST)
                # Deep merge the update into existing config
                self._deep_update(scheduler_config.run_config, run_config_update)
                scheduler_config.save()
            
            return Response({
                "success": True,
                "message": "Configuration updated successfully",
                "config": scheduler_config.run_config
            }, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _deep_update(self, base_dict, update_dict):
        """Recursively update nested dictionary"""
        for key, value in update_dict.items():
            if key in base_dict and isinstance(base_dict[key], dict) and isinstance(value, dict):
                self._deep_update(base_dict[key], value)
            else:
                base_dict[key] = value


@conditional_authentication
class ECWPipelineControlView(APIView):
    """Start/Stop/Monitor pipeline execution"""
    authentication_classes = [IsAuthenticated]
    
    def post(self, request):
        """Start or stop pipeline execution"""
        try:
            action = request.data.get('action')  # 'start', 'stop'
            
            if action == 'start':
                # Check if pipeline is already running
                if pipeline_state['running']:
                    return Response({
                        "success": False,
                        "message": "Pipeline is already running"
                    }, status=status.HTTP_400_BAD_REQUEST)
                
                # Start pipeline in background thread (runs within Django context)
                success, message = run_pipeline_in_background()
                
                if success:
                    return Response({
                        "success": True,
                        "message": message,
                        "thread_id": pipeline_state['process_id']
                    }, status=status.HTTP_200_OK)
                else:
                    return Response({
                        "success": False,
                        "message": message
                    }, status=status.HTTP_400_BAD_REQUEST)
                
            elif action == 'stop':
                # Stop running pipeline
                if not pipeline_state['running']:
                    return Response({
                        "success": False,
                        "message": "No running pipeline found"
                    }, status=status.HTTP_404_NOT_FOUND)
                
                # Set running flag to False (pipeline will check this and exit gracefully)
                pipeline_state['running'] = False
                
                return Response({
                    "success": True,
                    "message": "Pipeline stop requested. It will finish current step and exit."
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    "success": False,
                    "message": f"Invalid action: {action}"
                }, status=status.HTTP_400_BAD_REQUEST)
                
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class ECWPipelineStatusView(APIView):
    """Get real-time pipeline status"""
    authentication_classes = [IsAuthenticated]
    
    def get(self, request):
        """Get current pipeline status"""
        try:
            logs_dir = Path(settings.LOGS_DIR)
            
            if not logs_dir.exists():
                logs_dir.mkdir(parents=True, exist_ok=True)
            
            # Check if pipeline is running (using thread state)
            is_running = pipeline_state['running']
            
            # Check if thread is still alive
            if is_running and pipeline_state['thread']:
                is_running = pipeline_state['thread'].is_alive()
                if not is_running:
                    pipeline_state['running'] = False
            
            # Get most recent log file
            log_files = sorted(logs_dir.glob('ecw_pipeline_*.log'), key=os.path.getmtime, reverse=True)
            
            if not log_files:
                return Response({
                    "success": True,
                    "status": "idle",
                    "current_step": None,
                    "total_steps": 2,
                    "progress": 0,
                    "logs": [],
                    "message": "No pipeline runs found"
                }, status=status.HTTP_200_OK)
            
            # If pipeline is running, prefer the current log file
            if is_running and pipeline_state.get('log_file'):
                current_log_file = logs_dir / pipeline_state['log_file']
                if current_log_file.exists():
                    latest_log = current_log_file
                else:
                    latest_log = log_files[0] if log_files else None
            else:
                latest_log = log_files[0] if log_files else None
            
            if not latest_log:
                return Response({
                    "success": True,
                    "status": "idle",
                    "current_step": None,
                    "total_steps": 2,
                    "progress": 0,
                    "logs": [],
                    "message": "No log files found"
                }, status=status.HTTP_200_OK)
            
            # Parse log to determine status - read with error handling for file being written
            try:
                with open(latest_log, 'r', encoding='utf-8', errors='ignore') as f:
                    # Read all lines, handling files that might be actively written
                    lines = []
                    for line in f:
                        lines.append(line)
                    last_lines = lines[-100:] if len(lines) > 100 else lines
            except (IOError, OSError) as e:
                # If file is locked or being written, try reading what we can
                try:
                    with open(latest_log, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()
                        last_lines = lines[-100:] if len(lines) > 100 else lines
                except:
                    last_lines = [f"Error reading log file: {e}"]
            
            # Determine current step and progress
            current_step = None
            total_steps = 2
            step_name = None
            pipeline_status = "idle"
            
            # Look for step information in reverse order (most recent first)
            for line in reversed(last_lines):
                # Check for completion
                if "COMPLETED SUCCESSFULLY" in line:
                    pipeline_status = "completed"
                    current_step = 2
                    break
                
                # Check for failure
                if "FAILED" in line or "ERROR" in line.upper() or "❌" in line:
                    pipeline_status = "failed"
                    break
                
                # Look for current step
                if "STEP" in line:
                    match = re.search(r'STEP (\d+)/(\d+): (.+)', line)
                    if match:
                        current_step = int(match.group(1))
                        total_steps = int(match.group(2))
                        step_name = match.group(3).strip()
                        pipeline_status = "running" if is_running else "completed"
                        break
            
            # Calculate progress
            progress = 0
            if current_step:
                if pipeline_status == "completed":
                    progress = 100
                else:
                    progress = int((current_step / total_steps) * 100)
            
            # Get last 50 log lines for display
            display_logs = [line.strip() for line in last_lines[-50:]]
            
            return Response({
                "success": True,
                "status": pipeline_status,
                "current_step": current_step,
                "total_steps": total_steps,
                "step_name": step_name,
                "progress": progress,
                "logs": display_logs,
                "log_file": latest_log.name,
                "is_running": is_running
            }, status=status.HTTP_200_OK)
                
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class ECWPipelineLogsView(APIView):
    """Get pipeline logs"""
    authentication_classes = [IsAuthenticated]

    def get(self, request):
        """Get logs from specific run"""
        try:
            log_filename = request.query_params.get('file')
            logs_dir = Path(settings.LOGS_DIR)
            
            if not logs_dir.exists():
                return Response({
                    "success": False,
                    "message": "Logs directory not found"
                }, status=status.HTTP_404_NOT_FOUND)
            
            if log_filename:
                log_file = logs_dir / log_filename
            else:
                # Get most recent log
                log_files = sorted(logs_dir.glob('ecw_pipeline_*.log'), key=os.path.getmtime, reverse=True)
                log_file = log_files[0] if log_files else None
            
            if not log_file or not log_file.exists():
                return Response({
                    "success": False,
                    "message": "Log file not found"
                }, status=status.HTTP_404_NOT_FOUND)
            
            with open(log_file, 'r') as f:
                logs = f.readlines()
            
            # Return as list of strings
            log_lines = [line.strip() for line in logs]
            
            return Response({
                "success": True,
                "logs": log_lines,
                "file": log_file.name,
                "total_lines": len(log_lines)
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class ECWPipelineHistoryView(APIView):
    """Get pipeline execution history"""
    authentication_classes = [IsAuthenticated]
    
    def get(self, request):
        """List all pipeline runs"""
        try:
            logs_dir = Path(settings.LOGS_DIR)
            
            if not logs_dir.exists():
                return Response({
                    "success": True,
                    "history": [],
                    "total": 0
                }, status=status.HTTP_200_OK)
            
            log_files = sorted(logs_dir.glob('ecw_pipeline_*.log'), key=os.path.getmtime, reverse=True)
            
            history = []
            for log_file in log_files:
                try:
                    # Parse log file to get run info
                    with open(log_file, 'r') as f:
                        content = f.read()
                    
                    # Check status
                    completed = "COMPLETED SUCCESSFULLY" in content
                    failed = "FAILED" in content or "❌" in content
                    
                    # Extract date from filename or file modification time
                    date_match = re.search(r'(\d{8})', log_file.name)
                    if date_match:
                        date_str = date_match.group(1)
                        # Format as YYYY-MM-DD
                        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    else:
                        date_formatted = datetime.fromtimestamp(log_file.stat().st_mtime).strftime("%Y-%m-%d")
                    
                    # Get file stats
                    file_stats = log_file.stat()
                    
                    history.append({
                        "date": date_formatted,
                        "status": "completed" if completed else ("failed" if failed else "unknown"),
                        "file": log_file.name,
                        "size": file_stats.st_size,
                        "modified": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
                        "lines": len(content.split('\n'))
                    })
                except Exception as e:
                    # Skip files that can't be read
                    continue
            
            return Response({
                "success": True,
                "history": history,
                "total": len(history)
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# ============================================================================
# SCHEDULER FUNCTIONALITY
# ============================================================================

def run_scheduled_pipeline():
    """Execute pipeline on schedule - runs within Django context"""
    try:
        print(f"⏰ Scheduled ECW pipeline execution started at {datetime.now()}")
        
        # Check if pipeline is already running
        if pipeline_state['running']:
            print("⚠️ Pipeline is already running, skipping scheduled run")
            return
        
        # Start pipeline in background thread
        success, message = run_pipeline_in_background()
        
        if success:
            # Update last run time
            scheduler_state['last_run'] = datetime.now().isoformat()
            print(f"✅ Scheduled pipeline started: {message}")
        else:
            print(f"❌ Scheduled pipeline failed to start: {message}")
        
    except Exception as e:
        print(f"❌ Scheduled pipeline failed to start: {e}")
        import traceback
        traceback.print_exc()


def run_scheduler_loop():
    """Background thread to run the scheduler"""
    while scheduler_state['enabled']:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


@conditional_authentication
class ECWSchedulerStatusView(APIView):
    """Get scheduler status"""
    authentication_classes = [IsAuthenticated]
    
    def get(self, request):
        """Get current scheduler status"""
        try:
            if not SCHEDULER_AVAILABLE:
                return Response({
                    "success": False,
                    "error": "Scheduler library not available. Install with: pip install schedule"
                }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
            
            # Calculate next run if enabled
            next_run = None
            if scheduler_state['enabled'] and schedule.jobs:
                next_run_time = schedule.next_run()
                if next_run_time:
                    next_run = next_run_time.isoformat()
            
            return Response({
                "success": True,
                "enabled": scheduler_state['enabled'],
                "time": scheduler_state['time'],
                "timezone": scheduler_state['timezone'],
                "last_run": scheduler_state['last_run'],
                "next_run": next_run,
                "jobs_count": len(schedule.jobs) if schedule else 0
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class ECWSchedulerEnableView(APIView):
    """Enable scheduled pipeline runs"""
    authentication_classes = [IsAuthenticated]
    
    def post(self, request):
        """Enable scheduler with specified time and timezone"""
        try:
            if not SCHEDULER_AVAILABLE:
                return Response({
                    "success": False,
                    "error": "Scheduler library not available. Install with: pip install schedule"
                }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
            
            data = request.data
            schedule_time = data.get('time', '02:00')
            timezone_name = data.get('timezone', 'UTC')
            
            # Validate time format
            try:
                datetime.strptime(schedule_time, '%H:%M')
            except ValueError:
                return Response({
                    "success": False,
                    "error": "Invalid time format. Use HH:MM (24-hour format)"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Clear existing schedules
            schedule.clear()
            
            # Schedule daily run
            schedule.every().day.at(schedule_time).do(run_scheduled_pipeline)
            
            # Update state
            scheduler_state['enabled'] = True
            scheduler_state['time'] = schedule_time
            scheduler_state['timezone'] = timezone_name
            
            # Calculate next run
            if schedule.jobs:
                next_run_time = schedule.next_run()
                if next_run_time:
                    scheduler_state['next_run'] = next_run_time.isoformat()
            
            # Start scheduler thread if not running
            if scheduler_state['thread'] is None or not scheduler_state['thread'].is_alive():
                scheduler_state['thread'] = threading.Thread(target=run_scheduler_loop, daemon=True)
                scheduler_state['thread'].start()
            
            return Response({
                "success": True,
                "message": f"Scheduler enabled for daily runs at {schedule_time} {timezone_name}",
                "next_run": scheduler_state['next_run']
                }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class ECWSchedulerDisableView(APIView):
    """Disable scheduled pipeline runs"""
    authentication_classes = [IsAuthenticated]
    
    def post(self, request):
        """Disable scheduler"""
        try:
            if not SCHEDULER_AVAILABLE:
                return Response({
                    "success": False,
                    "error": "Scheduler library not available"
                }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
            
            # Disable scheduler
            scheduler_state['enabled'] = False
            scheduler_state['next_run'] = None
            schedule.clear()
            
            return Response({
                "success": True,
                "message": "Scheduler disabled successfully"
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# ============================================================================
# CONFIG EDITOR FUNCTIONALITY
# ============================================================================

@conditional_authentication
class ECWConfigEditorSaveView(APIView):
    """Save edited configuration with backup"""
    authentication_classes = [IsAuthenticated]
    
    def post(self, request):
        """Save configuration - accepts JSON config object"""
        try:
            if settings.INCREMENTAL_PROCESS_TYPE != "ecw_with_diff_script":
                return Response({
                    "success": False,
                    "error": f"Invalid incremental process type. Expected ecw_with_diff_script"
                }, status=status.HTTP_400_BAD_REQUEST)

            data = request.data
            new_config = data.get('config_content')
            
            if not new_config:
                return Response({
                    "success": False,
                    "error": "No configuration content provided"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # If config_content is a string, try to parse it as JSON
            if isinstance(new_config, str):
                try:
                    new_config = json.loads(new_config)
                except json.JSONDecodeError:
                    return Response({
                        "success": False,
                        "error": "Invalid JSON format in configuration"
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate config structure
            if not isinstance(new_config, dict):
                return Response({
                    "success": False,
                    "error": "Configuration must be a JSON object"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            scheduler_config = SchedulerConfig.objects.last()
            if scheduler_config is None:
                scheduler_config = SchedulerConfig.objects.create(run_config=new_config)
            else:
                scheduler_config.run_config = new_config
                scheduler_config.save()
            
            return Response({
                "success": True,
                "message": "Configuration updated successfully",
                "config": scheduler_config.run_config
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                "success": False,
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
