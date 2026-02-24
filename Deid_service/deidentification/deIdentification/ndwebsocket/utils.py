import logging
from django.conf import settings
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from typing import Dict, Any, Optional
from django.utils import timezone
from .models import Notification, NotificationType, NotificationPriority

logger = logging.getLogger(__name__)


def save_notification_to_db(
    title: str,
    message: str,
    task_name: Optional[str] = None,
    notification_type: str = NotificationType.INFO,
    priority: str = NotificationPriority.MEDIUM,
    progress: Optional[int] = None,
    error: Optional[str] = None,
    error_code: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    expires_at: Optional[timezone.datetime] = None
):
    """
    Save notification to database (no user authentication)
    
    Args:
        title: Notification title
        message: Notification message
        task_name: Name of the task
        notification_type: Type of notification
        priority: Notification priority
        progress: Progress percentage (0-100)
        error: Error message
        error_code: Error code
        details: Additional details as JSON
        expires_at: When this notification expires
    """
    try:
        notification = Notification.objects.create(
            title=title,
            message=message,
            task_name=task_name,
            notification_type=notification_type,
            priority=priority,
            progress=progress,
            error_code=error_code,
            details=details,
            expires_at=expires_at
        )
        logger.info(f"Saved notification to database: {notification.id}")
        return notification
    except Exception as e:
        logger.error(f"Error saving notification to database: {str(e)}")
        return None


def broadcast_task_status(
    status: str,
    task_name: str,
    message: Optional[str] = None,
    progress: Optional[int] = None,
    error: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
    priority: str = NotificationPriority.MEDIUM,
    notification_type: str = NotificationType.TASK_STATUS,
    save_to_db: bool = True
):
    """
    Broadcast task status to all connected WebSocket clients and optionally save to database
    
    Args:
        status: Task status (e.g., 'started', 'in_progress', 'completed', 'failed')
        task_name: Name of the task
        message: Optional status message
        progress: Optional progress percentage (0-100)
        error: Optional error message
        data: Optional additional data to send
        priority: Notification priority
        notification_type: Type of notification
        save_to_db: Whether to save notification to database
    """
    try:
        # Save to database if enabled
        if save_to_db and getattr(settings, 'WEBSOCKET_EVENT_ENABLED', True):
            save_notification_to_db(
                title=f"Task {status.title()}: {task_name}",
                message=message or f"Task {task_name} status: {status}",
                task_name=task_name,
                notification_type=notification_type,
                priority=priority,
                progress=progress,
                error=error,
                details=data
            )
        
        # Broadcast via WebSocket
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_task_status",
            "status": status,
            "task_name": task_name,
            "timestamp": str(timezone.now())
        }
        
        if message:
            event_data["message"] = message
        if progress is not None:
            event_data["progress"] = progress
        if error:
            event_data["error"] = error
        if data:
            event_data.update(data)
            
        async_to_sync(channel_layer.group_send)("task_group", event_data)
        logger.info(f"Broadcasted task status: {task_name} - {status}")
        
    except Exception as e:
        logger.error(f"Error broadcasting task status: {str(e)}")


def broadcast_task_progress(
    task_name: str,
    progress: int,
    message: Optional[str] = None,
    current_step: Optional[str] = None,
    total_steps: Optional[int] = None,
    save_to_db: bool = True
):
    """
    Broadcast task progress to all connected WebSocket clients and optionally save to database
    
    Args:
        task_name: Name of the task
        progress: Progress percentage (0-100)
        message: Optional progress message
        current_step: Current step being executed
        total_steps: Total number of steps
        save_to_db: Whether to save notification to database
    """
    try:
        # Save to database if enabled
        if save_to_db and getattr(settings, 'WEBSOCKET_EVENT_ENABLED', True):
            details = {}
            if current_step:
                details['current_step'] = current_step
            if total_steps:
                details['total_steps'] = total_steps
                
            save_notification_to_db(
                title=f"Progress Update: {task_name}",
                message=message or f"Task {task_name} progress: {progress}%",
                task_name=task_name,
                notification_type=NotificationType.TASK_PROGRESS,
                priority=NotificationPriority.LOW,
                progress=progress,
                details=details if details else None
            )
        
        # Broadcast via WebSocket
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_task_progress",
            "task_name": task_name,
            "progress": progress,
            "timestamp": str(timezone.now())
        }
        
        if message:
            event_data["message"] = message
        if current_step:
            event_data["current_step"] = current_step
        if total_steps:
            event_data["total_steps"] = total_steps
            
        async_to_sync(channel_layer.group_send)("task_group", event_data)
        logger.debug(f"Broadcasted task progress: {task_name} - {progress}%")
        
    except Exception as e:
        logger.error(f"Error broadcasting task progress: {str(e)}")


def broadcast_task_error(
    task_name: str,
    error: str,
    error_code: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    save_to_db: bool = True
):
    """
    Broadcast task error to all connected WebSocket clients and optionally save to database
    
    Args:
        task_name: Name of the task
        error: Error message
        error_code: Optional error code
        details: Optional error details
        save_to_db: Whether to save notification to database
    """
    try:
        # Save to database if enabled
        if save_to_db and getattr(settings, 'WEBSOCKET_EVENT_ENABLED', True):
            save_notification_to_db(
                title=f"Error in Task: {task_name}",
                message=error,
                task_name=task_name,
                notification_type=NotificationType.TASK_ERROR,
                priority=NotificationPriority.HIGH,
                error=error,
                error_code=error_code,
                details=details
            )
        
        # Broadcast via WebSocket
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_task_error",
            "task_name": task_name,
            "error": error,
            "timestamp": str(timezone.now())
        }
        
        if error_code:
            event_data["error_code"] = error_code
        if details:
            event_data["details"] = details
            
        async_to_sync(channel_layer.group_send)("task_group", event_data)
        logger.error(f"Broadcasted task error: {task_name} - {error}")
        
    except Exception as e:
        logger.error(f"Error broadcasting task error: {str(e)}")


def broadcast_to_specific_group(
    group_name: str,
    event_type: str,
    data: Dict[str, Any]
):
    """
    Broadcast to a specific group of WebSocket clients
    
    Args:
        group_name: Name of the group to broadcast to
        event_type: Type of event to send
        data: Data to send
    """
    try:
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": event_type,
            **data
        }
        
        async_to_sync(channel_layer.group_send)(group_name, event_data)
        logger.info(f"Broadcasted to group {group_name}: {event_type}")
        
    except Exception as e:
        logger.error(f"Error broadcasting to group {group_name}: {str(e)}")


def broadcast_table_status_update(
    table_id: int,
    table_name: str,
    process_type: str,  # 'deid', 'qc', 'gcp', 'embd'
    status: str,  # 'completed', 'failed', 'in_progress', 'not_started'
    message: Optional[str] = None,
    error_details: Optional[Dict[str, Any]] = None,
    save_to_db: bool = False  # Don't save to DB for UI updates
):
    """
    Broadcast table status updates specifically for UI real-time updates
    This function sends structured data that the frontend can use to update table statuses
    
    Args:
        table_id: ID of the table
        table_name: Name of the table
        process_type: Type of process ('deid', 'qc', 'gcp', 'embd')
        status: Process status ('completed', 'failed', 'in_progress', 'not_started')
        message: Optional status message
        error_details: Optional error details for failed processes
        save_to_db: Whether to save to database (default False for UI updates)
    """
    try:
        # Don't save to database for UI updates to avoid noise
        if save_to_db and getattr(settings, 'WEBSOCKET_EVENT_ENABLED', True):
            save_notification_to_db(
                title=f"Table {process_type.upper()} Update: {table_name}",
                message=message or f"Table {table_name} {process_type} status: {status}",
                task_name=f"Table {process_type}: {table_name}",
                notification_type=NotificationType.TASK_STATUS,
                priority=NotificationPriority.MEDIUM,
                details={
                    "table_id": table_id,
                    "table_name": table_name,
                    "process_type": process_type,
                    "status": status,
                    "error_details": error_details
                }
            )
        
        # Broadcast via WebSocket
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_table_status_update",
            "table_id": table_id,
            "table_name": table_name,
            "process_type": process_type,
            "status": status,
            "timestamp": str(timezone.now())
        }
        
        if message:
            event_data["message"] = message
        if error_details:
            event_data["error_details"] = error_details
            
        async_to_sync(channel_layer.group_send)("task_group", event_data)
        logger.info(f"Broadcasted table status update: {table_name} - {process_type}: {status}")
        
    except Exception as e:
        logger.error(f"Error broadcasting table status update: {str(e)}")


def broadcast_table_bulk_status_update(
    updates: list,  # List of dicts with table_id, table_name, process_type, status
    save_to_db: bool = False
):
    """
    Broadcast multiple table status updates at once for bulk operations
    
    Args:
        updates: List of update dictionaries
        save_to_db: Whether to save to database
    """
    try:
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_bulk_table_status_update",
            "updates": updates,
            "timestamp": str(timezone.now())
        }
            
        async_to_sync(channel_layer.group_send)("task_group", event_data)
        logger.info(f"Broadcasted bulk table status update: {len(updates)} updates")
        
    except Exception as e:
        logger.error(f"Error broadcasting bulk table status update: {str(e)}")


# Backward compatibility
def broadcast():
    """Legacy function for backward compatibility"""
    broadcast_task_status(
        status="completed",
        task_name="deidentification_of_table"
    )