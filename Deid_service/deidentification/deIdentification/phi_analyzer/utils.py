"""
Utility functions for PHI analysis broadcasting and notifications
"""

import logging
from django.conf import settings
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from typing import Dict, Any, Optional, List
from django.utils import timezone
from .models import PHIAnalysisSession

logger = logging.getLogger(__name__)


def broadcast_phi_analysis_status(
    session_id: int,
    status: str,
    message: Optional[str] = None,
    progress: Optional[int] = None,
    current_step: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None
):
    """
    Broadcast PHI analysis status to all connected WebSocket clients
    
    Args:
        session_id: ID of the PHI analysis session
        status: Analysis status (e.g., 'started', 'in_progress', 'completed', 'failed')
        message: Optional status message
        progress: Optional progress percentage (0-100)
        current_step: Optional current processing step
        data: Optional additional data to send
    """
    try:
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_analysis_status",
            "session_id": session_id,
            "status": status,
            "timestamp": str(timezone.now())
        }
        
        if message:
            event_data["message"] = message
        if progress is not None:
            event_data["progress"] = progress
        if current_step:
            event_data["current_step"] = current_step
        if data:
            event_data.update(data)
            
        # Broadcast to general PHI analysis group
        async_to_sync(channel_layer.group_send)("phi_analysis_group", event_data)
        
        # Broadcast to session-specific group
        async_to_sync(channel_layer.group_send)(f"phi_session_{session_id}", event_data)
        
        logger.info(f"Broadcasted PHI analysis status: Session {session_id} - {status}")
        
    except Exception as e:
        logger.error(f"Error broadcasting PHI analysis status: {str(e)}")


def broadcast_phi_analysis_progress(
    session_id: int,
    progress: int,
    message: Optional[str] = None,
    current_step: Optional[str] = None,
    statistics: Optional[Dict[str, Any]] = None
):
    """
    Broadcast PHI analysis progress to all connected WebSocket clients
    
    Args:
        session_id: ID of the PHI analysis session
        progress: Progress percentage (0-100)
        message: Optional progress message
        current_step: Current processing step
        statistics: Optional statistics data
    """
    try:
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_analysis_progress",
            "session_id": session_id,
            "progress": progress,
            "timestamp": str(timezone.now())
        }
        
        if message:
            event_data["message"] = message
        if current_step:
            event_data["current_step"] = current_step
        if statistics:
            event_data["statistics"] = statistics
            
        # Broadcast to general PHI analysis group
        async_to_sync(channel_layer.group_send)("phi_analysis_group", event_data)
        
        # Broadcast to session-specific group
        async_to_sync(channel_layer.group_send)(f"phi_session_{session_id}", event_data)
        
        logger.debug(f"Broadcasted PHI analysis progress: Session {session_id} - {progress}%")
        
    except Exception as e:
        logger.error(f"Error broadcasting PHI analysis progress: {str(e)}")


def broadcast_phi_analysis_error(
    session_id: int,
    error: str,
    error_code: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
):
    """
    Broadcast PHI analysis error to all connected WebSocket clients
    
    Args:
        session_id: ID of the PHI analysis session
        error: Error message
        error_code: Optional error code
        details: Optional error details
    """
    try:
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_analysis_error",
            "session_id": session_id,
            "error": error,
            "timestamp": str(timezone.now())
        }
        
        if error_code:
            event_data["error_code"] = error_code
        if details:
            event_data["details"] = details
            
        # Broadcast to general PHI analysis group
        async_to_sync(channel_layer.group_send)("phi_analysis_group", event_data)
        
        # Broadcast to session-specific group
        async_to_sync(channel_layer.group_send)(f"phi_session_{session_id}", event_data)
        
        logger.error(f"Broadcasted PHI analysis error: Session {session_id} - {error}")
        
    except Exception as e:
        logger.error(f"Error broadcasting PHI analysis error: {str(e)}")


def broadcast_table_status_update(
    session_id: int,
    table_id: int,
    table_name: str,
    status: str,
    message: Optional[str] = None,
    error_details: Optional[Dict[str, Any]] = None
):
    """
    Broadcast table status updates specifically for PHI analysis UI real-time updates
    
    Args:
        session_id: ID of the PHI analysis session
        table_id: ID of the table result
        table_name: Name of the table
        status: Process status ('completed', 'failed', 'in_progress', 'pending')
        message: Optional status message
        error_details: Optional error details for failed processes
    """
    try:
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_table_status_update",
            "session_id": session_id,
            "table_id": table_id,
            "table_name": table_name,
            "status": status,
            "timestamp": str(timezone.now())
        }
        
        if message:
            event_data["message"] = message
        if error_details:
            event_data["error_details"] = error_details
            
        # Broadcast to general PHI analysis group
        async_to_sync(channel_layer.group_send)("phi_analysis_group", event_data)
        
        # Broadcast to session-specific group
        async_to_sync(channel_layer.group_send)(f"phi_session_{session_id}", event_data)
        
        logger.info(f"Broadcasted table status update: Session {session_id} - {table_name} - {status}")
        
    except Exception as e:
        logger.error(f"Error broadcasting table status update: {str(e)}")


def broadcast_bulk_table_status_update(
    session_id: int,
    updates: List[Dict[str, Any]]
):
    """
    Broadcast multiple table status updates at once for bulk operations
    
    Args:
        session_id: ID of the PHI analysis session
        updates: List of update dictionaries
    """
    try:
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_bulk_table_status_update",
            "session_id": session_id,
            "updates": updates,
            "timestamp": str(timezone.now())
        }
            
        # Broadcast to general PHI analysis group
        async_to_sync(channel_layer.group_send)("phi_analysis_group", event_data)
        
        # Broadcast to session-specific group
        async_to_sync(channel_layer.group_send)(f"phi_session_{session_id}", event_data)
        
        logger.info(f"Broadcasted bulk table status update: Session {session_id} - {len(updates)} updates")
        
    except Exception as e:
        logger.error(f"Error broadcasting bulk table status update: {str(e)}")


def broadcast_session_update(
    session_id: int,
    update_type: str,
    data: Dict[str, Any]
):
    """
    Broadcast session-specific updates
    
    Args:
        session_id: ID of the PHI analysis session
        update_type: Type of update
        data: Update data
    """
    try:
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_session_update",
            "session_id": session_id,
            "update_type": update_type,
            "timestamp": str(timezone.now()),
            **data
        }
            
        # Broadcast to session-specific group
        async_to_sync(channel_layer.group_send)(f"phi_session_{session_id}", event_data)
        
        logger.debug(f"Broadcasted session update: Session {session_id} - {update_type}")
        
    except Exception as e:
        logger.error(f"Error broadcasting session update: {str(e)}")


def broadcast_statistics_update(
    session_id: int,
    statistics: Dict[str, Any]
):
    """
    Broadcast statistics updates
    
    Args:
        session_id: ID of the PHI analysis session
        statistics: Statistics data
    """
    try:
        channel_layer = get_channel_layer()
        if not channel_layer:
            logger.error("Channel layer not available")
            return
            
        event_data = {
            "type": "send_statistics_update",
            "session_id": session_id,
            "statistics": statistics,
            "timestamp": str(timezone.now())
        }
            
        # Broadcast to general PHI analysis group
        async_to_sync(channel_layer.group_send)("phi_analysis_group", event_data)
        
        # Broadcast to session-specific group
        async_to_sync(channel_layer.group_send)(f"phi_session_{session_id}", event_data)
        
        logger.debug(f"Broadcasted statistics update: Session {session_id}")
        
    except Exception as e:
        logger.error(f"Error broadcasting statistics update: {str(e)}")


def get_session_statistics(session_id: int) -> Dict[str, Any]:
    """
    Get current statistics for a PHI analysis session
    
    Args:
        session_id: ID of the PHI analysis session
        
    Returns:
        Dictionary containing session statistics
    """
    try:
        session = PHIAnalysisSession.objects.get(id=session_id)
        
        # Get table results summary
        table_results = session.table_results.all()
        completed_tables = table_results.filter(status='completed').count()
        failed_tables = table_results.filter(status='failed').count()
        processing_tables = table_results.filter(status='processing').count()
        pending_tables = table_results.filter(status='pending').count()
        
        return {
            "total_tables": session.total_tables,
            "processed_tables": session.processed_tables,
            "completed_tables": completed_tables,
            "failed_tables": failed_tables,
            "processing_tables": processing_tables,
            "pending_tables": pending_tables,
            "total_columns": session.total_columns,
            "phi_columns_found": session.phi_columns_found,
            "validation_passed": session.validation_passed,
            "validation_failed": session.validation_failed,
            "errors_count": session.errors_count,
            "progress": session.progress,
            "current_step": session.current_step,
            "status": session.status
        }
        
    except PHIAnalysisSession.DoesNotExist:
        logger.error(f"PHI analysis session {session_id} not found")
        return {}
    except Exception as e:
        logger.error(f"Error getting session statistics: {str(e)}")
        return {}
