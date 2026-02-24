import traceback
from typing import TypedDict, Dict, Any
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from django.shortcuts import get_object_or_404
from django.db import transaction

from nd_api.models import Clients, ClientDataDump
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication
from phi_analyzer.models import PHIAnalysisSession, PHITableResult, PHIColumnResult
from phi_analyzer.tasks import create_phi_analysis_task
from ndwebsocket.utils import broadcast_task_status


class RequestCtx(TypedDict):
    sample_size: int
    model_name: str
    temperature: float
    max_token: int
    config: Dict[str, Any]


@conditional_authentication
class RunAnalyzerForDumpView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, client_id: int, dump_id: int):
        """
        Start PHI analysis for a specific dump
        
        This endpoint creates a background task to process PHI analysis
        and returns immediately with task information.
        """
        try:
            # Get request data
            data: RequestCtx = request.data
            # Validate client and dump exist
            client = get_object_or_404(Clients, id=client_id)
            dump = get_object_or_404(ClientDataDump, id=dump_id, client=client)
            
            
            # Create PHI analysis task
            result = create_phi_analysis_task(client_id, dump_id, data)
            
            if result['status'] == 'already_running':
                return Response(
                    {
                        "message": result['message'],
                        "session_id": result['session_id'],
                        "status": "already_running"
                    },
                    status=status.HTTP_200_OK
                )
            
            # Broadcast task creation
            broadcast_task_status(
                status="created",
                task_name=f"PHI Analysis: {client.client_name} - {dump.dump_name}",
                message=f"PHI analysis task created for {dump.dump_name}",
                data={
                    "session_id": result['session_id'],
                    "client_id": client_id,
                    "dump_id": dump_id
                }
            )
            
            return Response(
                {
                    "message": result['message'],
                    "session_id": result['session_id'],
                    "task_id": result['task_id'],
                    "chain_id": result['chain_id'],
                    "status": "created"
                },
                status=status.HTTP_201_CREATED
            )
        
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": f"Internal server error: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class PHIAnalysisStatusView(APIView):
    """Get status of PHI analysis session"""
    authentication_classes = [IsAuthenticated]

    def get(self, request, session_id: int):
        """
        Get current status of PHI analysis session
        
        Args:
            session_id: ID of the PHI analysis session
        """
        try:
            session = get_object_or_404(PHIAnalysisSession, id=session_id)
            
            # Get table results summary
            table_results = session.table_results.all()
            completed_tables = table_results.filter(status='completed').count()
            failed_tables = table_results.filter(status='failed').count()
            processing_tables = table_results.filter(status='processing').count()
            pending_tables = table_results.filter(status='pending').count()
            
            # Get recent progress logs
            recent_progress = session.progress_logs.order_by('-timestamp')[:10]
            progress_logs = [
                {
                    'step_name': log.step_name,
                    'progress_percentage': log.progress_percentage,
                    'message': log.message,
                    'timestamp': log.timestamp.isoformat(),
                    'details': log.details
                }
                for log in recent_progress
            ]
            
            response_data = {
                "session_id": session.id,
                "client_id": session.client.id,
                "client_name": session.client.client_name,
                "dump_id": session.dump.id,
                "dump_name": session.dump.dump_name,
                "status": session.status,
                "progress": session.progress,
                "current_step": session.current_step,
                "started_at": session.started_at.isoformat() if session.started_at else None,
                "completed_at": session.completed_at.isoformat() if session.completed_at else None,
                "duration": str(session.duration) if session.duration else None,
                "statistics": {
                    "total_tables": session.total_tables,
                    "processed_tables": session.processed_tables,
                    "completed_tables": completed_tables,
                    "failed_tables": failed_tables,
                    "processing_tables": processing_tables,
                    "pending_tables": pending_tables,
                    "total_columns": session.total_columns,
                    "phi_columns_found": session.phi_columns_found,
                    "errors_count": session.errors_count
                },
                "output_file_path": session.output_file_path,
                "error_message": session.error_message,
                "progress_logs": progress_logs,
                "config": session.config
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            nd_logger.error(f"Error getting PHI analysis status: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": f"Error getting analysis status: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class PHIAnalysisResultsView(APIView):
    """Get detailed results of PHI analysis session"""
    authentication_classes = [IsAuthenticated]

    def get(self, request, session_id: int):
        """
        Get detailed results of PHI analysis session
        
        Args:
            session_id: ID of the PHI analysis session
        """
        try:
            session = get_object_or_404(PHIAnalysisSession, id=session_id)
            
            # Get table results with column details
            table_results = session.table_results.select_related().prefetch_related('column_results').all()
            
            tables_data = []
            for table_result in table_results:
                columns_data = []
                for col_result in table_result.column_results.all():
                    columns_data.append({
                        'column_name': col_result.column_name,
                        'is_phi': col_result.is_phi,
                        'phi_rule': col_result.phi_rule,
                        'pipeline_remark': col_result.pipeline_remark,
                        'user_remarks': col_result.user_remarks,
                        'is_manually_verified': col_result.is_manually_verified,
                        'verified_by': col_result.verified_by,
                        'verified_at': col_result.verified_at.isoformat() if col_result.verified_at else None
                    })
                
                tables_data.append({
                    'table_name': table_result.table_name,
                    'table_index': table_result.table_index,
                    'status': table_result.status,
                    'progress': table_result.progress,
                    'total_columns': table_result.total_columns,
                    'phi_columns': table_result.phi_columns,
                    'started_at': table_result.started_at.isoformat() if table_result.started_at else None,
                    'completed_at': table_result.completed_at.isoformat() if table_result.completed_at else None,
                    'error_message': table_result.error_message,
                    'retry_count': table_result.retry_count,
                    'columns': columns_data
                })
            
            response_data = {
                "session_id": session.id,
                "client_name": session.client.client_name,
                "dump_name": session.dump.dump_name,
                "status": session.status,
                "progress": session.progress,
                "statistics": {
                    "total_tables": session.total_tables,
                    "processed_tables": session.processed_tables,
                    "total_columns": session.total_columns,
                    "phi_columns_found": session.phi_columns_found,
                    "errors_count": session.errors_count
                },
                "output_file_path": session.output_file_path,
                "tables": tables_data
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            nd_logger.error(f"Error getting PHI analysis results: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": f"Error getting analysis results: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class PHIAnalysisListView(APIView):
    """List PHI analysis sessions"""
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int = None, dump_id: int = None):
        """
        List PHI analysis sessions with optional filtering
        
        Args:
            client_id: Optional client ID to filter by
            dump_id: Optional dump ID to filter by
        """
        try:
            # Build query
            sessions = PHIAnalysisSession.objects.select_related('client', 'dump').all()
            
            if client_id:
                sessions = sessions.filter(client_id=client_id)
            if dump_id:
                sessions = sessions.filter(dump_id=dump_id)
            
            # Get pagination parameters
            page = int(request.GET.get('page', 1))
            page_size = int(request.GET.get('page_size', 20))
            offset = (page - 1) * page_size
            
            # Get total count
            total_count = sessions.count()
            
            # Get paginated results
            sessions = sessions[offset:offset + page_size]
            
            sessions_data = []
            for session in sessions:
                sessions_data.append({
                    'session_id': session.id,
                    'client_id': session.client.id,
                    'client_name': session.client.client_name,
                    'dump_id': session.dump.id,
                    'dump_name': session.dump.dump_name,
                    'status': session.status,
                    'progress': session.progress,
                    'current_step': session.current_step,
                    'created_at': session.created_at.isoformat(),
                    'started_at': session.started_at.isoformat() if session.started_at else None,
                    'completed_at': session.completed_at.isoformat() if session.completed_at else None,
                    'duration': str(session.duration) if session.duration else None,
                    'total_tables': session.total_tables,
                    'processed_tables': session.processed_tables,
                    'phi_columns_found': session.phi_columns_found,
                    'output_file_path': session.output_file_path,
                    'error_message': session.error_message
                })
            
            response_data = {
                'sessions': sessions_data,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_count': total_count,
                    'total_pages': (total_count + page_size - 1) // page_size
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            nd_logger.error(f"Error listing PHI analysis sessions: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": f"Error listing analysis sessions: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
