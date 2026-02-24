"""
Background task functions for PHI analysis processing
"""

import os
import json
import logging
import traceback
from typing import Dict, Any, List, Optional
from django.db import transaction
from django.utils import timezone
from django.conf import settings

from worker.models import Task, Chain
from worker.utils import get_expiry
from nd_api.models import Clients, ClientDataDump
from phi_analyzer.models import (
    PHIAnalysisSession, PHITableResult, PHIColumnResult, PHIAnalysisProgress
)
from phi_analyzer.pipeline.phi_pipeline import PHIDeidentificationPipelineOptimized
from ndwebsocket.utils import (
    broadcast_task_status, broadcast_task_progress, broadcast_task_error
)
from phi_analyzer.utils import (
    broadcast_phi_analysis_status, broadcast_phi_analysis_progress, 
    broadcast_phi_analysis_error, broadcast_table_status_update,
    broadcast_statistics_update, get_session_statistics
)
from deIdentification.nd_logger import nd_logger

logger = logging.getLogger(__name__)


def start_phi_analysis_task(session_id: int, dependencies: List[Task] = None):
    """
    Main task function to start PHI analysis
    
    Args:
        session_id: ID of the PHI analysis session
        dependencies: List of dependency tasks (unused for now)
    """
    try:
        # Get the analysis session
        session = PHIAnalysisSession.objects.get(id=session_id)
        
        # Mark session as started
        session.mark_started()
        
        # Broadcast start notification
        broadcast_phi_analysis_status(
            session_id=session_id,
            status="started",
            message=f"Starting PHI analysis for {session.dump.dump_name}",
            data={"client_id": session.client.id, "dump_id": session.dump.id}
        )
        
        # Log progress
        PHIAnalysisProgress.objects.create(
            session=session,
            step_name="Initialization",
            step_index=1,
            progress_percentage=5,
            message="Starting PHI analysis pipeline",
            details={"session_id": session_id}
        )
        
        # Initialize pipeline with session-specific configuration
        config = session.config or {}
        json_dir = os.path.join(settings.MEDIA_ROOT, 'phi_analysis', f'session_{session_id}')
        os.makedirs(json_dir, exist_ok=True)
        
        # Update progress
        session.update_progress(10, "Initializing pipeline")
        broadcast_phi_analysis_progress(
            session_id=session_id,
            progress=10,
            message="Initializing PHI analysis pipeline"
        )
        
        # Create pipeline instance
        pipeline = PHIDeidentificationPipelineOptimized(
            client_id=session.client.id,
            dump_id=session.dump.id,
            llm_config=config,
            json_dir=json_dir
        )
        
        # Get tables to analyze
        tables = pipeline._get_tables_to_analyze()
        session.total_tables = len(tables)
        session.save(update_fields=['total_tables'])
        
        # Create table result records
        table_results = []
        for idx, table_name in enumerate(tables):
            table_result = PHITableResult.objects.create(
                session=session,
                table_name=table_name,
                table_index=idx,
                total_columns=0  # Will be updated during processing
            )
            table_results.append(table_result)
        
        # Log progress
        PHIAnalysisProgress.objects.create(
            session=session,
            step_name="Table Discovery",
            step_index=2,
            progress_percentage=15,
            message=f"Found {len(tables)} tables to analyze",
            details={"tables": tables}
        )
        
        # Update progress
        session.update_progress(15, f"Found {len(tables)} tables to analyze")
        broadcast_phi_analysis_progress(
            session_id=session_id,
            progress=15,
            message=f"Found {len(tables)} tables to analyze"
        )
        
        # Process tables in batches for better progress tracking
        batch_size = 5  # Process 5 tables at a time
        total_batches = (len(tables) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(tables))
            batch_tables = tables[start_idx:end_idx]
            
            # Process batch
            _process_table_batch(session, table_results[start_idx:end_idx], batch_tables, pipeline)
            
            # Update progress
            progress = 20 + (batch_idx + 1) * 70 // total_batches
            session.update_progress(progress, f"Processed batch {batch_idx + 1}/{total_batches}")
            broadcast_phi_analysis_progress(
                session_id=session_id,
                progress=progress,
                message=f"Processed batch {batch_idx + 1}/{total_batches} ({len(batch_tables)} tables)"
            )
        
        # Generate final output
        output_path = _generate_final_output(session, json_dir)
        
        # Mark session as completed
        session.mark_completed(output_path)
        
        # Log final progress
        PHIAnalysisProgress.objects.create(
            session=session,
            step_name="Completion",
            step_index=100,
            progress_percentage=100,
            message="PHI analysis completed successfully",
            details={
                "total_tables": session.total_tables,
                "processed_tables": session.processed_tables,
                "phi_columns_found": session.phi_columns_found,
                "output_file": output_path
            }
        )
        
        # Broadcast completion
        broadcast_phi_analysis_status(
            session_id=session_id,
            status="completed",
            message=f"PHI analysis completed successfully. Found {session.phi_columns_found} PHI columns.",
            progress=100,
            data={
                "client_id": session.client.id,
                "dump_id": session.dump.id,
                "output_file": output_path,
                "statistics": {
                    "total_tables": session.total_tables,
                    "processed_tables": session.processed_tables,
                    "phi_columns_found": session.phi_columns_found
                }
            }
        )
        
        nd_logger.info(f"PHI analysis completed successfully for session {session_id}")
        return {"status": "completed", "session_id": session_id, "output_file": output_path}
        
    except Exception as e:
        error_msg = f"PHI analysis failed: {str(e)}"
        nd_logger.error(f"PHI analysis task failed for session {session_id}: {error_msg}")
        nd_logger.error(traceback.format_exc())
        
        # Mark session as failed
        try:
            session = PHIAnalysisSession.objects.get(id=session_id)
            session.mark_failed(error_msg)
            
            # Log error progress
            PHIAnalysisProgress.objects.create(
                session=session,
                step_name="Error",
                step_index=999,
                progress_percentage=0,
                message=error_msg,
                details={"error": str(e), "traceback": traceback.format_exc()}
            )
            
            # Broadcast error
            broadcast_phi_analysis_error(
                session_id=session_id,
                error=error_msg,
                details={"error": str(e)}
            )
        except Exception as cleanup_error:
            nd_logger.error(f"Error during cleanup: {str(cleanup_error)}")
        
        raise


def _process_table_batch(session: PHIAnalysisSession, table_results: List[PHITableResult], 
                        table_names: List[str], pipeline: PHIDeidentificationPipelineOptimized):
    """
    Process a batch of tables
    
    Args:
        session: PHI analysis session
        table_results: List of table result objects
        table_names: List of table names to process
        pipeline: PHI pipeline instance
    """
    try:
        for table_result, table_name in zip(table_results, table_names):
            # Mark table as started
            table_result.mark_started()
            
            # Broadcast table status
            broadcast_table_status_update(
                session_id=session.id,
                table_id=table_result.id,
                table_name=table_name,
                status='in_progress',
                message=f"Processing table {table_name}"
            )
            
            try:
                # Process single table using pipeline
                result = pipeline.process_single_table(table_name)
                
                # Update table statistics
                table_result.total_columns = result.get('total_columns', 0)
                table_result.phi_columns = result.get('phi_columns', 0)
                
                # Store column results
                column_results = result.get('results', [])
                for col_data in column_results:
                    PHIColumnResult.objects.create(
                        table_result=table_result,
                        column_name=col_data.get('column_name', ''),
                        is_phi=col_data.get('is_phi', 'unknown'),
                        phi_rule=col_data.get('phi_rule', ''),
                        pipeline_remark=col_data.get('pipeline_remark', '')
                    )
                
                # Update table statistics from column results
                phi_columns = table_result.column_results.filter(is_phi='yes').count()
                
                table_result.phi_columns = phi_columns
                table_result.mark_completed()
                
                # Update session statistics
                session.processed_tables += 1
                session.phi_columns_found += phi_columns
                session.total_columns += table_result.total_columns
                session.save(update_fields=[
                    'processed_tables', 'phi_columns_found', 'total_columns'
                ])
                
                # Broadcast table completion
                broadcast_table_status_update(
                    session_id=session.id,
                    table_id=table_result.id,
                    table_name=table_name,
                    status='completed',
                    message=f"Completed processing table {table_name} - Found {phi_columns} PHI columns"
                )
                
                nd_logger.info(f"Completed processing table {table_name} - {phi_columns} PHI columns found")
                
            except Exception as table_error:
                error_msg = f"Failed to process table {table_name}: {str(table_error)}"
                nd_logger.error(error_msg)
                nd_logger.error(traceback.format_exc())
                
                table_result.mark_failed(error_msg)
                session.errors_count += 1
                session.save(update_fields=['errors_count'])
                
                # Broadcast table failure
                broadcast_table_status_update(
                    session_id=session.id,
                    table_id=table_result.id,
                    table_name=table_name,
                    status='failed',
                    message=error_msg,
                    error_details={"error": str(table_error)}
                )
                
    except Exception as e:
        nd_logger.error(f"Error processing table batch: {str(e)}")
        raise


def _generate_final_output(session: PHIAnalysisSession, json_dir: str) -> str:
    """
    Generate final CSV output file
    
    Args:
        session: PHI analysis session
        json_dir: Directory containing JSON results
        
    Returns:
        Path to generated CSV file
    """
    try:
        import pandas as pd
        from django.conf import settings
        
        # Collect all column results
        column_results = PHIColumnResult.objects.filter(
            table_result__session=session
        ).select_related('table_result').order_by('table_result__table_index', 'column_name')
        
        # Convert to DataFrame
        data = []
        for col_result in column_results:
            data.append({
                'database_name': session.client.name,
                'table_name': col_result.table_result.table_name,
                'column_name': col_result.column_name,
                'is_phi': col_result.is_phi,
                'phi_rule': col_result.phi_rule,
                'pipeline_remark': col_result.pipeline_remark,
                'user_remarks': col_result.user_remarks,
                'is_manually_verified': col_result.is_manually_verified,
                'verified_by': col_result.verified_by,
                'verified_at': col_result.verified_at.isoformat() if col_result.verified_at else '',
                'created_at': col_result.created_at.isoformat()
            })
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Generate output file path
        output_dir = os.path.join(settings.MEDIA_ROOT, 'phi_analysis_results')
        os.makedirs(output_dir, exist_ok=True)
        
        filename = f"phi_analysis_session_{session.id}_{session.client.name}_{session.dump.dump_name}.csv"
        output_path = os.path.join(output_dir, filename)
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        
        nd_logger.info(f"Generated final output: {output_path}")
        return output_path
        
    except Exception as e:
        nd_logger.error(f"Error generating final output: {str(e)}")
        raise


def create_phi_analysis_task(client_id: int, dump_id: int, config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Create a PHI analysis task for the given client and dump
    
    Args:
        client_id: ID of the client
        dump_id: ID of the data dump
        config: Analysis configuration parameters
        
    Returns:
        Dictionary containing task creation result
    """
    try:
        # Get client and dump
        client = Clients.objects.get(id=client_id)
        dump = ClientDataDump.objects.get(id=dump_id, client=client)
        
        # Check if there's already a running analysis for this dump
        existing_session = PHIAnalysisSession.objects.filter(
            client=client, dump=dump, status__in=['pending', 'running']
        ).first()
        
        if existing_session:
            return {
                "status": "already_running",
                "message": f"PHI analysis already running for dump {dump.dump_name}",
                "session_id": existing_session.id
            }
        
        # Create analysis session
        session = PHIAnalysisSession.objects.create(
            client=client,
            dump=dump,
            config=config or {}
        )
        
        # Create task chain
        chain, created = Chain.all_objects.get_or_create(
            reference_uuid=f"phi_analysis_{session.id}"
        )
        if not created:
            chain.revive_and_save()
        
        # Create main analysis task
        analysis_task = Task.create_task(
            chain=chain,
            fn=start_phi_analysis_task,
            arguments={"session_id": session.id},
            dependencies=[],
            timeout=7200,  # 2 hours timeout
            max_failure_count=3
        )
        
        # Update session with task chain reference
        session.task_chain = chain
        session.save(update_fields=['task_chain'])
        
        nd_logger.info(f"Created PHI analysis task for session {session.id}")
        
        return {
            "status": "created",
            "message": "PHI analysis task created successfully",
            "session_id": session.id,
            "task_id": analysis_task.id,
            "chain_id": chain.id
        }
        
    except Exception as e:
        error_msg = f"Failed to create PHI analysis task: {str(e)}"
        nd_logger.error(error_msg)
        nd_logger.error(traceback.format_exc())
        raise Exception(error_msg)
