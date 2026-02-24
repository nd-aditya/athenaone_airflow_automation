import csv
import io
import traceback
from typing import Dict, Any, List
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from django.shortcuts import get_object_or_404
from django.db import transaction
from django.conf import settings

from nd_api.models import Clients, ClientDataDump
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication
from phi_analyzer.models import PHIAnalysisSession, PHITableResult, PHIColumnResult


@conditional_authentication
class CSVImportView(APIView):
    """Import CSV results for PHI analysis"""
    authentication_classes = [IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request, client_id: int, dump_id: int):
        """
        Import CSV file with PHI analysis results
        
        Expected CSV format:
        TABLE_NAME,COLUMN_NAME,IS_PHI,DE_IDENTIFICATION_RULE,MASK_VALUE,PIPELINE_REMARK
        """
        try:
            # Validate client and dump exist
            client = get_object_or_404(Clients, id=client_id)
            dump = get_object_or_404(ClientDataDump, id=dump_id, client=client)
            
            # Check if CSV file is provided
            if 'csv_file' not in request.FILES:
                return Response(
                    {"error": "CSV file is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            csv_file = request.FILES['csv_file']
            
            # Validate file type
            if not csv_file.name.endswith('.csv'):
                return Response(
                    {"error": "File must be a CSV file"},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Read and parse CSV
            try:
                csv_content = csv_file.read().decode('utf-8')
                csv_reader = csv.DictReader(io.StringIO(csv_content))
                
                # Validate required columns
                required_columns = ['TABLE_NAME', 'COLUMN_NAME', 'IS_PHI', 'DE_IDENTIFICATION_RULE', 'MASK_VALUE', 'PIPELINE_REMARK']
                if not all(col in csv_reader.fieldnames for col in required_columns):
                    missing_columns = [col for col in required_columns if col not in csv_reader.fieldnames]
                    return Response(
                        {"error": f"Missing required columns: {', '.join(missing_columns)}"},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                
                # Parse CSV data
                csv_data = list(csv_reader)
                if not csv_data:
                    return Response(
                        {"error": "CSV file is empty"},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                
                # Create a new analysis session for imported data
                session = PHIAnalysisSession.objects.create(
                    client=client,
                    dump=dump,
                    config={"imported": True, "source_file": csv_file.name},
                    status='completed',  # Mark as completed since it's imported data
                    progress=100,
                    current_step='Imported from CSV'
                )
                
                # Process CSV data and create results
                with transaction.atomic():
                    # Group data by table name
                    tables_data = {}
                    for row in csv_data:
                        table_name = row['TABLE_NAME']
                        if table_name not in tables_data:
                            tables_data[table_name] = []
                        tables_data[table_name].append(row)
                    
                    # Create table results
                    table_results = []
                    for idx, (table_name, columns_data) in enumerate(tables_data.items()):
                        table_result = PHITableResult.objects.create(
                            session=session,
                            table_name=table_name,
                            table_index=idx,
                            status='completed',
                            progress=100,
                            total_columns=len(columns_data),
                            phi_columns=len([col for col in columns_data if col['IS_PHI'].lower() in ['yes', 'true', '1']]),
                        )
                        table_results.append(table_result)
                        
                        # Create column results
                        phi_columns = 0
                        for col_idx, col_data in enumerate(columns_data):
                            is_phi = col_data['IS_PHI'].lower().strip() in ['yes', 'true', '1']
                            phi_rule = col_data.get('DE_IDENTIFICATION_RULE', '')
                            mask_value = col_data.get('MASK_VALUE', '')
                            pipeline_remark = col_data.get('PIPELINE_REMARK', '')
                            
                            if is_phi:
                                phi_columns += 1
                            
                            PHIColumnResult.objects.create(
                                table_result=table_result,
                                column_name=col_data['COLUMN_NAME'],
                                is_phi='yes' if is_phi else 'no',
                                phi_rule=phi_rule,
                                pipeline_remark=pipeline_remark
                            )
                        
                        # Update table statistics
                        table_result.phi_columns = phi_columns
                        table_result.save(update_fields=['phi_columns'])
                    
                    # Update session statistics
                    session.total_tables = len(tables_data)
                    session.processed_tables = len(tables_data)
                    session.total_columns = sum(tr.total_columns for tr in table_results)
                    session.phi_columns_found = sum(tr.phi_columns for tr in table_results)
                    session.save(update_fields=[
                        'total_tables', 'processed_tables', 'total_columns', 
                        'phi_columns_found'
                    ])
                
                # Generate output file path for consistency
                import os
                output_dir = os.path.join(settings.MEDIA_ROOT, 'phi_analysis_results')
                os.makedirs(output_dir, exist_ok=True)
                filename = f"phi_analysis_imported_{session.id}_{client.client_name}_{dump.dump_name}.csv"
                output_path = os.path.join(output_dir, filename)
                
                # Save imported data as CSV for consistency
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Table Name', 'Column Name', 'Is PHI', 'PHI Rule', 'Pipeline Remark', 'User Remarks', 'Manually Verified', 'Verified By', 'Verified At'])
                    
                    for table_result in table_results:
                        for col_result in table_result.column_results.all():
                            writer.writerow([
                                table_result.table_name,
                                col_result.column_name,
                                col_result.is_phi,
                                col_result.phi_rule,
                                col_result.pipeline_remark,
                                col_result.user_remarks,
                                col_result.is_manually_verified,
                                col_result.verified_by,
                                col_result.verified_at.isoformat() if col_result.verified_at else ''
                            ])
                
                session.output_file_path = output_path
                session.save(update_fields=['output_file_path'])
                
                nd_logger.info(f"Successfully imported CSV data for session {session.id}")
                
                return Response({
                    "message": "CSV imported successfully",
                    "session_id": session.id,
                    "statistics": {
                        "total_tables": session.total_tables,
                        "total_columns": session.total_columns,
                        "phi_columns_found": session.phi_columns_found
                    }
                }, status=status.HTTP_201_CREATED)
                
            except Exception as csv_error:
                nd_logger.error(f"Error parsing CSV: {str(csv_error)}")
                return Response(
                    {"error": f"Error parsing CSV file: {str(csv_error)}"},
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        except Exception as e:
            nd_logger.error(f"Error importing CSV: {str(e)}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"error": f"Internal server error: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
