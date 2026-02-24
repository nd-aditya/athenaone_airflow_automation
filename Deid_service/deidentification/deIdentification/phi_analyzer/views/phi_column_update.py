"""
PHI Column Update Views
Handles updating PHI column results including IS_PHI, PHI_RULE, user remarks, and manual verification
"""

from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

from phi_analyzer.models.phimodels import PHIColumnResult, PHITableResult, PHIAnalysisSession
from nd_api.decorator import conditional_authentication


@conditional_authentication
class UpdatePHIColumnView(APIView):
    """Update PHI column classification and user remarks"""
    authentication_classes = [IsAuthenticated]

    def put(self, request, session_id: int, table_name: str, column_name: str):
        """
        Update PHI column results
        
        Expected JSON body:
        {
            "is_phi": "yes|no|unknown",
            "phi_rule": "rule_name",
            "user_remarks": "user comments"
        }
        """
        try:
            # Get the session and validate access
            session = get_object_or_404(PHIAnalysisSession, id=session_id)
            
            # Get the table result
            table_result = get_object_or_404(
                PHITableResult, 
                session=session, 
                table_name=table_name
            )
            
            # Get the column result
            column_result = get_object_or_404(
                PHIColumnResult,
                table_result=table_result,
                column_name=column_name
            )
            
            # Get request data
            data = request.data
            updated_fields = []
            
            # Update is_phi if provided
            if 'is_phi' in data:
                new_is_phi = data['is_phi']
                if new_is_phi in ['yes', 'no', 'unknown']:
                    column_result.is_phi = new_is_phi
                    updated_fields.append('is_phi')
                else:
                    return Response(
                        {"error": "is_phi must be 'yes', 'no', or 'unknown'"},
                        status=status.HTTP_400_BAD_REQUEST
                    )
            
            # Update phi_rule if provided
            if 'phi_rule' in data:
                column_result.phi_rule = data['phi_rule']
                updated_fields.append('phi_rule')
            
            # Update user_remarks if provided
            if 'user_remarks' in data:
                column_result.user_remarks = data['user_remarks']
                updated_fields.append('user_remarks')
            
            # Always update the updated_at field
            updated_fields.extend(['updated_at'])
            
            # Save the changes
            column_result.save(update_fields=updated_fields)
            
            # Return updated column data
            return Response({
                "success": True,
                "message": "Column updated successfully",
                "column": {
                    "column_name": column_result.column_name,
                    "is_phi": column_result.is_phi,
                    "phi_rule": column_result.phi_rule,
                    "pipeline_remark": column_result.pipeline_remark,
                    "user_remarks": column_result.user_remarks,
                    "is_manually_verified": column_result.is_manually_verified,
                    "verified_by": column_result.verified_by,
                    "verified_at": column_result.verified_at.isoformat() if column_result.verified_at else None,
                    "updated_at": column_result.updated_at.isoformat()
                }
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {"error": f"Failed to update column: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


@conditional_authentication
class UpdateManualVerificationView(APIView):
    """Update manual verification status for a table"""
    authentication_classes = [IsAuthenticated]

    def put(self, request, session_id: int, table_name: str):
        """
        Update manual verification status for all columns in a table
        
        Expected JSON body:
        {
            "is_manually_verified": true,
            "verified_by": "username"
        }
        """
        try:
            # Get the session and validate access
            session = get_object_or_404(PHIAnalysisSession, id=session_id)
            
            # Get the table result
            table_result = get_object_or_404(
                PHITableResult, 
                session=session, 
                table_name=table_name
            )
            
            # Get request data
            data = request.data
            is_verified = data.get('is_manually_verified', False)
            verified_by = data.get('verified_by', '')
            
            # Get all columns for this table
            columns = PHIColumnResult.objects.filter(table_result=table_result)
            
            # Update verification status for all columns
            update_fields = {
                'is_manually_verified': is_verified,
                'verified_by': verified_by if is_verified else '',
                'verified_at': timezone.now() if is_verified else None,
                'updated_at': timezone.now()
            }
            
            # Bulk update all columns
            columns.update(**update_fields)
            
            # Get updated column count
            updated_count = columns.count()
            
            return Response({
                "success": True,
                "message": f"Manual verification status updated for {updated_count} columns",
                "table_name": table_name,
                "is_manually_verified": is_verified,
                "verified_by": verified_by,
                "updated_columns": updated_count
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {"error": f"Failed to update manual verification: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


@conditional_authentication
class GetPHIColumnView(APIView):
    """Get PHI column details"""
    authentication_classes = [IsAuthenticated]

    def get(self, request, session_id: int, table_name: str, column_name: str):
        """Get detailed information about a specific PHI column"""
        try:
            # Get the session and validate access
            session = get_object_or_404(PHIAnalysisSession, id=session_id)
            
            # Get the table result
            table_result = get_object_or_404(
                PHITableResult, 
                session=session, 
                table_name=table_name
            )
            
            # Get the column result
            column_result = get_object_or_404(
                PHIColumnResult,
                table_result=table_result,
                column_name=column_name
            )
            
            # Return column data
            return Response({
                "column": {
                    "column_name": column_result.column_name,
                    "is_phi": column_result.is_phi,
                    "phi_rule": column_result.phi_rule,
                    "pipeline_remark": column_result.pipeline_remark,
                    "user_remarks": column_result.user_remarks,
                    "is_manually_verified": column_result.is_manually_verified,
                    "verified_by": column_result.verified_by,
                    "verified_at": column_result.verified_at.isoformat() if column_result.verified_at else None,
                    "created_at": column_result.created_at.isoformat(),
                    "updated_at": column_result.updated_at.isoformat()
                }
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {"error": f"Failed to get column details: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


@conditional_authentication
class GetTableVerificationStatusView(APIView):
    """Get manual verification status for a table"""
    authentication_classes = [IsAuthenticated]

    def get(self, request, session_id: int, table_name: str):
        """Get manual verification status summary for a table"""
        try:
            # Get the session and validate access
            session = get_object_or_404(PHIAnalysisSession, id=session_id)
            
            # Get the table result
            table_result = get_object_or_404(
                PHITableResult, 
                session=session, 
                table_name=table_name
            )
            
            # Get verification status for all columns
            columns = PHIColumnResult.objects.filter(table_result=table_result)
            total_columns = columns.count()
            verified_columns = columns.filter(is_manually_verified=True).count()
            
            # Check if all columns are verified
            is_fully_verified = verified_columns == total_columns and total_columns > 0
            
            # Get verification details if any columns are verified
            verification_details = None
            if verified_columns > 0:
                sample_verified = columns.filter(is_manually_verified=True).first()
                if sample_verified:
                    verification_details = {
                        "verified_by": sample_verified.verified_by,
                        "verified_at": sample_verified.verified_at.isoformat() if sample_verified.verified_at else None
                    }
            
            return Response({
                "table_name": table_name,
                "total_columns": total_columns,
                "verified_columns": verified_columns,
                "is_fully_verified": is_fully_verified,
                "verification_percentage": (verified_columns / total_columns * 100) if total_columns > 0 else 0,
                "verification_details": verification_details
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response(
                {"error": f"Failed to get verification status: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
