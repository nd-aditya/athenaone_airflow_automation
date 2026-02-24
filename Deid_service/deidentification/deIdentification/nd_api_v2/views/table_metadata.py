from django.conf import settings
from typing import Dict, Any
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.core.paginator import Paginator
from django.db.models import Q
from keycloakauth.utils import IsAuthenticated
from nd_api_v2.models.table_details import TableMetadata
from nd_api_v2.decorator import conditional_authentication

RequestCtx = Dict[str, Any]


@conditional_authentication
class TableMetadataListView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request):
        """List all table metadata with search, filter, and pagination"""
        try:
            # Get query parameters
            search = request.query_params.get("search", "").strip()
            priority = request.query_params.get("priority", None)
            is_required = request.query_params.get("is_required", None)
            is_phi_marking_done = request.query_params.get("is_phi_marking_done", None)
            page = int(request.query_params.get("page", 1))
            page_size = int(request.query_params.get("page_size", 50))

            # Build query
            queryset = TableMetadata.objects.all()

            # Search by table name
            if search:
                queryset = queryset.filter(table_name__icontains=search)

            # Filter by priority
            if priority:
                try:
                    priority_int = int(priority)
                    queryset = queryset.filter(priority=priority_int)
                except ValueError:
                    pass

            # Filter by is_required
            if is_required is not None:
                is_required_bool = is_required.lower() in ["true", "1", "yes"]
                queryset = queryset.filter(is_required=is_required_bool)

            # Filter by is_phi_marking_done
            if is_phi_marking_done is not None:
                is_phi_bool = is_phi_marking_done.lower() in ["true", "1", "yes"]
                queryset = queryset.filter(is_phi_marking_done=is_phi_bool)

            # Order by table name
            queryset = queryset.order_by("table_name")

            # Pagination
            paginator = Paginator(queryset, page_size)
            page_obj = paginator.get_page(page)

            # Serialize results
            results = []
            for metadata in page_obj:
                # Determine PII status from columns_details
                pii_status = "Not PII"
                if metadata.table_details_for_ui and isinstance(metadata.table_details_for_ui, dict):
                    columns_details = metadata.table_details_for_ui.get("columns_details", [])
                    if columns_details:
                        phi_columns = [col for col in columns_details if col.get("is_phi", False)]
                        if len(phi_columns) == len(columns_details):
                            pii_status = "All PII"
                        elif len(phi_columns) > 0:
                            pii_status = "Partial PII"

                results.append({
                    "id": metadata.id,
                    "table_name": metadata.table_name,
                    "priority": metadata.priority,
                    "is_required": metadata.is_required,
                    "is_phi_marking_done": metadata.is_phi_marking_done,
                    "pii_status": pii_status,
                    "updated_at": metadata.updated_at.isoformat() if metadata.updated_at else None,
                })

            return Response({
                "results": results,
                "count": paginator.count,
                "page": page,
                "page_size": page_size,
                "total_pages": paginator.num_pages,
                "success": True
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "message": "Internal server error",
                "success": False,
                "error": str(e) if settings.DEBUG else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class TableMetadataDetailView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, table_id: int):
        """Get single table metadata by ID"""
        try:
            try:
                metadata = TableMetadata.objects.get(id=table_id)
            except TableMetadata.DoesNotExist:
                return Response({
                    "message": "Table metadata not found",
                    "success": False
                }, status=status.HTTP_404_NOT_FOUND)
            return Response({
                "id": metadata.id,
                "table_name": metadata.table_name,
                "columns": metadata.columns,
                "primary_key": metadata.primary_key,
                "max_nd_auto_increment_id": metadata.max_nd_auto_increment_id,
                "table_details_for_ui": metadata.table_details_for_ui,
                "run_config": metadata.run_config,
                "is_required": metadata.is_required,
                "priority": metadata.priority,
                "is_phi_marking_done": metadata.is_phi_marking_done,
                "created_at": metadata.created_at.isoformat() if metadata.created_at else None,
                "updated_at": metadata.updated_at.isoformat() if metadata.updated_at else None,
                "success": True
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "message": "Internal server error",
                "success": False,
                "error": str(e) if settings.DEBUG else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def post(self, request, table_id: int = None):
        """Create or update table metadata"""
        try:
            data: RequestCtx = request.data

            # If table_id is provided, update existing
            if table_id:
                try:
                    metadata = TableMetadata.objects.get(id=table_id)
                    if "columns" in data:
                        metadata.columns = data["columns"]
                    if "primary_key" in data:
                        metadata.primary_key = data["primary_key"]
                    if "max_nd_auto_increment_id" in data:
                        metadata.max_nd_auto_increment_id = data["max_nd_auto_increment_id"]
                    if "table_details_for_ui" in data:
                        metadata.table_details_for_ui = data["table_details_for_ui"]
                    if "run_config" in data:
                        metadata.run_config = data["run_config"]
                    if "is_required" in data:
                        metadata.is_required = data["is_required"]
                    if "priority" in data:
                        metadata.priority = data["priority"]
                    if "is_phi_marking_done" in data:
                        metadata.is_phi_marking_done = data["is_phi_marking_done"]
                    metadata.save()
                except TableMetadata.DoesNotExist:
                    return Response({
                        "message": "Table metadata not found",
                        "success": False
                    }, status=status.HTTP_404_NOT_FOUND)
            else:
                # Create new
                if "table_name" not in data:
                    return Response({
                        "message": "table_name is required",
                        "success": False
                    }, status=status.HTTP_400_BAD_REQUEST)

                # Check if table with same name already exists
                if TableMetadata.objects.filter(table_name=data["table_name"]).exists():
                    return Response({
                        "message": f"Table with name '{data['table_name']}' already exists",
                        "success": False
                    }, status=status.HTTP_400_BAD_REQUEST)

                metadata = TableMetadata.objects.create(
                    table_name=data["table_name"],
                    columns=data.get("columns", {}),
                    primary_key=data.get("primary_key", {}),
                    max_nd_auto_increment_id=data.get("max_nd_auto_increment_id", 0),
                    table_details_for_ui=data.get("table_details_for_ui", {}),
                    run_config=data.get("run_config", {}),
                    is_required=data.get("is_required", True),
                    priority=data.get("priority", 0),
                    is_phi_marking_done=data.get("is_phi_marking_done", False),
                )

            return Response({
                "id": metadata.id,
                "message": "Table metadata saved successfully",
                "success": True
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "message": "Internal server error",
                "success": False,
                "error": str(e) if settings.DEBUG else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, table_id: int):
        """Delete table metadata"""
        try:
            try:
                metadata = TableMetadata.objects.get(id=table_id)
                metadata.delete()
                return Response({
                    "message": "Table metadata deleted successfully",
                    "success": True
                }, status=status.HTTP_200_OK)
            except TableMetadata.DoesNotExist:
                return Response({
                    "message": "Table metadata not found",
                    "success": False
                }, status=status.HTTP_404_NOT_FOUND)

        except Exception as e:
            return Response({
                "message": "Internal server error",
                "success": False,
                "error": str(e) if settings.DEBUG else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class TableMetadataBulkUpdateView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request):
        """Bulk update table metadata"""
        try:
            data: RequestCtx = request.data
            table_ids = data.get("table_ids", [])
            updates = data.get("updates", {})

            if not table_ids:
                return Response({
                    "message": "table_ids is required",
                    "success": False
                }, status=status.HTTP_400_BAD_REQUEST)

            if not updates:
                return Response({
                    "message": "updates is required",
                    "success": False
                }, status=status.HTTP_400_BAD_REQUEST)

            # Get all tables
            tables = TableMetadata.objects.filter(id__in=table_ids)
            updated_count = 0

            # Update each table
            for table in tables:
                if "priority" in updates:
                    table.priority = updates["priority"]
                if "is_required" in updates:
                    table.is_required = updates["is_required"]
                if "is_phi_marking_done" in updates:
                    table.is_phi_marking_done = updates["is_phi_marking_done"]
                if "table_details_for_ui" in updates:
                    table.table_details_for_ui = updates["table_details_for_ui"]
                if "run_config" in updates:
                    table.run_config = updates["run_config"]
                table.save()
                updated_count += 1

            return Response({
                "message": f"Successfully updated {updated_count} table(s)",
                "updated_count": updated_count,
                "success": True
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "message": "Internal server error",
                "success": False,
                "error": str(e) if settings.DEBUG else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class TableMetadataExportView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request):
        """Export table metadata as CSV"""
        try:
            import csv
            from django.http import HttpResponse
            from io import StringIO

            # Get all tables
            queryset = TableMetadata.objects.all().order_by("table_name")

            # Create CSV response
            response = HttpResponse(content_type="text/csv")
            response["Content-Disposition"] = 'attachment; filename="table_metadata_export.csv"'

            writer = csv.writer(response)
            # Write header
            writer.writerow([
                "Table Name",
                "Priority",
                "Is Required",
                "Is PII Marking Done",
                "Max ND Auto Increment ID",
                "Created At",
                "Updated At"
            ])

            # Write data
            for metadata in queryset:
                writer.writerow([
                    metadata.table_name,
                    metadata.priority,
                    metadata.is_required,
                    metadata.is_phi_marking_done,
                    metadata.max_nd_auto_increment_id,
                    metadata.created_at.isoformat() if metadata.created_at else "",
                    metadata.updated_at.isoformat() if metadata.updated_at else "",
                ])

            return response

        except Exception as e:
            return Response({
                "message": "Internal server error",
                "success": False,
                "error": str(e) if settings.DEBUG else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

