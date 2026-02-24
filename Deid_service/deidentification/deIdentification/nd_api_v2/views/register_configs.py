from django.conf import settings
from typing import TypedDict, Dict, Any
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from keycloakauth.utils import IsAuthenticated
from nd_api_v2.models.configs import (
    MappingConfig,
    MasterTableConfig,
    PIIMaskingConfig,
    QCConfig,
    ClientRunConfig,
)
from nd_api_v2.decorator import conditional_authentication

RequestCtx = Dict[str, Any]


@conditional_authentication
class RegisterClientRunConfigsView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request):
        """Create or update client run configuration"""
        try:
            data: RequestCtx = request.data
            client_run_config = ClientRunConfig.objects.last()
            if client_run_config is None:
                client_run_config = ClientRunConfig.objects.create(
                    patient_identifier_columns=data.get(
                        "patient_identifier_columns", []
                    ),
                    admin_connection_str=data.get("admin_connection_str", ""),
                    nd_patient_start_value=data.get("nd_patient_start_value", 0),
                    default_offset_value=data.get("default_offset_value", 0),
                    ehr_type=data.get("ehr_type", "AnthenaOne"),
                    enable_auto_qc=data.get("enable_auto_qc", False),
                    enable_auto_gcp=data.get("enable_auto_gcp", False),
                    enable_auto_embd=data.get("enable_auto_embd", False),
                )
            # Update existing config
            client_run_config.patient_identifier_columns = data.get(
                "patient_identifier_columns", []
            )
            client_run_config.admin_connection_str = data.get(
                "admin_connection_str", ""
            )
            client_run_config.nd_patient_start_value = data.get(
                "nd_patient_start_value", 0
            )
            client_run_config.default_offset_value = data.get("default_offset_value", 0)
            client_run_config.ehr_type = data.get("ehr_type", "AnthenaOne")
            client_run_config.enable_auto_qc = data.get("enable_auto_qc", False)
            client_run_config.enable_auto_gcp = data.get("enable_auto_gcp", False)
            client_run_config.enable_auto_embd = data.get("enable_auto_embd", False)
            client_run_config.is_configured = True
            client_run_config.save()

            return Response(
                {
                    "patient_identifier_columns": client_run_config.patient_identifier_columns,
                    "admin_connection_str": client_run_config.admin_connection_str,
                    "nd_patient_start_value": client_run_config.nd_patient_start_value,
                    "default_offset_value": client_run_config.default_offset_value,
                    "ehr_type": client_run_config.ehr_type,
                    "enable_auto_qc": client_run_config.enable_auto_qc,
                    "enable_auto_gcp": client_run_config.enable_auto_gcp,
                    "enable_auto_embd": client_run_config.enable_auto_embd,
                    "is_configured": client_run_config.is_configured,
                    "success": True,
                    "message": "Client run config saved successfully",
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def get(self, request):
        """Get client run configuration"""
        try:
            config = ClientRunConfig.objects.filter(is_configured=True).last()
            if not config:
                return Response(
                    {
                        "message": "No configuration found",
                        "success": True,
                        "is_configured": False,
                    },
                    status=status.HTTP_200_OK,
                )

            return Response(
                {
                    "id": config.id,
                    "patient_identifier_columns": config.patient_identifier_columns,
                    "admin_connection_str": config.admin_connection_str,
                    "nd_patient_start_value": config.nd_patient_start_value,
                    "default_offset_value": config.default_offset_value,
                    "ehr_type": config.ehr_type,
                    "enable_auto_qc": config.enable_auto_qc,
                    "enable_auto_gcp": config.enable_auto_gcp,
                    "enable_auto_embd": config.enable_auto_embd,
                    "is_configured": config.is_configured,
                    "success": True,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class RegisterMappingConfigsView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request):
        """Create or update mapping configuration"""
        try:
            data: RequestCtx = request.data
            mapping_config = MappingConfig.objects.last()

            if mapping_config is None:
                mapping_config = MappingConfig.objects.create(
                    mapping_config=data.get("mapping_config", {}),
                    mapping_schema=data.get("mapping_schema", ""),
                    is_configured=True,
                )
            else:
                mapping_config.mapping_config = data.get("mapping_config", {})
                mapping_config.mapping_schema = data.get("mapping_schema", "")
                mapping_config.is_configured = True
                mapping_config.save()

            return Response(
                {
                    "mapping_config": mapping_config.mapping_config,
                    "mapping_schema": mapping_config.mapping_schema,
                    "is_configured": mapping_config.is_configured,
                    "success": True,
                    "message": "Mapping config saved successfully",
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def get(self, request):
        """Get mapping configuration"""
        try:
            config = MappingConfig.objects.filter(is_configured=True).last()
            if not config:
                return Response(
                    {
                        "message": "No configuration found",
                        "success": True,
                        "is_configured": False,
                    },
                    status=status.HTTP_200_OK,
                )

            return Response(
                {
                    "id": config.id,
                    "mapping_config": config.mapping_config,
                    "mapping_schema": config.mapping_schema,
                    "is_configured": config.is_configured,
                    "success": True,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class RegisterMasterTableConfigsView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request):
        """Create or update master table configuration"""
        try:
            data: RequestCtx = request.data
            master_table_config = MasterTableConfig.objects.last()

            if master_table_config is None:
                # Update existing config
                master_table_config = MasterTableConfig.objects.create(
                    pii_tables_config=data.get("pii_tables_config", {}),
                    pii_schema_name=data.get("pii_schema_name", ""),
                    is_configured=True,
                )
            else:
                master_table_config.pii_tables_config = data.get(
                    "pii_tables_config", {}
                )
                master_table_config.pii_schema_name = data.get("pii_schema_name", "")
                master_table_config.is_configured = True
                master_table_config.save()

            return Response(
                {
                    "id": master_table_config.id,
                    "success": True,
                    "message": "Master table config saved successfully",
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def get(self, request):
        """Get master table configuration"""
        try:
            config = MasterTableConfig.objects.filter(is_configured=True).first()
            if not config:
                return Response(
                    {
                        "message": "No configuration found",
                        "success": True,
                        "is_configured": False,
                    },
                    status=status.HTTP_200_OK,
                )

            return Response(
                {
                    "id": config.id,
                    "pii_tables_config": config.pii_tables_config,
                    "pii_schema_name": config.pii_schema_name,
                    "is_configured": config.is_configured,
                    "success": True,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class RegisterPIIMaskingConfigsView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request):
        """Create or update PII masking configuration"""
        try:
            data: RequestCtx = request.data
            pii_masking_config = PIIMaskingConfig.objects.last()

            if pii_masking_config is None:
                pii_masking_config = PIIMaskingConfig.objects.create(
                    pii_masking_config=data.get("pii_masking_config", {}),
                    secondary_config=data.get("secondary_config", {}),
                    is_configured=True,
                )
            else:
                pii_masking_config.pii_masking_config = data.get(
                    "pii_masking_config", {}
                )
                pii_masking_config.secondary_config = data.get("secondary_config", {})
                pii_masking_config.is_configured = True
                pii_masking_config.save()

            return Response(
                {
                    "pii_masking_config": pii_masking_config.pii_masking_config,
                    "secondary_config": pii_masking_config.secondary_config,
                    "is_configured": pii_masking_config.is_configured,
                    "success": True,
                    "message": "PII masking config saved successfully",
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def get(self, request):
        """Get PII masking configuration"""
        try:
            config = PIIMaskingConfig.objects.filter(is_configured=True).first()
            if not config:
                return Response(
                    {
                        "message": "No configuration found",
                        "success": True,
                        "is_configured": False,
                    },
                    status=status.HTTP_200_OK,
                )

            return Response(
                {
                    "id": config.id,
                    "pii_masking_config": config.pii_masking_config,
                    "secondary_config": config.secondary_config,
                    "is_configured": config.is_configured,
                    "success": True,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class RegisterQCConfigsView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request):
        """Create or update QC configuration"""
        try:
            data: RequestCtx = request.data
            qc_config = QCConfig.objects.last()

            if qc_config is None:
                # Update existing config
                qc_config = QCConfig.objects.create(
                    qc_config=data.get("qc_config", {}),
                    is_configured=True,
                )
            else:
                qc_config.qc_config = data.get("qc_config", {})
                qc_config.is_configured = True
                qc_config.save()

            return Response(
                {
                    "qc_config": qc_config.qc_config,
                    "is_configured": qc_config.is_configured,
                    "success": True,
                    "message": "QC config saved successfully",
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def get(self, request):
        """Get QC configuration"""
        try:
            config = QCConfig.objects.filter(is_configured=True).last()
            if not config:
                return Response(
                    {
                        "message": "No configuration found",
                        "success": True,
                        "is_configured": False,
                    },
                    status=status.HTTP_200_OK,
                )

            return Response(
                {
                    "id": config.id,
                    "qc_config": config.qc_config,
                    "is_configured": config.is_configured,
                    "success": True,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {
                    "message": "Internal server error",
                    "success": False,
                    "error": str(e) if settings.DEBUG else None,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
