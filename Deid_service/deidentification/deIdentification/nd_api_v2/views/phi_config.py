import csv
import io
import traceback
from rest_framework.views import APIView
from django.http import HttpResponse
from datetime import datetime
from keycloakauth.utils import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from deIdentification.nd_logger import nd_logger
from nd_api_v2.decorator import conditional_authentication
from nd_api_v2.models.table_details import TableMetadata



CSV_HEADERS = [
    "TABLE_NAME",
    "COLUMN_NAME",
    "IS_PHI",
    "DE_IDENTIFICATION_RULE",
    "MASK_VALUE",
    "REFERENCE_PATIENT_ID",
    "REFERENCE_ENCOUNTER_ID",
    "PRIORITY",
]

@conditional_authentication
class DownloadConfigAsCSV(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request):
        try:
            
            all_tables: list[TableMetadata] = TableMetadata.objects.all()

            # Create CSV buffer
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(CSV_HEADERS)

            for table in all_tables:
                table_details_for_ui = table.table_details_for_ui

                # Get reference columns
                ref_patient_id = table_details_for_ui.get(
                    "reference_patient_id_column", None
                )
                ref_encounter_id = table_details_for_ui.get("reference_enc_id_column", None)

                # Write each column's configuration
                for column in table_details_for_ui.get("columns_details", []):
                    writer.writerow(
                        [
                            table.table_name,
                            column["column_name"],
                            column.get("is_phi", False),
                            column.get("de_identification_rule", None),
                            column.get("mask_value", None),
                            ref_patient_id,
                            ref_encounter_id,
                            table.priority,
                        ]
                    )

            # Prepare response
            output.seek(0)
            response = HttpResponse(output.getvalue(), content_type="text/csv")
            response["Content-Disposition"] = (
                f'attachment; filename="table_configuration_{datetime.now().strftime("%d_%m_%Y")}.csv"'
            )

            return response
        except Exception as e:
            message = f"ViewTableDataView.get: Internal server error : {e}, for user: {request.user}"
            nd_logger.info(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

@conditional_authentication
class UploadConfigFromCSV(APIView):
    authentication_classes = [IsAuthenticated]
    
    def post(self, request):
        try:
            uploaded_file = request.FILES.get("file")
            if not uploaded_file:
                return Response(
                    {"success": False, "message": "No file provided."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            file_data = uploaded_file.read().decode("utf-8")
            csv_reader = csv.DictReader(io.StringIO(file_data))

            # Validate uploaded CSV has all expected headers, regardless of order
            expected_headers_set = set(CSV_HEADERS)
            if not csv_reader.fieldnames or set(csv_reader.fieldnames) != expected_headers_set:
                return Response(
                    {"success": False, "message": "Invalid CSV headers."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Group rows by table name
            table_configs = {}
            for row in csv_reader:
                table_name = row.get(
                    "TABLE_NAME", ""
                ).strip().lower()  # Strip whitespace from table name
                if not table_name:  # Skip empty table names
                    continue
                if table_name not in table_configs:
                    # Parse priority with default value 10000
                    priority_value = row.get("PRIORITY", "10000")
                    try:
                        priority = int(priority_value) if priority_value else 10000
                    except (ValueError, TypeError):
                        priority = 10000
                    
                    table_configs[table_name] = {
                        "columns": [],
                        "ref_patient_id": row.get("REFERENCE_PATIENT_ID"),
                        "ref_encounter_id": row.get("REFERENCE_ENCOUNTER_ID"),
                        "priority": priority,
                    }

                column_name = row.get("COLUMN_NAME").lower()
                existing_column = next(
                    (
                        col
                        for col in table_configs[table_name]["columns"]
                        if col["column_name"].lower() == column_name
                    ),
                    None,
                )
                if existing_column:
                    # Update existing column values
                    existing_column["is_phi"] = row.get("IS_PHI", "False").lower() in [
                        "true",
                        "1",
                        "yes"
                    ]
                    existing_column["de_identification_rule"] = row.get(
                        "DE_IDENTIFICATION_RULE"
                    )
                    existing_column["mask_value"] = row.get("MASK_VALUE")
                else:
                    # Add new column if it doesn't exist
                    table_configs[table_name]["columns"].append(
                        {
                            "column_name": column_name,
                            "is_phi": row.get("IS_PHI", "False").lower()
                            in ["true", "1", "yes"],
                            "de_identification_rule": row.get("DE_IDENTIFICATION_RULE"),
                            "mask_value": row.get("MASK_VALUE"),
                        }
                    )

            # Process each table's configuration
            tables_not_found = []
            for table_name, config in table_configs.items():
                # Fetch the table details
                table: TableMetadata = TableMetadata.objects.filter(table_name__iexact=table_name).first()
                if not table:
                    message = f"Table '{table_name}' not found in database."
                    nd_logger.error(message)
                    tables_not_found.append(table_name)
                    continue
                table_details_for_ui = table.table_details_for_ui
                columns_details = table_details_for_ui.get("columns_details", [])

                # Update all columns for this table
                for new_column in config["columns"]:
                    column = next(
                        (
                            col
                            for col in columns_details
                            if col["column_name"].lower() == new_column["column_name"].lower()
                        ),
                        None,
                    )
                    if not column:
                        message = f"Column '{new_column['column_name']}' not found in table '{table_name}'."
                        nd_logger.error(message)
                        return Response(
                            {
                                "success": False,
                                "tables_not_found": tables_not_found,
                                "total_tables_not_found": len(tables_not_found),
                                "message": message,
                            },
                            status=status.HTTP_400_BAD_REQUEST,
                        )

                    column["is_phi"] = new_column["is_phi"]
                    column["de_identification_rule"] = new_column[
                        "de_identification_rule"
                    ]
                    column["mask_value"] = new_column["mask_value"]

                # Update reference columns if provided
                if config["ref_patient_id"]:
                    table_details_for_ui["reference_patient_id_column"] = config[
                        "ref_patient_id"
                    ]
                if config["ref_encounter_id"]:
                    table_details_for_ui["reference_enc_id_column"] = config[
                        "ref_encounter_id"
                    ]

                # Update priority if provided
                if "priority" in config:
                    table.priority = config["priority"]

                # Save changes to the table
                table.table_details_for_ui = table_details_for_ui
                table.is_phi_marking_done = True
                table.save()
            nd_logger.info("Configuration uploaded successfully.")
            return Response(
                {"success": True, "message": "Configuration uploaded successfully.", "tables_not_found": tables_not_found, "total_tables_not_found": len(tables_not_found)},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            nd_logger.error(traceback.format_exc())
            return Response(
                {"success": False, "message": f"{e}", "tables_not_found": [], "total_tables_not_found": 0},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
