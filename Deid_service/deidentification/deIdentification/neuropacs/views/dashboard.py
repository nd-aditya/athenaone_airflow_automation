import traceback
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from neuropacs.models import (
    PacsClient,
    Patients,
    PatientStudy,
    PatientSeries,
    PatientInstance,
)
from neuropacs.models.utils import Status
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import get_object_or_404


@conditional_authentication
class PacsClientPatientsDashboardView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, pacs_client_id: int):
        try:
            pacs_client_obj = get_object_or_404(PacsClient, id=pacs_client_id)
            patients_qs = pacs_client_obj.patients.all().order_by("id")
            paginator = Paginator(patients_qs, 10)
            page_number = request.query_params.get("page", 1)
            try:
                patients_page = paginator.page(page_number)
            except (PageNotAnInteger, ValueError):
                patients_page = paginator.page(1)
            except EmptyPage:
                patients_page = paginator.page(paginator.num_pages)

            results_list = []
            for patient in patients_page.object_list:
                study_uids = list(
                    patient.studies.values_list("client_study_instance_uid", flat=True)
                )
                results_list.append(
                    {
                        "id": patient.id,
                        "nd_patient_id": patient.nd_patient_id,
                        "client_patient_id": patient.client_patient_id,
                        "deid_status": patient.deid_status,
                        "cloud_uploaded": patient.cloud_uploaded,
                        "study_uids": study_uids,
                    }
                )

            response_output = {
                "count": paginator.count,
                "num_pages": paginator.num_pages,
                "current_page": patients_page.number,
                "results": results_list,
            }
            return Response(response_output, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class PacsClientStudyDashboardView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, pacs_client_id: int, client_patient_id: int):
        try:
            pacs_client_obj = get_object_or_404(PacsClient, id=pacs_client_id)
            patient_obj = get_object_or_404(
                Patients,
                client_patient_id=client_patient_id,
                pacs_client=pacs_client_obj,
            )
            studies_qs = patient_obj.studies.all().order_by("id")
            paginator = Paginator(studies_qs, 10)
            page_number = request.query_params.get("page", 1)
            try:
                studies_page = paginator.page(page_number)
            except (PageNotAnInteger, ValueError):
                studies_page = paginator.page(1)
            except EmptyPage:
                studies_page = paginator.page(paginator.num_pages)

            results_list = []
            for study in studies_page.object_list:
                series_uids = list(
                    study.series.values_list("client_series_instance_uid", flat=True)
                )
                results_list.append(
                    {
                        "id": study.id,
                        "client_study_instance_uid": study.client_study_instance_uid,
                        "nd_study_instance_uid": study.nd_study_instance_uid,
                        "deid_status": study.deid_status,
                        "cloud_uploaded": study.cloud_uploaded,
                        "series_uids": series_uids,
                    }
                )

            response_output = {
                "count": paginator.count,
                "num_pages": paginator.num_pages,
                "current_page": studies_page.number,
                "results": results_list,
            }
            return Response(response_output, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class PacsClientSeriesDashboardView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(
        self,
        request,
        pacs_client_id: int,
        client_patient_id: int,
        client_study_uid: str,
    ):
        try:
            pacs_client_obj = get_object_or_404(PacsClient, id=pacs_client_id)
            patient_obj = get_object_or_404(
                Patients,
                client_patient_id=client_patient_id,
                pacs_client=pacs_client_obj,
            )
            study_obj = get_object_or_404(
                PatientStudy,
                client_study_instance_uid=client_study_uid,
                patient=patient_obj,
            )
            series_qs = study_obj.series.all().order_by("id")
            paginator = Paginator(series_qs, 10)
            page_number = request.query_params.get("page", 1)
            try:
                series_page = paginator.page(page_number)
            except (PageNotAnInteger, ValueError):
                series_page = paginator.page(1)
            except EmptyPage:
                series_page = paginator.page(paginator.num_pages)

            results_list = []
            for series in series_page.object_list:
                instance_uids = list(
                    series.instances.values_list("client_sop_instance_uid", flat=True)
                )
                results_list.append(
                    {
                        "id": series.id,
                        "client_series_instance_uid": series.client_series_instance_uid,
                        "nd_series_instance_uid": series.nd_series_instance_uid,
                        "deid_status": series.deid_status,
                        "cloud_uploaded": series.cloud_uploaded,
                        "instance_uids": instance_uids,
                    }
                )
            response_output = {
                "count": paginator.count,
                "num_pages": paginator.num_pages,
                "current_page": series_page.number,
                "results": results_list,
            }
            return Response(response_output, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class PacsClientInstanceDashboardView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(
        self,
        request,
        pacs_client_id: int,
        client_patient_id: int,
        client_study_uid: str,
        client_series_uid: str,
    ):
        try:
            pacs_client_obj = get_object_or_404(PacsClient, id=pacs_client_id)
            patient_obj = get_object_or_404(
                Patients,
                client_patient_id=client_patient_id,
                pacs_client=pacs_client_obj,
            )
            study_obj = get_object_or_404(
                PatientStudy,
                client_study_instance_uid=client_study_uid,
                patient=patient_obj,
            )
            series_obj = get_object_or_404(
                PatientSeries,
                client_series_instance_uid=client_series_uid,
                study=study_obj,
            )
            instances_qs = series_obj.instances.all().order_by("id")
            paginator = Paginator(instances_qs, 10)
            page_number = request.query_params.get("page", 1)
            try:
                instances_page = paginator.page(page_number)
            except (PageNotAnInteger, ValueError):
                instances_page = paginator.page(1)
            except EmptyPage:
                instances_page = paginator.page(paginator.num_pages)

            results_list = []
            for instance in instances_page.object_list:
                results_list.append(
                    {
                        "id": instance.id,
                        "client_sop_instance_uid": instance.client_sop_instance_uid,
                        "nd_sop_instance_uid": instance.nd_sop_instance_uid,
                        "deid_status": instance.deid_status,
                        "cloud_uploaded": instance.cloud_uploaded,
                    }
                )

            response_output = {
                "count": paginator.count,
                "num_pages": paginator.num_pages,
                "current_page": instances_page.number,
                "results": results_list,
            }
            return Response(response_output, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )



@conditional_authentication
class PacsClientOverViewView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int, pacs_client_id: int):
        try:
            pacs_client_obj = PacsClient.objects.get(
                id=pacs_client_id, client__id=client_id
            )

            total_patients = pacs_client_obj.patients.all()
            total_studies = PatientStudy.objects.filter(
                patient__pacs_client=pacs_client_obj
            )
            total_series = PatientSeries.objects.filter(
                study__patient__pacs_client=pacs_client_obj
            )
            total_instances = PatientInstance.objects.filter(
                series__study__patient__pacs_client=pacs_client_obj
            )

            overview = {
                "total_patients_count": total_patients.count(),
                "total_studies_count": total_studies.count(),
                "total_series_count": total_series.count(),
                "total_instances_count": total_instances.count(),
                "deid_status": {
                    "total_patient_deid_done": total_patients.filter(
                        deid_status=Status.COMPLETED
                    ).count(),
                    "total_patient_deid_failed": total_patients.filter(
                        deid_status=Status.FAILED
                    ).count(),
                    "total_patient_deid_pending": total_patients.exclude(
                        deid_status=Status.COMPLETED
                    )
                    .exclude(deid_status=Status.FAILED)
                    .count(),
                    "total_study_deid_done": total_studies.filter(
                        deid_status=Status.COMPLETED
                    ).count(),
                    "total_study_deid_failed": total_studies.filter(
                        deid_status=Status.FAILED
                    ).count(),
                    "total_study_deid_pending": total_studies.exclude(
                        deid_status=Status.COMPLETED
                    )
                    .exclude(deid_status=Status.FAILED)
                    .count(),
                    "total_series_deid_done": total_series.filter(
                        deid_status=Status.COMPLETED
                    ).count(),
                    "total_series_deid_failed": total_series.filter(
                        deid_status=Status.FAILED
                    ).count(),
                    "total_series_deid_pending": total_series.exclude(
                        deid_status=Status.COMPLETED
                    )
                    .exclude(deid_status=Status.FAILED)
                    .count(),
                    "total_instances_deid_done": total_instances.filter(
                        deid_status=Status.COMPLETED
                    ).count(),
                    "total_instances_deid_failed": total_instances.filter(
                        deid_status=Status.FAILED
                    ).count(),
                    "total_instances_deid_pending": total_instances.exclude(
                        deid_status=Status.COMPLETED
                    )
                    .exclude(deid_status=Status.FAILED)
                    .count(),
                },
                "cloud_upload_status": {
                    "total_patient_uploaded": total_patients.filter(
                        cloud_uploaded=True
                    ).count(),
                    "total_patient_upload_pending": total_patients.filter(
                        cloud_uploaded=False
                    ).count(),
                    "total_study_uploaded": total_studies.filter(
                        cloud_uploaded=True
                    ).count(),
                    "total_study_upload_pending": total_studies.filter(
                        cloud_uploaded=False
                    ).count(),
                    "total_series_uploaded": total_series.filter(
                        cloud_uploaded=True
                    ).count(),
                    "total_series_upload_pending": total_series.filter(
                        cloud_uploaded=False
                    ).count(),
                    "total_instances_uploaded": total_instances.filter(
                        cloud_uploaded=True
                    ).count(),
                    "total_instances_upload_pending": total_instances.filter(
                        cloud_uploaded=False
                    ).count(),
                },
            }

            return Response(overview, status=status.HTTP_200_OK)

        except PacsClient.DoesNotExist:
            return Response(
                {"message": "PacsClient not found"},
                status=status.HTTP_404_NOT_FOUND,
            )
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": f"Internal server error: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
