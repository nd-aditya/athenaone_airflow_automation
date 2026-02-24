from django.urls import path

from .views import (
    RegisterPACSHandlerView,
    GetPacsClientView,

    PacsInventoryView,

    StartDeidPacsDataView,

    PacsClientOverViewView,

    PacsClientPatientsDashboardView,
    PacsClientStudyDashboardView,
    PacsClientSeriesDashboardView,
    PacsClientInstanceDashboardView

)

urlpatterns = [
    path("pacs/register_handler/<int:client_id>/", RegisterPACSHandlerView.as_view(), name="register_handler"),
    path("pacs/get_pacs_client/<int:client_id>/<int:pacs_client_id>/", GetPacsClientView.as_view(), name="get_pacs_client"),
    
    path("pacs/create_inventory/<int:client_id>/<int:pacs_client_id>/", PacsInventoryView.as_view(), name="create_inventory"),
    
    path("pacs/start_deidentification/<int:client_id>/<int:pacs_client_id>/", StartDeidPacsDataView.as_view(), name="pacs_start_deidentification"),
    
    path("pacs/overview/<int:client_id>/<int:pacs_client_id>/", PacsClientOverViewView.as_view(), name="pacs_overview"),
    path("pacs/patients/<int:pacs_client_id>/", PacsClientPatientsDashboardView.as_view(), name="pacs_patients"),
    path("pacs/studies/<int:pacs_client_id>/<int:client_patient_id>/", PacsClientStudyDashboardView.as_view(), name="pacs_studies"),
    path("pacs/series/<int:pacs_client_id>/<int:client_patient_id>/<str:client_study_uid>/", PacsClientSeriesDashboardView.as_view(), name="pacs_series"),
    path("pacs/instance/<int:pacs_client_id>/<int:client_patient_id>/<str:client_study_uid>/<str:client_series_uid>/", PacsClientInstanceDashboardView.as_view(), name="pacs_instance"),
]

