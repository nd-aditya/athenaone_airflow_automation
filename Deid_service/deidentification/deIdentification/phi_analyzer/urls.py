from django.urls import path
from django.http import JsonResponse
from phi_analyzer.views.run_analyzer import (
    RunAnalyzerForDumpView, 
    PHIAnalysisStatusView, 
    PHIAnalysisResultsView, 
    PHIAnalysisListView
)
from phi_analyzer.views.model_configuration import (
    ModelConfigurationListCreateView,
    ModelConfigurationRetrieveUpdateDestroyView,
    GetDefaultConfigurationView,
    SetDefaultConfigurationView,
    DuplicateConfigurationView
)
from phi_analyzer.views.csv_import import CSVImportView
from phi_analyzer.views.phi_column_update import (
    UpdatePHIColumnView,
    UpdateManualVerificationView,
    GetPHIColumnView,
    GetTableVerificationStatusView
)

def cors_test_view(request):
    """Simple test endpoint to verify CORS is working"""
    return JsonResponse({"message": "CORS test successful", "method": request.method})

urlpatterns = [
    # CORS test endpoint
    path("phi_analyzer/cors-test/", cors_test_view, name="cors_test"),
    
    # Main analysis endpoint
    path("phi_analyzer/run_analyzer/<int:client_id>/<int:dump_id>/", RunAnalyzerForDumpView.as_view(), name="run_analyzer"),
    
    # Status and results endpoints
    path("phi_analyzer/status/<int:session_id>/", PHIAnalysisStatusView.as_view(), name="phi_analysis_status"),
    path("phi_analyzer/results/<int:session_id>/", PHIAnalysisResultsView.as_view(), name="phi_analysis_results"),
    path("phi_analyzer/list/", PHIAnalysisListView.as_view(), name="phi_analysis_list"),
    path("phi_analyzer/list/<int:client_id>/", PHIAnalysisListView.as_view(), name="phi_analysis_list_by_client"),
    path("phi_analyzer/list/<int:client_id>/<int:dump_id>/", PHIAnalysisListView.as_view(), name="phi_analysis_list_by_client_dump"),
    
    # Model configuration endpoints
    path("phi_analyzer/configurations/", ModelConfigurationListCreateView.as_view(), name="model_configuration_list"),
    path("phi_analyzer/configurations/<int:id>/", ModelConfigurationRetrieveUpdateDestroyView.as_view(), name="model_configuration_detail"),
    path("phi_analyzer/configurations/default/", GetDefaultConfigurationView.as_view(), name="model_configuration_default"),
    path("phi_analyzer/configurations/<int:config_id>/set-default/", SetDefaultConfigurationView.as_view(), name="model_configuration_set_default"),
    path("phi_analyzer/configurations/<int:config_id>/duplicate/", DuplicateConfigurationView.as_view(), name="model_configuration_duplicate"),
    
    # CSV import endpoint
    path("phi_analyzer/import_csv/<int:client_id>/<int:dump_id>/", CSVImportView.as_view(), name="csv_import"),
    
    # PHI Column update endpoints
    path("phi_analyzer/sessions/<int:session_id>/tables/<str:table_name>/columns/<str:column_name>/", UpdatePHIColumnView.as_view(), name="update_phi_column"),
    path("phi_analyzer/sessions/<int:session_id>/tables/<str:table_name>/columns/<str:column_name>/details/", GetPHIColumnView.as_view(), name="get_phi_column"),
    path("phi_analyzer/sessions/<int:session_id>/tables/<str:table_name>/verification/", UpdateManualVerificationView.as_view(), name="update_table_verification"),
    path("phi_analyzer/sessions/<int:session_id>/tables/<str:table_name>/verification/status/", GetTableVerificationStatusView.as_view(), name="get_table_verification_status"),
]