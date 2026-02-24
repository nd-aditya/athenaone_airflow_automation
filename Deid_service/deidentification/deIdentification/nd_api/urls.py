from django.urls import path

from .views import (
    ClientDumpView,
    ClientUpdateView,
    DumpDetailsView,
    ClientView,
    StartProcessingForDumpView,

    TablesSchemaView,
    PIITablesView,

    DumpDashboardView,

    TablesConfigForUIView,
    DeIdentifyTableView,
    TableDeidBulkView,
    InterruptDeidView,
    InterruptQCView,

    StopDeIdentificationView,
    DownloadConfigAsCSV,
    # UpdateTableProgress,
    # UpdateDbProgress,
    UploadConfigFromCSV,
    
    TablesDetailsForUIView,
    TablesConfigForUIView,
    ViewTableDataView,
    ViewTableDataWithNameView,

    QCResultView,
    QCBulkView,

    GCPResultView,
    TableGCPView,
    EmbeddingResultView,
    TableEmbeddingView,

    ConfigsView,
    ReUseProcessingConfigView,
    PrimaryKeyConfigView,

    DEIDRulesView,

    # DbStatsView,
    UserPermissions,
    # CloudMovement,
    DumpDataView,
    StartDumpView,
    DumpRestoreView,
    StartDumpRestoreView,

    StartDeIdentification,

    ReUseTableConfigView,

    RefershSourceDbView
)

urlpatterns = [
    path("clients/", ClientView.as_view(), name="client"),
    path("client_dumps/<int:client_id>/", ClientDumpView.as_view(), name="client_dumps"),
    path("client_details/<int:client_id>/", ClientUpdateView.as_view(), name="client_details"),
    
    path("dump_details/<int:client_id>/<int:dump_id>/", DumpDetailsView.as_view(), name="dump_details"),
    path("start_dump_processing/<int:client_id>/<int:dump_id>/", StartProcessingForDumpView.as_view(), name="start_dump_processing"),
    
    path("get_tables/<int:client_id>/<int:dump_id>/", TablesDetailsForUIView.as_view(), name="get_tables"),
    path(
        "tables_details_for_ui/<int:table_id>/",
        TablesConfigForUIView.as_view(),
        name="tables_details_for_ui",
    ),
    path("table_schema/<int:client_id>/<int:dump_id>/", TablesSchemaView.as_view(), name="table_schema"),
    
    path("pii_tables_details/<int:client_id>/", PIITablesView.as_view(), name="pii_tables_details"),
    
    path("dump_dashboard/<int:client_id>/<int:dump_id>/", DumpDashboardView.as_view(), name="dump_dashboard"),

    path("view_table_data/<int:table_id>/", ViewTableDataView.as_view(), name="view_table_data"),
    path("view_table_data/<int:client_id>/<int:dump_id>/<str:table_name>/", ViewTableDataWithNameView.as_view(), name="view_table_data_with_name"),
    path(
        "download_config_as_csv/<int:client_id>/<int:dump_id>/",
        DownloadConfigAsCSV.as_view(),
        name="download_config_as_csv",
    ),
    path(
        "upload_config_from_csv/<int:client_id>/<int:dump_id>/",
        UploadConfigFromCSV.as_view(),
        name="upload_config_from_csv",
    ),

    path(
        "start_de_identification/<int:table_id>/",
        DeIdentifyTableView.as_view(),
        name="start_de_identification",
    ),
    path(
        "start_whole_identification/<int:client_id>/<int:dump_id>/",
        StartDeIdentification.as_view(),
        name="start_whole_identification",
    ),

    path(
        "stop_de_identification/<int:table_id>/",
        StopDeIdentificationView.as_view(),
        name="stop_de_identification",
    ),

    # Add apis for QC
    path("qc/start/<int:client_id>/<int:dump_id>/", QCBulkView.as_view(), name="qc_start"),
    path("qc/result/<int:client_id>/<int:dump_id>/", QCResultView.as_view(), name="qc_result"),
    path("qc/interrupt/<int:client_id>/<int:dump_id>/", InterruptQCView.as_view(), name="qc_interrupt"),
    
    path("gcp/start/<int:client_id>/<int:dump_id>/", TableGCPView.as_view(), name="gcp_start"),
    path("gcp/result/<int:client_id>/<int:dump_id>/", GCPResultView.as_view(), name="gcp_result"),
    
    path("embd/start/<int:client_id>/<int:dump_id>/", TableEmbeddingView.as_view(), name="embd_start"),
    path("embd/result/<int:client_id>/<int:dump_id>/", EmbeddingResultView.as_view(), name="embd_result"),
    
    path("deid/start/<int:client_id>/<int:dump_id>/", TableDeidBulkView.as_view(), name="deid_start"),
    path("deid/interrupt/<int:client_id>/<int:dump_id>/", InterruptDeidView.as_view(), name="deid_interrupt"),
    
    path("configuration/<int:client_id>/<int:dump_id>/", ConfigsView.as_view(), name="configuration"),
    path("reuse_configuration/<int:client_id>/<int:dump_id>/", ReUseProcessingConfigView.as_view(), name="reuse_configuration"),
    
    path("primary_key_config/<int:client_id>/<int:dump_id>/", PrimaryKeyConfigView.as_view(), name="primary_key_config"),
    
    path("deid_rules/<int:client_id>/", DEIDRulesView.as_view(), name="deid_rules"),
    
    path("reuse_table_config/<int:client_id>/<int:dump_id>/", ReUseTableConfigView.as_view(), name="reuse_table_config"),
    
    path("refresh_source_db/<int:client_id>/<int:dump_id>/", RefershSourceDbView.as_view(), name="refresh_source_db"),
    

    # path(
    #     "update_table_progress/",
    #     UpdateTableProgress.as_view(),
    #     name="update_table_progress",
    # ),
    # path("update_db_progress/", UpdateDbProgress.as_view(), name="update_db_progress"),

    # path("stats_view/<int:db_id>/", DbStatsView.as_view(), name="stats_view"),
    path("user_permissions/", UserPermissions.as_view(), name="user_permissions"),
    # path("cloudmove/<int:table_id>/", CloudMovement.as_view(), name="cloudmove"),
    
    path("dump/", DumpDataView.as_view(), name="datadump"),
    path("start_dump_creation/<int:dump_id>/", StartDumpView.as_view(), name="datadump"),
    path("restore_dump/<int:restore_dump_id>/", StartDumpRestoreView.as_view(), name="restore_dump"),
    path("restore_details/", DumpRestoreView.as_view(), name="restore_details"),

]

