from django.urls import path
from nd_api_v2.views.register_configs import (
    RegisterClientRunConfigsView,
    RegisterMappingConfigsView,
    RegisterMasterTableConfigsView,
    RegisterPIIMaskingConfigsView,
    RegisterQCConfigsView,
)
from nd_api_v2.views.table_metadata import (
    TableMetadataListView,
    TableMetadataDetailView,
    TableMetadataBulkUpdateView,
    TableMetadataExportView,
)
from nd_api_v2.views.query_management import (
    StartDailyDumpProcessingView,
    IncrementalQueueListView,
    IncrementalQueueDetailView,
    IncrementalQueueUpdateView,
    IncrementalQueueBulkUpdateView,
)
from nd_api_v2.views.monitoring import (
    MonitoringQueueTablesView,
    MonitoringQueuesListView,
)
from nd_api_v2.views.deid_rules import DEIDRulesView

from nd_api_v2.views.phi_config import (
    DownloadConfigAsCSV,
    UploadConfigFromCSV,
)

from nd_api_v2.views.operations import StartDeIdentificationView, QCBulkView


from nd_api_v2.views.incremental_scheduler.athenaone import (
    IncrementalPipelineConfigView,
    IncrementalPipelineControlView,
    IncrementalPipelineStatusView,
    IncrementalPipelineLogsView,
    IncrementalPipelineHistoryView,
    SchedulerStatusView,
    SchedulerEnableView,
    SchedulerDisableView,
    ConfigEditorSaveView,
)
from nd_api_v2.views.incremental_scheduler.ecw_with_diff_view import (
    ECWPipelineConfigView,
    ECWPipelineControlView,
    ECWPipelineStatusView,
    ECWPipelineLogsView,
    ECWPipelineHistoryView,
    ECWSchedulerStatusView,
    ECWSchedulerEnableView,
    ECWSchedulerDisableView,
    ECWConfigEditorSaveView,
)
from nd_api_v2.views.incremental_scheduler.scheduler import IncrementalSchedulerTypeView


urlpatterns = [
    path("deid-rules/", DEIDRulesView.as_view(), name="deid_rules"),

    # Client Run Config APIs
    path("configs/client-run/", RegisterClientRunConfigsView.as_view(), name="client_run_config"),
    
    # Mapping Config APIs
    path("configs/mapping/", RegisterMappingConfigsView.as_view(), name="mapping_config"),
    
    # Master Table Config APIs
    path("configs/master-table/", RegisterMasterTableConfigsView.as_view(), name="master_table_config"),
    
    # PII Masking Config APIs
    path("configs/pii-masking/", RegisterPIIMaskingConfigsView.as_view(), name="pii_masking_config"),
    
    # QC Config APIs
    path("configs/qc/", RegisterQCConfigsView.as_view(), name="qc_config"),
    
    # Table Metadata APIs
    path("table-metadata/", TableMetadataListView.as_view(), name="table_metadata_list"),
    path("table-metadata/<int:table_id>/", TableMetadataDetailView.as_view(), name="table_metadata_detail"),
    path("table-metadata/bulk-update/", TableMetadataBulkUpdateView.as_view(), name="table_metadata_bulk_update"),
    path("table-metadata/export/", TableMetadataExportView.as_view(), name="table_metadata_export"),
    
    # Queue Management APIs
    path("queue-management/start-daily-dump/", StartDailyDumpProcessingView.as_view(), name="start_daily_dump"),
    path("queue-management/queues/", IncrementalQueueListView.as_view(), name="incremental_queue_list"),
    path("queue-management/queues/<str:queue_name>/", IncrementalQueueDetailView.as_view(), name="incremental_queue_detail"),
    path("queue-management/queues/<int:queue_id>/update/", IncrementalQueueUpdateView.as_view(), name="incremental_queue_update"),
    path("queue-management/queues/bulk-update/", IncrementalQueueBulkUpdateView.as_view(), name="incremental_queue_bulk_update"),
    
    # Monitoring APIs
    path("monitoring/queues/", MonitoringQueuesListView.as_view(), name="monitoring_queues_list"),
    path("monitoring/queues/<str:queue_name>/tables/", MonitoringQueueTablesView.as_view(), name="monitoring_queue_tables"),
    
    # Incremental Pipeline Type API
    path("incremental-pipeline/type/", IncrementalSchedulerTypeView.as_view(), name="incremental_pipeline_type"),
    
    # Incremental Pipeline APIs (AthenaOne)
    path("incremental-pipeline/config/", IncrementalPipelineConfigView.as_view(), name="incremental_config"),
    path("incremental-pipeline/control/", IncrementalPipelineControlView.as_view(), name="incremental_control"),
    path("incremental-pipeline/status/", IncrementalPipelineStatusView.as_view(), name="incremental_status"),
    path("incremental-pipeline/logs/", IncrementalPipelineLogsView.as_view(), name="incremental_logs"),
    path("incremental-pipeline/history/", IncrementalPipelineHistoryView.as_view(), name="incremental_history"),
    
    # ECW with Diff Pipeline APIs
    path("ecw-with-diff-pipeline/config/", ECWPipelineConfigView.as_view(), name="ecw_with_diff_config"),
    path("ecw-with-diff-pipeline/control/", ECWPipelineControlView.as_view(), name="ecw_with_diff_control"),
    path("ecw-with-diff-pipeline/status/", ECWPipelineStatusView.as_view(), name="ecw_with_diff_status"),
    path("ecw-with-diff-pipeline/logs/", ECWPipelineLogsView.as_view(), name="ecw_with_diff_logs"),
    path("ecw-with-diff-pipeline/history/", ECWPipelineHistoryView.as_view(), name="ecw_with_diff_history"),
    path("ecw-with-diff-pipeline/scheduler/status/", ECWSchedulerStatusView.as_view(), name="ecw_scheduler_status"),
    path("ecw-with-diff-pipeline/scheduler/enable/", ECWSchedulerEnableView.as_view(), name="ecw_scheduler_enable"),
    path("ecw-with-diff-pipeline/scheduler/disable/", ECWSchedulerDisableView.as_view(), name="ecw_scheduler_disable"),
    path("ecw-with-diff-pipeline/config/save/", ECWConfigEditorSaveView.as_view(), name="ecw_config_save"),
    
    # Scheduler APIs (AthenaOne)
    path("incremental-pipeline/scheduler/status/", SchedulerStatusView.as_view(), name="scheduler_status"),
    path("incremental-pipeline/scheduler/enable/", SchedulerEnableView.as_view(), name="scheduler_enable"),
    path("incremental-pipeline/scheduler/disable/", SchedulerDisableView.as_view(), name="scheduler_disable"),
    
    # Config Editor API (AthenaOne)
    path("incremental-pipeline/config/save/", ConfigEditorSaveView.as_view(), name="config_save"),

    # phi marking
    path("phi-marking/download/", DownloadConfigAsCSV.as_view(), name="download_phi_marking"),
    path("phi-marking/upload/", UploadConfigFromCSV.as_view(), name="upload_phi_marking"),

    # Operations APIs
    path("operations/deid/start/<int:queue_id>/", StartDeIdentificationView.as_view(), name="start_deid"),
    path("operations/qc/start/<int:queue_id>/", QCBulkView.as_view(), name="start_qc"),
]

