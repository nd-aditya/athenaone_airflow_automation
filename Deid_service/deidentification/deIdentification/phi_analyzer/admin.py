from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from .models import PHIAnalysisSession, PHITableResult, PHIColumnResult, PHIAnalysisProgress, ModelConfiguration


@admin.register(PHIAnalysisSession)
class PHIAnalysisSessionAdmin(admin.ModelAdmin):
    list_display = [
        'id', 'client', 'dump', 'status', 'progress', 'current_step',
        'total_tables', 'processed_tables', 'phi_columns_found',
        'created_at', 'started_at', 'completed_at', 'duration_display'
    ]
    list_filter = ['status', 'client', 'created_at', 'started_at']
    search_fields = ['client__name', 'dump__dump_name', 'current_step']
    readonly_fields = ['created_at', 'updated_at', 'duration_display', 'statistics_display']
    ordering = ['-created_at']
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('client', 'dump', 'config', 'status', 'progress', 'current_step')
        }),
        ('Statistics', {
            'fields': ('total_tables', 'processed_tables', 'total_columns', 
                      'phi_columns_found', 'errors_count')
        }),
        ('Timing', {
            'fields': ('created_at', 'started_at', 'completed_at', 'duration_display')
        }),
        ('Results', {
            'fields': ('output_file_path', 'error_message')
        }),
        ('Task Management', {
            'fields': ('task_chain',)
        })
    )
    
    def duration_display(self, obj):
        if obj.duration:
            return str(obj.duration)
        return "-"
    duration_display.short_description = "Duration"
    
    def statistics_display(self, obj):
        return format_html(
            "<strong>Tables:</strong> {} / {}<br>"
            "<strong>Columns:</strong> {}<br>"
            "<strong>PHI Columns:</strong> {}<br>"
            "<strong>Errors:</strong> {}",
            obj.processed_tables, obj.total_tables,
            obj.total_columns, obj.phi_columns_found,
            obj.errors_count
        )
    statistics_display.short_description = "Statistics"
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('client', 'dump')


@admin.register(PHITableResult)
class PHITableResultAdmin(admin.ModelAdmin):
    list_display = [
        'id', 'session_link', 'table_name', 'table_index', 'status', 'progress',
        'total_columns', 'phi_columns', 'started_at', 'completed_at', 'retry_count'
    ]
    list_filter = ['status', 'session__client', 'session__dump', 'created_at']
    search_fields = ['table_name', 'session__client__name', 'session__dump__dump_name']
    readonly_fields = ['created_at', 'updated_at', 'duration_display']
    ordering = ['session', 'table_index']
    
    def session_link(self, obj):
        url = reverse('admin:phi_analyzer_phianalysissession_change', args=[obj.session.id])
        return format_html('<a href="{}">Session {}</a>', url, obj.session.id)
    session_link.short_description = "Session"
    
    def duration_display(self, obj):
        if obj.started_at and obj.completed_at:
            duration = obj.completed_at - obj.started_at
            return str(duration)
        return "-"
    duration_display.short_description = "Duration"
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('session__client', 'session__dump')


@admin.register(PHIColumnResult)
class PHIColumnResultAdmin(admin.ModelAdmin):
    list_display = [
        'id', 'table_link', 'column_name', 'is_phi', 'phi_rule', 'created_at'
    ]
    list_filter = ['is_phi', 'created_at']
    search_fields = ['column_name', 'table_result__table_name', 'phi_rule']
    readonly_fields = ['created_at', 'updated_at']
    ordering = ['table_result', 'column_name']
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('table_result', 'column_name')
        }),
        ('PHI Classification', {
            'fields': ('is_phi', 'phi_rule')
        }),
        ('Details', {
            'fields': ('pipeline_remark',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at')
        })
    )
    
    def table_link(self, obj):
        url = reverse('admin:phi_analyzer_phitableresult_change', args=[obj.table_result.id])
        return format_html('<a href="{}">{}</a>', url, obj.table_result.table_name)
    table_link.short_description = "Table"
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('table_result__session__client', 'table_result__session__dump')


@admin.register(PHIAnalysisProgress)
class PHIAnalysisProgressAdmin(admin.ModelAdmin):
    list_display = [
        'id', 'session_link', 'step_name', 'step_index', 'progress_percentage',
        'message', 'timestamp'
    ]
    list_filter = ['step_name', 'session__client', 'session__status', 'timestamp']
    search_fields = ['step_name', 'message', 'session__client__name']
    readonly_fields = ['timestamp', 'details_display']
    ordering = ['-timestamp']
    
    fieldsets = (
        ('Progress Information', {
            'fields': ('session', 'step_name', 'step_index', 'progress_percentage', 'message')
        }),
        ('Details', {
            'fields': ('details_display', 'timestamp')
        })
    )
    
    def session_link(self, obj):
        url = reverse('admin:phi_analyzer_phianalysissession_change', args=[obj.session.id])
        return format_html('<a href="{}">Session {}</a>', url, obj.session.id)
    session_link.short_description = "Session"
    
    def details_display(self, obj):
        if obj.details:
            return format_html('<pre>{}</pre>', str(obj.details))
        return "-"
    details_display.short_description = "Details"
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('session__client', 'session__dump')


@admin.register(ModelConfiguration)
class ModelConfigurationAdmin(admin.ModelAdmin):
    list_display = [
        'name', 'model_name', 'temperature', 'max_tokens', 'sample_size',
        'is_default', 'is_active', 'created_by', 'created_at'
    ]
    list_filter = ['is_default', 'is_active', 'model_name', 'created_at']
    search_fields = ['name', 'description', 'model_name', 'created_by']
    readonly_fields = ['created_at', 'updated_at']
    ordering = ['-is_default', 'name']
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'description', 'is_default', 'is_active')
        }),
        ('Model Parameters', {
            'fields': ('model_name', 'temperature', 'max_tokens', 'sample_size')
        }),
        ('Metadata', {
            'fields': ('created_by', 'created_at', 'updated_at')
        })
    )
    
    def save_model(self, request, obj, form, change):
        if not change:  # New object
            obj.created_by = request.user.username if request.user.is_authenticated else 'admin'
        super().save_model(request, obj, form, change)
