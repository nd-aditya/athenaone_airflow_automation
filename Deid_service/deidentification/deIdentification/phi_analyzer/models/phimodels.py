from django.db import models
from django.utils import timezone
from nd_api.models import Clients, ClientDataDump


class PHIAnalysisSession(models.Model):
    """Model to track PHI analysis sessions"""
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
    ]
    
    id = models.AutoField(primary_key=True)
    client = models.ForeignKey(Clients, on_delete=models.CASCADE, related_name='phi_analysis_sessions')
    dump = models.ForeignKey(ClientDataDump, on_delete=models.CASCADE, related_name='phi_analysis_sessions')
    
    # Analysis configuration
    config = models.JSONField(default=dict, help_text="Analysis configuration parameters")
    
    # Status tracking
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    progress = models.IntegerField(default=0, help_text="Progress percentage (0-100)")
    current_step = models.CharField(max_length=200, blank=True, help_text="Current processing step")
    
    # Statistics
    total_tables = models.IntegerField(default=0)
    processed_tables = models.IntegerField(default=0)
    total_columns = models.IntegerField(default=0)
    phi_columns_found = models.IntegerField(default=0)
    validation_passed = models.IntegerField(default=0)
    validation_failed = models.IntegerField(default=0)
    errors_count = models.IntegerField(default=0)
    
    # Timing
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Results
    output_file_path = models.CharField(max_length=500, blank=True, help_text="Path to generated CSV file")
    error_message = models.TextField(blank=True, help_text="Error message if analysis failed")
    
    # Task reference
    task_chain = models.ForeignKey('worker.Chain', on_delete=models.SET_NULL, null=True, blank=True)
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['client', 'status']),
            models.Index(fields=['status', 'created_at']),
        ]
    
    def __str__(self):
        return f"PHI Analysis {self.id} - {self.client.name} - {self.dump.dump_name} ({self.status})"
    
    @property
    def duration(self):
        """Calculate analysis duration"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        elif self.started_at:
            return timezone.now() - self.started_at
        return None
    
    def update_progress(self, progress, current_step=None):
        """Update progress and current step"""
        self.progress = min(100, max(0, progress))
        if current_step:
            self.current_step = current_step
        self.save(update_fields=['progress', 'current_step', 'updated_at'])
    
    def mark_started(self):
        """Mark analysis as started"""
        self.status = 'running'
        self.started_at = timezone.now()
        self.save(update_fields=['status', 'started_at', 'updated_at'])
    
    def mark_completed(self, output_file_path=None):
        """Mark analysis as completed"""
        self.status = 'completed'
        self.completed_at = timezone.now()
        self.progress = 100
        if output_file_path:
            self.output_file_path = output_file_path
        self.save(update_fields=['status', 'completed_at', 'progress', 'output_file_path', 'updated_at'])
    
    def mark_failed(self, error_message=None):
        """Mark analysis as failed"""
        self.status = 'failed'
        self.completed_at = timezone.now()
        if error_message:
            self.error_message = error_message
        self.save(update_fields=['status', 'completed_at', 'error_message', 'updated_at'])


class PHITableResult(models.Model):
    """Model to store results for individual tables"""
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    id = models.AutoField(primary_key=True)
    session = models.ForeignKey(PHIAnalysisSession, on_delete=models.CASCADE, related_name='table_results')
    
    # Table information
    table_name = models.CharField(max_length=200, db_index=True)
    table_index = models.IntegerField(help_text="Order of table in analysis")
    
    # Processing status
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    progress = models.IntegerField(default=0, help_text="Table processing progress (0-100)")
    
    # Statistics
    total_columns = models.IntegerField(default=0)
    phi_columns = models.IntegerField(default=0)
    validation_passed = models.IntegerField(default=0)
    validation_failed = models.IntegerField(default=0)
    
    # Timing
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Error handling
    error_message = models.TextField(blank=True)
    retry_count = models.IntegerField(default=0)
    
    class Meta:
        ordering = ['table_index']
        unique_together = ['session', 'table_name']
        indexes = [
            models.Index(fields=['session', 'status']),
            models.Index(fields=['table_name', 'status']),
        ]
    
    def __str__(self):
        return f"Table {self.table_name} - Session {self.session.id} ({self.status})"
    
    def mark_started(self):
        """Mark table processing as started"""
        self.status = 'processing'
        self.started_at = timezone.now()
        self.save(update_fields=['status', 'started_at', 'updated_at'])
    
    def mark_completed(self):
        """Mark table processing as completed"""
        self.status = 'completed'
        self.completed_at = timezone.now()
        self.progress = 100
        self.save(update_fields=['status', 'completed_at', 'progress', 'updated_at'])
    
    def mark_failed(self, error_message=None):
        """Mark table processing as failed"""
        self.status = 'failed'
        self.completed_at = timezone.now()
        self.retry_count += 1
        if error_message:
            self.error_message = error_message
        self.save(update_fields=['status', 'completed_at', 'retry_count', 'error_message', 'updated_at'])


class PHIColumnResult(models.Model):
    """Model to store results for individual columns"""
    
    PHI_CHOICES = [
        ('yes', 'Yes'),
        ('no', 'No'),
        ('unknown', 'Unknown'),
    ]
    
    
    id = models.AutoField(primary_key=True)
    table_result = models.ForeignKey(PHITableResult, on_delete=models.CASCADE, related_name='column_results')
    
    # Column information
    column_name = models.CharField(max_length=200, db_index=True)
    
    # PHI classification results
    is_phi = models.CharField(max_length=10, choices=PHI_CHOICES, default='unknown')
    phi_rule = models.CharField(max_length=100, blank=True, help_text="Applied PHI rule")
    
    # Remarks and details
    pipeline_remark = models.TextField(blank=True, help_text="Pipeline validation remarks")
    user_remarks = models.TextField(blank=True, help_text="Manual user remarks")
    
    # Manual verification status
    is_manually_verified = models.BooleanField(default=False, help_text="Manual verification completed status")
    verified_by = models.CharField(max_length=200, blank=True, help_text="User who performed manual verification")
    verified_at = models.DateTimeField(null=True, blank=True, help_text="When manual verification was completed")
    
    # Timing
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['column_name']
        unique_together = ['table_result', 'column_name']
        indexes = [
            models.Index(fields=['table_result', 'is_phi']),
        ]
    
    def __str__(self):
        return f"Column {self.column_name} - Table {self.table_result.table_name} - PHI: {self.is_phi}"
    
    @property
    def is_phi_detected(self):
        """Check if PHI was detected"""
        return self.is_phi == 'yes'


class PHIAnalysisProgress(models.Model):
    """Model to track detailed progress of PHI analysis"""
    
    id = models.AutoField(primary_key=True)
    session = models.ForeignKey(PHIAnalysisSession, on_delete=models.CASCADE, related_name='progress_logs')
    
    # Progress information
    step_name = models.CharField(max_length=200)
    step_index = models.IntegerField(help_text="Order of step in analysis")
    progress_percentage = models.IntegerField(help_text="Overall progress percentage")
    
    # Details
    message = models.TextField(blank=True)
    details = models.JSONField(default=dict, help_text="Additional step details")
    
    # Timing
    timestamp = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['timestamp']
        indexes = [
            models.Index(fields=['session', 'timestamp']),
            models.Index(fields=['step_index', 'timestamp']),
        ]
    
    def __str__(self):
        return f"Progress {self.session.id} - {self.step_name} ({self.progress_percentage}%)"
