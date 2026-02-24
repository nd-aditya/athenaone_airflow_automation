from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
import uuid


class NotificationType(models.TextChoices):
    """Notification type choices"""
    TASK_STATUS = 'task_status', 'Task Status'
    TASK_PROGRESS = 'task_progress', 'Task Progress'
    TASK_ERROR = 'task_error', 'Task Error'
    SYSTEM_ALERT = 'system_alert', 'System Alert'
    INFO = 'info', 'Information'
    WARNING = 'warning', 'Warning'
    SUCCESS = 'success', 'Success'


class NotificationPriority(models.TextChoices):
    """Notification priority choices"""
    LOW = 'low', 'Low'
    MEDIUM = 'medium', 'Medium'
    HIGH = 'high', 'High'
    CRITICAL = 'critical', 'Critical'


class NotificationStatus(models.TextChoices):
    """Notification status choices"""
    UNREAD = 'unread', 'Unread'
    READ = 'read', 'Read'
    ARCHIVED = 'archived', 'Archived'


class Notification(models.Model):
    """
    Model to store WebSocket notifications in database
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    # Removed user field since there's no user authentication
    notification_type = models.CharField(
        max_length=20,
        choices=NotificationType.choices,
        default=NotificationType.INFO
    )
    priority = models.CharField(
        max_length=10,
        choices=NotificationPriority.choices,
        default=NotificationPriority.MEDIUM
    )
    status = models.CharField(
        max_length=10,
        choices=NotificationStatus.choices,
        default=NotificationStatus.UNREAD
    )
    title = models.CharField(max_length=200, help_text="Notification title")
    message = models.TextField(help_text="Notification message")
    task_name = models.CharField(
        max_length=100, 
        null=True, 
        blank=True,
        help_text="Name of the task this notification relates to"
    )
    progress = models.IntegerField(
        null=True, 
        blank=True,
        help_text="Progress percentage (0-100) if applicable"
    )
    error_code = models.CharField(
        max_length=50, 
        null=True, 
        blank=True,
        help_text="Error code if this is an error notification"
    )
    details = models.JSONField(
        null=True, 
        blank=True,
        help_text="Additional details as JSON"
    )
    # Removed is_broadcast since all notifications are broadcast to everyone
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    read_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(
        null=True, 
        blank=True,
        help_text="When this notification expires (optional)"
    )

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['notification_type']),
            models.Index(fields=['created_at']),
            models.Index(fields=['status']),
        ]

    def __str__(self):
        return f"{self.title} - {self.get_notification_type_display()}"

    def mark_as_read(self):
        """Mark notification as read"""
        if self.status == NotificationStatus.UNREAD:
            self.status = NotificationStatus.READ
            self.read_at = timezone.now()
            self.save(update_fields=['status', 'read_at'])

    def mark_as_archived(self):
        """Mark notification as archived"""
        self.status = NotificationStatus.ARCHIVED
        self.save(update_fields=['status'])

    def is_expired(self):
        """Check if notification has expired"""
        if self.expires_at:
            return timezone.now() > self.expires_at
        return False

    @classmethod
    def get_all_notifications(cls, limit=50, status=None, notification_type=None):
        """
        Get all notifications (since there's no user authentication)
        
        Args:
            limit: Maximum number of notifications to return
            status: Filter by notification status
            notification_type: Filter by notification type
        """
        queryset = cls.objects.exclude(expires_at__lt=timezone.now())
        
        if status:
            queryset = queryset.filter(status=status)
        if notification_type:
            queryset = queryset.filter(notification_type=notification_type)
            
        return queryset[:limit]

    @classmethod
    def get_unread_count(cls):
        """Get count of unread notifications"""
        return cls.objects.filter(
            status=NotificationStatus.UNREAD
        ).exclude(expires_at__lt=timezone.now()).count()

    @classmethod
    def mark_all_as_read(cls):
        """Mark all notifications as read"""
        cls.objects.filter(
            status=NotificationStatus.UNREAD
        ).update(
            status=NotificationStatus.READ,
            read_at=timezone.now()
        )

    @classmethod
    def cleanup_expired(cls):
        """Clean up expired notifications"""
        expired_count = cls.objects.filter(
            expires_at__lt=timezone.now()
        ).count()
        cls.objects.filter(expires_at__lt=timezone.now()).delete()
        return expired_count


