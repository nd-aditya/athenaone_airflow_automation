from rest_framework import serializers
from .models import Notification, NotificationType, NotificationPriority, NotificationStatus


class NotificationSerializer(serializers.ModelSerializer):
    """Serializer for Notification model"""
    notification_type_display = serializers.CharField(source='get_notification_type_display', read_only=True)
    priority_display = serializers.CharField(source='get_priority_display', read_only=True)
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    is_expired = serializers.BooleanField(read_only=True)
    time_since_created = serializers.SerializerMethodField()
    time_since_read = serializers.SerializerMethodField()

    class Meta:
        model = Notification
        fields = [
            'id', 'notification_type', 'notification_type_display',
            'priority', 'priority_display', 'status', 'status_display',
            'title', 'message', 'task_name', 'progress', 'error_code',
            'details', 'created_at', 'updated_at',
            'read_at', 'expires_at', 'is_expired', 'time_since_created',
            'time_since_read'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'is_expired']

    def get_time_since_created(self, obj):
        """Get human-readable time since creation"""
        from django.utils import timezone
        now = timezone.now()
        
        # Handle both model instances and dict objects
        if hasattr(obj, 'created_at'):
            created_at = obj.created_at
        elif isinstance(obj, dict) and 'created_at' in obj:
            created_at = obj['created_at']
        else:
            return "Unknown"
            
        delta = now - created_at
        
        if delta.days > 0:
            return f"{delta.days} day{'s' if delta.days != 1 else ''} ago"
        elif delta.seconds > 3600:
            hours = delta.seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif delta.seconds > 60:
            minutes = delta.seconds // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        else:
            return "Just now"

    def get_time_since_read(self, obj):
        """Get human-readable time since read"""
        # Handle both model instances and dict objects
        if hasattr(obj, 'read_at'):
            read_at = obj.read_at
        elif isinstance(obj, dict) and 'read_at' in obj:
            read_at = obj['read_at']
        else:
            read_at = None
            
        if not read_at:
            return None
        
        from django.utils import timezone
        now = timezone.now()
        delta = now - read_at
        
        if delta.days > 0:
            return f"{delta.days} day{'s' if delta.days != 1 else ''} ago"
        elif delta.seconds > 3600:
            hours = delta.seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif delta.seconds > 60:
            minutes = delta.seconds // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        else:
            return "Just now"


class NotificationCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating notifications"""
    
    class Meta:
        model = Notification
        fields = [
            'notification_type', 'priority', 'title', 'message',
            'task_name', 'progress', 'error_code', 'details',
            'expires_at'
        ]

    def validate_progress(self, value):
        """Validate progress is between 0 and 100"""
        if value is not None and (value < 0 or value > 100):
            raise serializers.ValidationError("Progress must be between 0 and 100")
        return value

    def validate_notification_type(self, value):
        """Validate notification type"""
        if value not in [choice[0] for choice in NotificationType.choices]:
            raise serializers.ValidationError("Invalid notification type")
        return value

    def validate_priority(self, value):
        """Validate priority"""
        if value not in [choice[0] for choice in NotificationPriority.choices]:
            raise serializers.ValidationError("Invalid priority")
        return value


class NotificationUpdateSerializer(serializers.ModelSerializer):
    """Serializer for updating notifications"""
    
    class Meta:
        model = Notification
        fields = ['status', 'title', 'message', 'details']

    def validate_status(self, value):
        """Validate status"""
        if value not in [choice[0] for choice in NotificationStatus.choices]:
            raise serializers.ValidationError("Invalid status")
        return value


class NotificationSummarySerializer(serializers.ModelSerializer):
    """Lightweight serializer for notification summaries"""
    notification_type_display = serializers.CharField(source='get_notification_type_display', read_only=True)
    priority_display = serializers.CharField(source='get_priority_display', read_only=True)
    time_since_created = serializers.SerializerMethodField()

    class Meta:
        model = Notification
        fields = [
            'id', 'notification_type', 'notification_type_display',
            'priority', 'priority_display', 'status', 'title',
            'task_name', 'progress', 'created_at', 'time_since_created'
        ]

    def get_time_since_created(self, obj):
        """Get human-readable time since creation"""
        from django.utils import timezone
        now = timezone.now()
        
        # Handle both model instances and dict objects
        if hasattr(obj, 'created_at'):
            created_at = obj.created_at
        elif isinstance(obj, dict) and 'created_at' in obj:
            created_at = obj['created_at']
        else:
            return "Unknown"
            
        delta = now - created_at
        
        if delta.days > 0:
            return f"{delta.days}d ago"
        elif delta.seconds > 3600:
            hours = delta.seconds // 3600
            return f"{hours}h ago"
        elif delta.seconds > 60:
            minutes = delta.seconds // 60
            return f"{minutes}m ago"
        else:
            return "now"


class NotificationStatsSerializer(serializers.Serializer):
    """Serializer for notification statistics"""
    total_notifications = serializers.IntegerField()
    unread_count = serializers.IntegerField()
    read_count = serializers.IntegerField()
    archived_count = serializers.IntegerField()
    by_type = serializers.DictField()
    by_priority = serializers.DictField()
    recent_notifications = NotificationSummarySerializer(many=True)


