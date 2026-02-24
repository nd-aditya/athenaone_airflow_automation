from rest_framework import generics, status, filters
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from django.db.models import Count
from django.utils import timezone
from django.conf import settings

from .models import Notification, NotificationType, NotificationPriority, NotificationStatus
from .serializers import (
    NotificationSerializer, NotificationCreateSerializer, NotificationUpdateSerializer,
    NotificationSummarySerializer, NotificationStatsSerializer
)


class NotificationPagination(PageNumberPagination):
    """Custom pagination for notifications"""
    page_size = 20
    page_size_query_param = 'page_size'
    max_page_size = 100


class NotificationListView(generics.ListCreateAPIView):
    """
    List all notifications or create new notifications
    GET: List all notifications (no authentication required)
    POST: Create a new notification
    """
    pagination_class = NotificationPagination
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['title', 'message', 'task_name']
    ordering_fields = ['created_at', 'priority', 'status']
    ordering = ['-created_at']

    def get_serializer_class(self):
        if self.request.method == 'POST':
            return NotificationCreateSerializer
        return NotificationSerializer

    def get_queryset(self):
        """Get all notifications (no user filtering)"""
        queryset = Notification.objects.exclude(expires_at__lt=timezone.now())
        
        # Filter by status
        status_filter = self.request.query_params.get('status')
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        # Filter by notification type
        type_filter = self.request.query_params.get('type')
        if type_filter:
            queryset = queryset.filter(notification_type=type_filter)
        
        # Filter by priority
        priority_filter = self.request.query_params.get('priority')
        if priority_filter:
            queryset = queryset.filter(priority=priority_filter)
        
        # Filter by task name
        task_filter = self.request.query_params.get('task_name')
        if task_filter:
            queryset = queryset.filter(task_name__icontains=task_filter)
        
        return queryset


class NotificationDetailView(generics.RetrieveUpdateDestroyAPIView):
    """
    Retrieve, update or delete a notification
    """
    serializer_class = NotificationSerializer

    def get_queryset(self):
        """Get all notifications (no user filtering)"""
        return Notification.objects.all()

    def retrieve(self, request, *args, **kwargs):
        """Mark notification as read when retrieved"""
        instance = self.get_object()
        if instance.status == NotificationStatus.UNREAD:
            instance.mark_as_read()
        serializer = self.get_serializer(instance)
        return Response(serializer.data)


@api_view(['GET'])
def notification_stats(request):
    """
    Get notification statistics (no authentication required)
    """
    # Get all notifications
    notifications = Notification.objects.exclude(expires_at__lt=timezone.now())
    
    # Calculate statistics
    total_notifications = notifications.count()
    unread_count = notifications.filter(status=NotificationStatus.UNREAD).count()
    read_count = notifications.filter(status=NotificationStatus.READ).count()
    archived_count = notifications.filter(status=NotificationStatus.ARCHIVED).count()
    
    # Count by type
    by_type = {}
    for choice in NotificationType.choices:
        count = notifications.filter(notification_type=choice[0]).count()
        by_type[choice[0]] = count
    
    # Count by priority
    by_priority = {}
    for choice in NotificationPriority.choices:
        count = notifications.filter(priority=choice[0]).count()
        by_priority[choice[0]] = count
    
    # Get recent notifications
    recent_notifications = notifications.order_by('-created_at')[:5]
    
    stats_data = {
        'total_notifications': total_notifications,
        'unread_count': unread_count,
        'read_count': read_count,
        'archived_count': archived_count,
        'by_type': by_type,
        'by_priority': by_priority,
        'recent_notifications': recent_notifications
    }
    
    serializer = NotificationStatsSerializer(stats_data)
    return Response(serializer.data)


@api_view(['POST'])
def mark_all_as_read(request):
    """
    Mark all notifications as read
    """
    Notification.mark_all_as_read()
    return Response({'message': 'All notifications marked as read'})


@api_view(['POST'])
def mark_notification_read(request, notification_id):
    """
    Mark a specific notification as read
    """
    try:
        notification = Notification.objects.get(id=notification_id)
        notification.mark_as_read()
        return Response({'message': 'Notification marked as read'})
    except Notification.DoesNotExist:
        return Response(
            {'error': 'Notification not found'}, 
            status=status.HTTP_404_NOT_FOUND
        )


@api_view(['POST'])
def mark_notification_archived(request, notification_id):
    """
    Mark a specific notification as archived
    """
    try:
        notification = Notification.objects.get(id=notification_id)
        notification.mark_as_archived()
        return Response({'message': 'Notification archived'})
    except Notification.DoesNotExist:
        return Response(
            {'error': 'Notification not found'}, 
            status=status.HTTP_404_NOT_FOUND
        )


@api_view(['GET'])
def unread_count(request):
    """
    Get unread notification count
    """
    count = Notification.get_unread_count()
    return Response({'unread_count': count})


@api_view(['GET'])
def notification_types(request):
    """
    Get available notification types and priorities
    """
    return Response({
        'notification_types': [
            {'value': choice[0], 'label': choice[1]} 
            for choice in NotificationType.choices
        ],
        'priorities': [
            {'value': choice[0], 'label': choice[1]} 
            for choice in NotificationPriority.choices
        ],
        'statuses': [
            {'value': choice[0], 'label': choice[1]} 
            for choice in NotificationStatus.choices
        ]
    })


@api_view(['POST'])
def cleanup_expired_notifications(request):
    """
    Clean up expired notifications
    """
    expired_count = Notification.cleanup_expired()
    return Response({
        'message': f'Cleaned up {expired_count} expired notifications'
    })


@api_view(['GET'])
def websocket_connection_info(request):
    """
    Get WebSocket connection information for the frontend
    """
    protocol = 'wss' if request.is_secure() else 'ws'
    host = request.get_host()
    websocket_url = f"{protocol}://{host}/ws/tasks/"
    
    return Response({
        'websocket_url': websocket_url,
        'authentication_required': False,  # No authentication required
        'supported_message_types': [
            'ping', 'subscribe'
        ],
        'supported_subscription_types': [
            'all', 'deidentification', 'system', 'alerts'
        ]
    })
