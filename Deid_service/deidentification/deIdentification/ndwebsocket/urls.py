from django.urls import path
from . import views
from .consumers import TaskConsumer

# Notification API URLs
urlpatterns = [
    # Notification CRUD
    path('notifications/', views.NotificationListView.as_view(), name='notification-list'),
    path('notifications/<uuid:pk>/', views.NotificationDetailView.as_view(), name='notification-detail'),
    
    # Notification actions
    path('notifications/stats/', views.notification_stats, name='notification-stats'),
    path('notifications/mark-all-read/', views.mark_all_as_read, name='mark-all-read'),
    path('notifications/<uuid:notification_id>/mark-read/', views.mark_notification_read, name='mark-notification-read'),
    path('notifications/<uuid:notification_id>/archive/', views.mark_notification_archived, name='mark-notification-archived'),
    path('notifications/unread-count/', views.unread_count, name='unread-count'),
    
    # Utility endpoints
    path('types/', views.notification_types, name='notification-types'),
    path('cleanup/', views.cleanup_expired_notifications, name='cleanup-expired'),
    path('websocket-info/', views.websocket_connection_info, name='websocket-info'),
]

# WebSocket URLs (for ASGI routing)
websocket_urlpatterns = [
    path("ws/tasks/", TaskConsumer.as_asgi()),
    path("tasks/", TaskConsumer.as_asgi()),  # Handle clients connecting to tasks/ without ws/ prefix
    
]