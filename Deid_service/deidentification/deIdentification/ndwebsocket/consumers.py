import json
import logging
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from django.contrib.auth.models import AnonymousUser
from django.conf import settings

logger = logging.getLogger(__name__)


class TaskConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        """Handle WebSocket connection"""
        try:
            # Get user from scope (if authentication is enabled)
            self.user = self.scope.get("user", AnonymousUser())
            
            # Accept the WebSocket connection first
            await self.accept()
            
            # Add to task group after accepting
            await self.channel_layer.group_add("task_group", self.channel_name)
            
            # Send connection confirmation
            await self.send(text_data=json.dumps({
                "type": "connection",
                "status": "connected",
                "message": "Successfully connected to task updates"
            }))
            
            logger.info(f"WebSocket connected: {self.channel_name}")
            
        except Exception as e:
            logger.error(f"WebSocket connection error: {str(e)}")
            await self.close(code=4000)

    async def disconnect(self, close_code):
        """Handle WebSocket disconnection"""
        try:
            await self.channel_layer.group_discard("task_group", self.channel_name)
            logger.info(f"WebSocket disconnected: {self.channel_name}, code: {close_code}")
        except Exception as e:
            logger.error(f"WebSocket disconnection error: {str(e)}")

    async def receive(self, text_data):
        """Handle messages received from client"""
        try:
            data = json.loads(text_data)
            message_type = data.get('type', 'unknown')
            
            if message_type == 'ping':
                await self.send(text_data=json.dumps({
                    "type": "pong",
                    "message": "pong"
                }))
            elif message_type == 'subscribe':
                # Handle subscription to specific task types
                task_type = data.get('task_type', 'all')
                try:
                    await self.channel_layer.group_add(f"task_{task_type}", self.channel_name)
                    await self.send(text_data=json.dumps({
                        "type": "subscription",
                        "status": "subscribed",
                        "task_type": task_type
                    }))
                except Exception as e:
                    logger.error(f"Error adding to group {task_type}: {str(e)}")
                    await self.send(text_data=json.dumps({
                        "type": "error",
                        "message": f"Failed to subscribe to {task_type}"
                    }))
            elif message_type == 'connection':
                # Handle connection confirmation request
                await self.send(text_data=json.dumps({
                    "type": "connection",
                    "status": "connected",
                    "message": "Connection confirmed"
                }))
            else:
                await self.send(text_data=json.dumps({
                    "type": "error",
                    "message": f"Unknown message type: {message_type}"
                }))
                
        except json.JSONDecodeError:
            await self.send(text_data=json.dumps({
                "type": "error",
                "message": "Invalid JSON format"
            }))
        except Exception as e:
            logger.error(f"WebSocket receive error: {str(e)}")
            await self.send(text_data=json.dumps({
                "type": "error",
                "message": "Internal server error"
            }))

    async def send_task_status(self, event: dict):
        """Send task status updates to client"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'task_status',
                **event
            }))
        except Exception as e:
            logger.error(f"Error sending task status: {str(e)}")

    async def send_task_progress(self, event: dict):
        """Send task progress updates to client"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'task_progress',
                **event
            }))
        except Exception as e:
            logger.error(f"Error sending task progress: {str(e)}")

    async def send_task_error(self, event: dict):
        """Send task error updates to client"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'task_error',
                **event
            }))
        except Exception as e:
            logger.error(f"Error sending task error: {str(e)}")

    async def send_table_status_update(self, event: dict):
        """Send table status updates to client for UI real-time updates"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'table_status_update',
                'table_id': event.get('table_id'),
                'table_name': event.get('table_name'),
                'process_type': event.get('process_type'),
                'status': event.get('status'),
                'message': event.get('message'),
                'error_details': event.get('error_details'),
                'timestamp': event.get('timestamp')
            }))
        except Exception as e:
            logger.error(f"Error sending table status update: {str(e)}")

    async def send_bulk_table_status_update(self, event: dict):
        """Send bulk table status updates to client for UI real-time updates"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'bulk_table_status_update',
                'updates': event.get('updates', []),
                'timestamp': event.get('timestamp')
            }))
        except Exception as e:
            logger.error(f"Error sending bulk table status update: {str(e)}")