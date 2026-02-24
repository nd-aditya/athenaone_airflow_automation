"""
WebSocket consumers for PHI analysis real-time updates
"""

import json
import logging
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from django.contrib.auth.models import AnonymousUser
from django.conf import settings

logger = logging.getLogger(__name__)


class PHIAnalysisConsumer(AsyncWebsocketConsumer):
    """WebSocket consumer for PHI analysis real-time updates"""
    
    async def connect(self):
        """Handle WebSocket connection"""
        try:
            # Get user from scope (if authentication is enabled)
            self.user = self.scope.get("user", AnonymousUser())
            
            # Accept the WebSocket connection first
            await self.accept()
            
            # Add to PHI analysis group
            await self.channel_layer.group_add("phi_analysis_group", self.channel_name)
            
            # Send connection confirmation
            await self.send(text_data=json.dumps({
                "type": "connection",
                "status": "connected",
                "message": "Successfully connected to PHI analysis updates"
            }))
            
            logger.info(f"PHI Analysis WebSocket connected: {self.channel_name}")
            
        except Exception as e:
            logger.error(f"PHI Analysis WebSocket connection error: {str(e)}")
            await self.close(code=4000)

    async def disconnect(self, close_code):
        """Handle WebSocket disconnection"""
        try:
            await self.channel_layer.group_discard("phi_analysis_group", self.channel_name)
            logger.info(f"PHI Analysis WebSocket disconnected: {self.channel_name}, code: {close_code}")
        except Exception as e:
            logger.error(f"PHI Analysis WebSocket disconnection error: {str(e)}")

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
            elif message_type == 'subscribe_session':
                # Handle subscription to specific analysis session
                session_id = data.get('session_id')
                if session_id:
                    try:
                        await self.channel_layer.group_add(f"phi_session_{session_id}", self.channel_name)
                        await self.send(text_data=json.dumps({
                            "type": "subscription",
                            "status": "subscribed",
                            "session_id": session_id
                        }))
                    except Exception as e:
                        logger.error(f"Error subscribing to session {session_id}: {str(e)}")
                        await self.send(text_data=json.dumps({
                            "type": "error",
                            "message": f"Failed to subscribe to session {session_id}"
                        }))
            elif message_type == 'unsubscribe_session':
                # Handle unsubscription from specific analysis session
                session_id = data.get('session_id')
                if session_id:
                    try:
                        await self.channel_layer.group_discard(f"phi_session_{session_id}", self.channel_name)
                        await self.send(text_data=json.dumps({
                            "type": "unsubscription",
                            "status": "unsubscribed",
                            "session_id": session_id
                        }))
                    except Exception as e:
                        logger.error(f"Error unsubscribing from session {session_id}: {str(e)}")
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
            logger.error(f"PHI Analysis WebSocket receive error: {str(e)}")
            await self.send(text_data=json.dumps({
                "type": "error",
                "message": "Internal server error"
            }))

    async def send_analysis_status(self, event: dict):
        """Send analysis status updates to client"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'analysis_status',
                **event
            }))
        except Exception as e:
            logger.error(f"Error sending analysis status: {str(e)}")

    async def send_analysis_progress(self, event: dict):
        """Send analysis progress updates to client"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'analysis_progress',
                **event
            }))
        except Exception as e:
            logger.error(f"Error sending analysis progress: {str(e)}")

    async def send_analysis_error(self, event: dict):
        """Send analysis error updates to client"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'analysis_error',
                **event
            }))
        except Exception as e:
            logger.error(f"Error sending analysis error: {str(e)}")

    async def send_table_status_update(self, event: dict):
        """Send table status updates to client for UI real-time updates"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'table_status_update',
                'table_id': event.get('table_id'),
                'table_name': event.get('table_name'),
                'session_id': event.get('session_id'),
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
                'session_id': event.get('session_id'),
                'updates': event.get('updates', []),
                'timestamp': event.get('timestamp')
            }))
        except Exception as e:
            logger.error(f"Error sending bulk table status update: {str(e)}")

    async def send_session_update(self, event: dict):
        """Send session-specific updates to client"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'session_update',
                'session_id': event.get('session_id'),
                **event
            }))
        except Exception as e:
            logger.error(f"Error sending session update: {str(e)}")

    async def send_statistics_update(self, event: dict):
        """Send statistics updates to client"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'statistics_update',
                'session_id': event.get('session_id'),
                'statistics': event.get('statistics', {}),
                'timestamp': event.get('timestamp')
            }))
        except Exception as e:
            logger.error(f"Error sending statistics update: {str(e)}")
