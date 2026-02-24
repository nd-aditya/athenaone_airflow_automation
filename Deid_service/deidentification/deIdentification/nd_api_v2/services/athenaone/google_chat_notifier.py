#!/usr/bin/env python3
"""
Google Chat Webhook Notifier
Sends formatted notifications to Google Chat with machine identification
"""

import requests
import json
from datetime import datetime
from typing import Optional


class GoogleChatNotifier:
    """Send notifications to Google Chat via webhook"""
    
    def __init__(self, webhook_url: str, enabled: bool = True, machine_name: str = "Unknown"):
        self.webhook_url = webhook_url
        self.enabled = enabled
        self.machine_name = machine_name
    
    def _send_message(self, message: dict) -> bool:
        """Send message to Google Chat"""
        if not self.enabled:
            return False
        
        if not self.webhook_url or self.webhook_url == "":
            print("⚠️ Google Chat webhook URL not configured")
            return False
        
        try:
            response = requests.post(
                self.webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Failed to send Google Chat notification: {e}")
            return False
    
    def send_pipeline_start(self, schema: str = None):
        """Notify pipeline start"""
        message = {
            "cards": [{
                "header": {
                    "title": f"🚀 Pipeline Started - {self.machine_name}",
                    "subtitle": f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "imageUrl": "https://img.icons8.com/color/96/rocket.png"
                },
                "sections": [{
                    "widgets": [
                        {
                            "keyValue": {
                                "topLabel": "Machine",
                                "content": self.machine_name,
                                "contentMultiline": False,
                                "icon": "STAR"
                            }
                        },
                        {
                            "keyValue": {
                                "topLabel": "Status",
                                "content": "Pipeline execution started",
                                "contentMultiline": False,
                                "icon": "CLOCK"
                            }
                        }
                    ]
                }]
            }]
        }
        
        if schema:
            message["cards"][0]["sections"][0]["widgets"].append({
                "keyValue": {
                    "topLabel": "Target Schema",
                    "content": schema,
                    "contentMultiline": False,
                    "icon": "DESCRIPTION"
                }
            })
        
        return self._send_message(message)
    
    def send_step_progress(self, step_num: int, total_steps: int, step_name: str, status: str = "running"):
        """Notify step progress"""
        emoji = "⏳" if status == "running" else "✅" if status == "completed" else "❌"
        
        message = {
            "cards": [{
                "header": {
                    "title": f"{emoji} Step {step_num}/{total_steps}: {step_name}",
                    "subtitle": f"{self.machine_name} | Status: {status.upper()}",
                },
                "sections": [{
                    "widgets": [
                        {
                            "keyValue": {
                                "topLabel": "Machine",
                                "content": self.machine_name,
                                "contentMultiline": False,
                                "icon": "STAR"
                            }
                        },
                        {
                            "textParagraph": {
                                "text": f"<b>Progress:</b> {step_num}/{total_steps} steps completed ({int(step_num/total_steps*100)}%)"
                            }
                        },
                        {
                            "keyValue": {
                                "topLabel": "Current Step",
                                "content": step_name,
                                "contentMultiline": False,
                                "icon": "CLOCK"
                            }
                        }
                    ]
                }]
            }]
        }
        
        return self._send_message(message)
    
    def send_pipeline_success(self, duration: str = None, stats: dict = None):
        """Notify pipeline success"""
        message = {
            "cards": [{
                "header": {
                    "title": f"✅ Pipeline Completed - {self.machine_name}",
                    "subtitle": f"Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "imageUrl": "https://img.icons8.com/color/96/checkmark.png"
                },
                "sections": [{
                    "widgets": [
                        {
                            "keyValue": {
                                "topLabel": "Machine",
                                "content": self.machine_name,
                                "contentMultiline": False,
                                "icon": "STAR"
                            }
                        },
                        {
                            "keyValue": {
                                "topLabel": "Status",
                                "content": "All steps completed successfully",
                                "contentMultiline": False,
                                "icon": "CONFIRMATION_NUMBER_ICON"
                            }
                        }
                    ]
                }]
            }]
        }
        
        if duration:
            message["cards"][0]["sections"][0]["widgets"].append({
                "keyValue": {
                    "topLabel": "Duration",
                    "content": duration,
                    "contentMultiline": False,
                    "icon": "CLOCK"
                }
            })
        
        if stats:
            for key, value in stats.items():
                message["cards"][0]["sections"][0]["widgets"].append({
                    "keyValue": {
                        "topLabel": key,
                        "content": str(value),
                        "contentMultiline": False
                    }
                })
        
        return self._send_message(message)
    
    def send_pipeline_failure(self, step_num: int, step_name: str, error: str):
        """Notify pipeline failure"""
        message = {
            "cards": [{
                "header": {
                    "title": f"❌ Pipeline Failed - {self.machine_name}",
                    "subtitle": f"Failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "imageUrl": "https://img.icons8.com/color/96/error.png"
                },
                "sections": [{
                    "widgets": [
                        {
                            "keyValue": {
                                "topLabel": "Machine",
                                "content": self.machine_name,
                                "contentMultiline": False,
                                "icon": "STAR"
                            }
                        },
                        {
                            "keyValue": {
                                "topLabel": "Failed Step",
                                "content": f"Step {step_num}: {step_name}",
                                "contentMultiline": False,
                                "icon": "TICKET"
                            }
                        },
                        {
                            "textParagraph": {
                                "text": f"<b>Error:</b><br><font color=\"#d93025\">{error[:500]}</font>"
                            }
                        },
                        {
                            "buttons": [
                                {
                                    "textButton": {
                                        "text": "View Dashboard",
                                        "onClick": {
                                            "openLink": {
                                                "url": "http://localhost:5001"
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    ]
                }]
            }]
        }
        
        return self._send_message(message)


def get_notifier():
    """Get configured notifier instance"""
    try:
        from nd_api_v2.services.incrementalflow.config_loader import MACHINE_NAME, GOOGLE_CHAT_WEBHOOK, ENABLE_CHAT_NOTIFICATIONS
        
        return GoogleChatNotifier(
            webhook_url=GOOGLE_CHAT_WEBHOOK,
            enabled=ENABLE_CHAT_NOTIFICATIONS,
            machine_name=MACHINE_NAME
        )
    except (ImportError, NameError) as e:
        print(f"⚠️ Could not import config_loader for Google Chat notifications: {e}")
        return GoogleChatNotifier('', False, 'Unknown Machine')
