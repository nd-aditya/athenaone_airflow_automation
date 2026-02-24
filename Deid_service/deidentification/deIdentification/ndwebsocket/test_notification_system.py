"""
Test script for the complete notification system
Run this after starting the Django server to test both WebSocket and API functionality
"""

import asyncio
import websockets
import json
import requests
import time
from datetime import datetime, timedelta


class NotificationSystemTester:
    def __init__(self, base_url="http://localhost:8000", websocket_url="ws://localhost:8000/ws/tasks/"):
        self.base_url = base_url
        self.websocket_url = websocket_url
        self.session = requests.Session()
        
    def test_api_endpoints(self):
        """Test all notification API endpoints"""
        print("🧪 Testing Notification API Endpoints...")
        
        # Test 1: Get notification types
        print("\n1. Testing notification types endpoint...")
        try:
            response = self.session.get(f"{self.base_url}/types/")
            if response.status_code == 200:
                print("✅ Notification types endpoint working")
                print(f"   Available types: {response.json()}")
            else:
                print(f"❌ Notification types endpoint failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Error testing notification types: {str(e)}")
        
        # Test 2: Get WebSocket info
        print("\n2. Testing WebSocket info endpoint...")
        try:
            response = self.session.get(f"{self.base_url}/websocket-info/")
            if response.status_code == 200:
                print("✅ WebSocket info endpoint working")
                print(f"   WebSocket URL: {response.json().get('websocket_url')}")
            else:
                print(f"❌ WebSocket info endpoint failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Error testing WebSocket info: {str(e)}")
        
        # Test 3: Create a test notification (if authenticated)
        print("\n3. Testing notification creation...")
        try:
            test_notification = {
                "title": "Test Notification",
                "message": "This is a test notification created via API",
                "notification_type": "info",
                "priority": "medium",
                "task_name": "test_task",
                "is_broadcast": True
            }
            response = self.session.post(f"{self.base_url}/notifications/", json=test_notification)
            if response.status_code in [200, 201]:
                print("✅ Notification creation working")
                notification_id = response.json().get('id')
                print(f"   Created notification ID: {notification_id}")
            else:
                print(f"❌ Notification creation failed: {response.status_code}")
                print(f"   Response: {response.text}")
        except Exception as e:
            print(f"❌ Error creating notification: {str(e)}")
        
        # Test 4: List notifications
        print("\n4. Testing notification listing...")
        try:
            response = self.session.get(f"{self.base_url}/notifications/")
            if response.status_code == 200:
                print("✅ Notification listing working")
                notifications = response.json().get('results', [])
                print(f"   Found {len(notifications)} notifications")
            else:
                print(f"❌ Notification listing failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Error listing notifications: {str(e)}")
        
        # Test 5: Get notification stats
        print("\n5. Testing notification stats...")
        try:
            response = self.session.get(f"{self.base_url}/notifications/stats/")
            if response.status_code == 200:
                print("✅ Notification stats working")
                stats = response.json()
                print(f"   Total notifications: {stats.get('total_notifications')}")
                print(f"   Unread count: {stats.get('unread_count')}")
            else:
                print(f"❌ Notification stats failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Error getting stats: {str(e)}")
    
    async def test_websocket_connection(self):
        """Test WebSocket connection and message handling"""
        print("\n🌐 Testing WebSocket Connection...")
        
        try:
            async with websockets.connect(self.websocket_url) as websocket:
                print("✅ Connected to WebSocket")
                
                # Test ping/pong
                print("\n1. Testing ping/pong...")
                await websocket.send(json.dumps({"type": "ping"}))
                response = await websocket.recv()
                response_data = json.loads(response)
                if response_data.get("type") == "pong":
                    print("✅ Ping/pong working")
                else:
                    print(f"❌ Ping/pong failed: {response_data}")
                
                # Test subscription
                print("\n2. Testing subscription...")
                await websocket.send(json.dumps({
                    "type": "subscribe",
                    "task_type": "test_task"
                }))
                response = await websocket.recv()
                response_data = json.loads(response)
                if response_data.get("type") == "subscription":
                    print("✅ Subscription working")
                else:
                    print(f"❌ Subscription failed: {response_data}")
                
                # Test receiving messages
                print("\n3. Testing message reception...")
                print("   Waiting for messages (will timeout after 10 seconds)...")
                
                try:
                    # Wait for messages with timeout
                    message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                    message_data = json.loads(message)
                    print(f"✅ Received message: {message_data.get('type', 'unknown')}")
                    print(f"   Content: {message_data}")
                except asyncio.TimeoutError:
                    print("⏰ No messages received within timeout (this is normal if no notifications are being sent)")
                
                print("\n✅ WebSocket test completed")
                
        except websockets.exceptions.ConnectionRefused:
            print("❌ WebSocket connection refused. Make sure Django server is running with ASGI support")
        except Exception as e:
            print(f"❌ WebSocket error: {str(e)}")
    
    def test_notification_creation_via_utils(self):
        """Test creating notifications using the utils functions"""
        print("\n🔧 Testing Notification Creation via Utils...")
        
        try:
            # Import the utils functions
            import sys
            import os
            sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
            
            from ndwebsocket.utils import (
                broadcast_task_status, 
                broadcast_task_progress, 
                broadcast_task_error,
                save_notification_to_db
            )
            
            # Test 1: Create a simple notification
            print("1. Testing simple notification creation...")
            notification = save_notification_to_db(
                title="Test Notification via Utils",
                message="This notification was created using the utils function",
                task_name="test_utils_task",
                notification_type="info",
                priority="medium"
            )
            if notification:
                print(f"✅ Created notification: {notification.id}")
            else:
                print("❌ Failed to create notification")
            
            # Test 2: Broadcast task status
            print("2. Testing task status broadcast...")
            broadcast_task_status(
                status="started",
                task_name="test_broadcast_task",
                message="Test task has started",
                priority="medium"
            )
            print("✅ Task status broadcast sent")
            
            # Test 3: Broadcast task progress
            print("3. Testing task progress broadcast...")
            broadcast_task_progress(
                task_name="test_broadcast_task",
                progress=50,
                message="Task is 50% complete",
                current_step="processing_data"
            )
            print("✅ Task progress broadcast sent")
            
            # Test 4: Broadcast task error
            print("4. Testing task error broadcast...")
            broadcast_task_error(
                task_name="test_broadcast_task",
                error="Test error occurred",
                error_code="TEST_ERROR",
                details={"retry_count": 1}
            )
            print("✅ Task error broadcast sent")
            
        except ImportError as e:
            print(f"❌ Import error: {str(e)}")
            print("   Make sure you're running this from the Django project directory")
        except Exception as e:
            print(f"❌ Error testing utils: {str(e)}")
    
    def run_all_tests(self):
        """Run all tests"""
        print("🚀 Starting Complete Notification System Test")
        print("=" * 50)
        
        # Test API endpoints
        self.test_api_endpoints()
        
        # Test notification creation via utils
        self.test_notification_creation_via_utils()
        
        # Test WebSocket connection
        print("\n" + "=" * 50)
        asyncio.run(self.test_websocket_connection())
        
        print("\n" + "=" * 50)
        print("🎉 All tests completed!")
        print("\nNext steps:")
        print("1. Check your database for created notifications")
        print("2. Test the frontend integration using the WebSocket URL")
        print("3. Use the API endpoints to build your notification UI")


def main():
    """Main function to run tests"""
    print("Notification System Tester")
    print("Make sure your Django server is running with: python manage.py runserver")
    print("Press Enter to continue or Ctrl+C to cancel...")
    
    try:
        input()
    except KeyboardInterrupt:
        print("\nTest cancelled by user")
        return
    
    tester = NotificationSystemTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()
