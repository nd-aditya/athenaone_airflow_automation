"""
Simple test script to verify WebSocket functionality
Run this after starting the Django server to test WebSocket connection
"""

import asyncio
import websockets
import json


async def test_websocket():
    """Test WebSocket connection and message handling"""
    uri = "ws://localhost:8000/ws/tasks/"
    
    try:
        async with websockets.connect(uri) as websocket:
            print("✅ Connected to WebSocket")
            
            # Test ping/pong
            await websocket.send(json.dumps({"type": "ping"}))
            response = await websocket.recv()
            print(f"✅ Ping response: {response}")
            
            # Test subscription
            await websocket.send(json.dumps({
                "type": "subscribe",
                "task_type": "deidentification"
            }))
            response = await websocket.recv()
            print(f"✅ Subscription response: {response}")
            
            # Wait for any incoming messages
            print("⏳ Waiting for messages... (Press Ctrl+C to stop)")
            try:
                while True:
                    message = await websocket.recv()
                    print(f"📨 Received: {message}")
            except KeyboardInterrupt:
                print("\n🛑 Test stopped by user")
                
    except websockets.exceptions.ConnectionRefused:
        print("❌ Connection refused. Make sure Django server is running with ASGI support")
    except Exception as e:
        print(f"❌ Error: {str(e)}")


if __name__ == "__main__":
    print("🧪 Testing WebSocket connection...")
    asyncio.run(test_websocket())
