"""
ASGI config for deIdentification project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.1/howto/deployment/asgi/
"""
import os
from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")

django_asgi_app = get_asgi_application()


def get_application():
    """Get ASGI application with proper imports after Django is ready"""
    from channels.routing import ProtocolTypeRouter, URLRouter
    from channels.auth import AuthMiddlewareStack
    from ndwebsocket.urls import websocket_urlpatterns
    from ndwebsocket.middleware import WebSocketAuthMiddleware
    
    return ProtocolTypeRouter(
        {
            "http": django_asgi_app,
            "websocket": WebSocketAuthMiddleware(
                AuthMiddlewareStack(
                    URLRouter(
                        websocket_urlpatterns
                    )
                )
            ),
        }
    )


application = get_application()