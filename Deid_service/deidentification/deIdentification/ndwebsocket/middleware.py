import logging
from django.contrib.auth.models import AnonymousUser
from django.contrib.auth import get_user_model
from django.db import close_old_connections
from channels.middleware import BaseMiddleware
from channels.db import database_sync_to_async
from django.conf import settings

logger = logging.getLogger(__name__)
User = get_user_model()


class WebSocketAuthMiddleware(BaseMiddleware):
    """
    Custom WebSocket authentication middleware
    """
    
    def __init__(self, inner):
        super().__init__(inner)
    
    async def __call__(self, scope, receive, send):
        # Close old database connections
        close_old_connections()
        
        # Get user from scope (set by Django's AuthMiddlewareStack)
        user = scope.get("user", AnonymousUser())
        
        # If authentication is disabled, allow all connections
        if getattr(settings, 'DISABLE_AUTHENTICATION', False):
            logger.info("WebSocket authentication disabled - allowing all connections")
            scope["user"] = user
            return await self.inner(scope, receive, send)
        
        # Check if user is authenticated
        if user.is_authenticated:
            logger.info(f"WebSocket authenticated user: {user.username}")
            scope["user"] = user
        else:
            logger.warning("WebSocket connection from unauthenticated user")
            # For now, allow unauthenticated connections but log them
            # You can change this to reject connections if needed
            scope["user"] = AnonymousUser()
        
        return await self.inner(scope, receive, send)


@database_sync_to_async
def get_user_from_token(token):
    """
    Get user from token (if using token-based authentication)
    This is a placeholder - implement based on your auth system
    """
    try:
        # Implement your token validation logic here
        # For example, if using JWT tokens:
        # from jwt import decode
        # payload = decode(token, settings.SECRET_KEY, algorithms=['HS256'])
        # user_id = payload.get('user_id')
        # return User.objects.get(id=user_id)
        return AnonymousUser()
    except Exception as e:
        logger.error(f"Token validation error: {str(e)}")
        return AnonymousUser()


class WebSocketTokenAuthMiddleware(BaseMiddleware):
    """
    Alternative WebSocket authentication middleware using token-based auth
    Use this if you want to authenticate via tokens instead of session-based auth
    """
    
    def __init__(self, inner):
        super().__init__(inner)
    
    async def __call__(self, scope, receive, send):
        # Close old database connections
        close_old_connections()
        
        # Extract token from query parameters or headers
        query_string = scope.get("query_string", b"").decode()
        token = None
        
        # Parse query string for token
        if query_string:
            params = dict(param.split('=') for param in query_string.split('&') if '=' in param)
            token = params.get('token')
        
        # If no token and authentication is disabled, allow connection
        if not token and getattr(settings, 'DISABLE_AUTHENTICATION', False):
            scope["user"] = AnonymousUser()
            return await self.inner(scope, receive, send)
        
        # Validate token and get user
        if token:
            user = await get_user_from_token(token)
            if user.is_authenticated:
                logger.info(f"WebSocket token-authenticated user: {user.username}")
                scope["user"] = user
            else:
                logger.warning("WebSocket connection with invalid token")
                scope["user"] = AnonymousUser()
        else:
            logger.warning("WebSocket connection without token")
            scope["user"] = AnonymousUser()
        
        return await self.inner(scope, receive, send)