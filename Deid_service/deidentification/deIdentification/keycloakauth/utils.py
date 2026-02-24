from rest_framework.permissions import BasePermission
from rest_framework.authentication import SessionAuthentication
from rest_framework.exceptions import AuthenticationFailed, NotAuthenticated
from keycloakauth.keycloakapi import NDKeyCloakAPI

from .models import AuthUser
from deIdentification.nd_logger import nd_logger


def get_access_token(request):
    auth_header = request.headers.get('Authorization')
    
    if auth_header:
        if auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
            return token
        else:
            return None
    return None

class IsAuthenticated(SessionAuthentication):
    def authenticate(self, request):
        try:
            access_token = get_access_token(request)
            if not access_token:
                raise NotAuthenticated(detail="No access token provided.")
            nd_keycloak = NDKeyCloakAPI()
            response = nd_keycloak.is_authenticated(access_token)
            if response["is_authenticated"]:
                auth_user = AuthUser.objects.get(access_token=access_token)
                return (auth_user, access_token)
            else:
                raise NotAuthenticated(detail="User is not authenticated.")
        except Exception as e:
            nd_logger.error(f"Exception during authentcation check: {e}")
            raise NotAuthenticated(detail="User is not authenticated.")

    def authenticate_header(self, request):
        """
        Return the authentication header for bearer token authentication.
        This method is called by DRF to get the WWW-Authenticate header value.
        """
        return 'Bearer realm="api"'
    
    def enforce_csrf(self, request):
        """
        Override to disable CSRF checks for token-based authentication.
        """
        return