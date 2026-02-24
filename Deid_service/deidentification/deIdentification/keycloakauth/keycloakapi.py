import os
from typing import TypedDict
from keycloak import KeycloakOpenID, KeycloakAdmin
from keycloak.exceptions import (
    KeycloakAuthenticationError,
    KeycloakError,
    KeycloakPostError,
)
from django.conf import settings
from keycloakauth.models import AuthUser
from deIdentification.nd_logger import nd_logger
from rest_framework import status


class UserDetails(TypedDict, total=False):
    email: str
    first_name: str
    last_name: str
    password: str
    username: str


class NDKeyCloakAPI:
    def __init__(self):
        self.keycloak_openid = KeycloakOpenID(
            server_url=settings.KEYCLOAK_URI,
            client_id=settings.KEYCLOAK_CLIENT_ID,
            realm_name=settings.KEYCLOAK_REALM_NAME,
            client_secret_key=settings.KEYCLOAK_CLIENT_SECRET,
        )
        self.keycloak_admin = KeycloakAdmin(
            server_url=settings.KEYCLOAK_URI,
            username=settings.KEYCLOAK_ADMIN_USERNAME,
            password=settings.KEYCLOAK_ADMIN_PASSWORD,
            realm_name=settings.KEYCLOAK_REALM_NAME,
            client_id=settings.KEYCLOAK_CLIENT_ID,
            client_secret_key=settings.KEYCLOAK_CLIENT_SECRET,
            verify=False,
        )

    def is_authenticated(self, access_token: str):
        try:
            self.keycloak_openid.userinfo(access_token)
            return {
                "success": True,
                "is_authenticated": True,
                "status_code": status.HTTP_200_OK,
            }
        except KeycloakAuthenticationError:
            return {
                "success": True,
                "is_authenticated": False,
                "status_code": status.HTTP_200_OK,
            }
        except Exception as e:
            return {
                "success": False,
                "is_authenticated": False,
                "message": f"{e}",
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            }

    def logout(self, email: str):
        try:
            auth_user = AuthUser.objects.get(email=email)
        except AuthUser.DoesNotExist:
            return {
                "success": False,
                "message": f"Uesr not found user: {email}",
                "status_code": status.HTTP_400_BAD_REQUEST,
            }
        try:
            refresh_token = auth_user.refresh_token
            self.keycloak_openid.logout(refresh_token)
            auth_user.access_token = None
            auth_user.refresh_token = None
            auth_user.session_id = None
            auth_user.is_authenticated = False
            auth_user.save()
            return {
                "success": True,
                "message": f"logout user: {email}",
                "status_code": status.HTTP_200_OK,
            }
        except KeycloakPostError:
            return {
                "success": False,
                "message": f"Invalid refresh token for user: {email}",
                "status_code": status.HTTP_400_BAD_REQUEST,
            }
        except:
            return {
                "success": False,
                "message": f"facing issue in logout for user: {email}",
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            }

    def register_user(self, user_details: UserDetails):
        try:
            user = AuthUser.objects.get(email=user_details["email"])
            return {
                "success": False,
                "message": f"already exists user with email: {user_details['email']}",
                "status_code": status.HTTP_400_BAD_REQUEST,
            }
        except AuthUser.DoesNotExist:
            new_user_data = {
                "email": user_details["email"],
                "username": user_details.get("username", user_details["email"]),
                "enabled": True,
                "credentials": [
                    {
                        "type": "password",
                        "value": user_details["password"],
                        "temporary": False,
                    }
                ],
                "emailVerified": True,
                "firstName": user_details.get("first_name", "DefaultFirstName"),
                "lastName": user_details.get("last_name", "DefaultLastName"),
            }
            self.keycloak_admin.create_user(new_user_data, exist_ok=False)
            nd_logger.info(f"Created new user in Keycloak: {user_details['email']}")

            new_user = AuthUser.objects.create(email=user_details["email"])
            return {
                "success": True,
                "user": new_user,
                "status_code": status.HTTP_200_OK,
            }

    def login(self, user_details: UserDetails):
        try:
            user = AuthUser.objects.get(email=user_details["email"])
        except AuthUser.DoesNotExist as e:
            return {
                "success": False,
                "message": f"Signin attempt for non-existent user: {user_details['email']}",
                "status_code": status.HTTP_400_BAD_REQUEST,
            }
        try:
            keycloak_response = self.keycloak_openid.token(
                username=user_details["email"],
                password=user_details["password"],
                grant_type="password",
            )
        except KeycloakAuthenticationError as e:
            error_msg = f"Keycloak authentication error during signin for {user_details['email']}: {str(e)}"
            nd_logger.error(error_msg)
            return {
                "success": False,
                "message": error_msg,
                "status_code": status.HTTP_401_UNAUTHORIZED,
            }
        except KeycloakError as e:
            error_msg = (
                f"Keycloak error during signin for {user_details['email']}: {str(e)}"
            )
            nd_logger.error(error_msg)
            return {
                "success": False,
                "message": error_msg,
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            }

        if not keycloak_response:
            msg = f"No valid response from the keycloak {user_details['email']}"
            nd_logger.warning(
                f"No valid response from the keycloak {user_details['email']}"
            )
            return {
                "success": False,
                "message": msg,
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            }
        user.access_token = keycloak_response["access_token"]
        user.refresh_token = keycloak_response["refresh_token"]
        user.session_id = keycloak_response["session_state"]
        user.is_authenticated = True
        user.save()
        return {
            "success": True,
            "access_token": keycloak_response["access_token"],
            "refresh_token": keycloak_response["refresh_token"],
            "session_id": keycloak_response["session_state"],
            "status_code": status.HTTP_200_OK,
        }

    def refresh_token(self, session_id: str):
        try:
            auth_user = AuthUser.objects.get(session_id=session_id)
        except AuthUser.DoesNotExist:
            return {"success": False, "message": f"Token refresh attempt with invalid session: {session_id}", "status_code": status.HTTP_401_UNAUTHORIZED}
        
        try:
            token_response = self.keycloak_openid.refresh_token(auth_user.refresh_token)
            auth_user.access_token = token_response['access_token']
            auth_user.refresh_token = token_response['refresh_token']
            auth_user.save()
            
            nd_logger.info(f"Token refreshed for session_id: {session_id}")  
            return {"success": True, "access_token": token_response['access_token'], "session_id": token_response['session_state'], "status_code": status.HTTP_200_OK}
        except KeycloakError as e:
            error_msg = f"Keycloak error during token refresh for session {session_id}: {str(e)}"
            nd_logger.error(error_msg)
            raise {"success": False, "message": error_msg, "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR}
    

    def reset_password(self, email: str, new_password: str):
        try:
            auth_user = AuthUser.objects.get(email=email)
        except AuthUser.DoesNotExist:
            return {"success": True, "message": f"User with email : {email} not exists", "status": status.HTTP_400_BAD_REQUEST}
        try:
            response = self.keycloak_admin.set_user_password(user_id=email, password=new_password, temporary=False)
            nd_logger.info(f"Password reset for {email}")
        except KeycloakError as e:
            error_msg = f"Keycloak error during forgot-password for {email}: {str(e)}"
            nd_logger.error(error_msg)
            return {"success": True, "message": error_msg, "status": status.HTTP_500_INTERNAL_SERVER_ERROR}
            