from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from keycloakauth.keycloakapi import NDKeyCloakAPI, UserDetails

KeycloakAuthApi = NDKeyCloakAPI()


class UserSignIn(APIView):
    def post(self, request):
        try:
            data = request.data
            user_details = UserDetails(email=data["email"], password=data["password"])
            if "username" in data:
                user_details["username"] = data["username"]
            response_json = KeycloakAuthApi.login(user_details)
            return Response(response_json, status=response_json["status_code"])
        except KeyError as e:
            messgae = f"required field not provided, {e}"
            return Response(messgae, status=status.HTTP_400_BAD_REQUEST)


class UserLogout(APIView):
    def post(self, request):
        try:
            data = request.data
            response_json = KeycloakAuthApi.logout(data["email"])
            return Response(response_json, status=response_json["status_code"])
        except KeyError as e:
            messgae = f"required field not provided, {e}"
            return Response(messgae, status=status.HTTP_400_BAD_REQUEST)


class IsUserAuthenticated(APIView):
    def post(self, request):
        data = request.data
        try:
            response_json = KeycloakAuthApi.is_authenticated(
                access_token=data["refresh_token"]
            )
            return Response(response_json, status=response_json["status_code"])
        except KeyError:
            return Response(
                {"success": False, "message": "access token is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            return Response(
                {"success": False, "message": f"Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

class RefreshToken(APIView):
    def post(self, request):
        data = request.data
        try:
            response_json = KeycloakAuthApi.refresh_token(
                session_id=data["session_id"]
            )
            return Response(response_json, status=response_json["status_code"])
        except KeyError:
            return Response(
                {"success": False, "message": "session_id is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            return Response(
                {"success": False, "message": f"Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
