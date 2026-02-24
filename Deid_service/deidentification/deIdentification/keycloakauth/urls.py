from django.urls import path
from keycloakauth.views import UserSignIn, UserLogout, IsUserAuthenticated, RefreshToken

urlpatterns = [
    path("auth/login/", UserSignIn.as_view(), name="login"),
    path("auth/logout/", UserLogout.as_view(), name="logout"),
    path("auth/is_authenticated/", IsUserAuthenticated.as_view(), name="is_authenticated"),
    path("auth/refresh_token/", RefreshToken.as_view(), name="refresh_token"),
]
