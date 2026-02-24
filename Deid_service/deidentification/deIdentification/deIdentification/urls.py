"""
URL configuration for deIdentification project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import path, include
from nd_api.urls import urlpatterns as nd_api_urls
from nd_api_v2.urls import urlpatterns as nd_api_v2_urls
from keycloakauth.urls import urlpatterns as auth_urlpatterns
from neuropacs.urls import urlpatterns as pacs_urlpatterns
from ndwebsocket.urls import urlpatterns as notification_urls
from phi_analyzer.urls import urlpatterns as phi_analyzer_urls

urlpatterns = []

urlpatterns += auth_urlpatterns
# urlpatterns += nd_api_urls
urlpatterns += nd_api_v2_urls
# urlpatterns += pacs_urlpatterns
# urlpatterns += notification_urls
# urlpatterns += phi_analyzer_urls
