from django.db import models


class AuthUser(models.Model):
    id = models.AutoField(primary_key=True)
    email = models.EmailField(unique=True)
    session_id = models.CharField(max_length=200, default=None, null=True)
    is_authenticated = models.BooleanField(default=False)
    access_token  = models.TextField(default=None, null=True)
    refresh_token  = models.TextField(default=None, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
