from django.db import models
from django.conf import settings

# from django.contrib.auth.models import User
from keycloakauth.models import AuthUser
from django.contrib.auth.models import AnonymousUser

LIST_OF_FRONTEND_COMPONENT = [
    "AddDataBase",
    "UploadPHIConfigCSV" "UpdatePHIConfig",
    "PHIMarkingCompletedTick",
    "PHIMarkingVerifiedTick",
    "TableQCTick" "LockPHIMarkingDB",
    "UnLockPHIMarkingDB",
    "LockConfigForDatabase",
    "StartDeIdentificationButton",
    "StopDeIdentificationButton",
]
DEFAULT_PERMISSIONS = {
    "AddDataBase": {"has_permission": True},
    "UploadPHIConfigCSV": {"has_permission": True},
    "PHIMarkingCompletedTick": {"has_permission": True},
    "PHIMarkingVerifiedTick": {"has_permission": True},
    "TableQCTick": {"has_permission": True},
    "UnLockPHIMarkingDB": {"has_permission": True},
    "LockPHIMarkingDB": {"has_permission": True},
    "LockPHIMarkingTable": {"has_permission": True},
    "UnLockPHIMarkingTable": {"has_permission": True},
    "StartDeIdentificationButton": {"has_permission": True},
}

def get_default_permissions():
    return DEFAULT_PERMISSIONS.copy()


class RoleModel(models.Model):
    id = models.AutoField(primary_key=True)
    permissions = models.JSONField(default=get_default_permissions)
    user = models.OneToOneField(
        AuthUser, on_delete=models.CASCADE, related_name="nd_role"
    )

    class Meta:
        pass
    
    @classmethod
    def get_permissions_for_user(cls, user):
        if isinstance(user, AnonymousUser) and settings.DISABLE_AUTHENTICATION:
            return get_default_permissions()
        else:
            user_role = RoleModel.objects.get(user=user)
            return user_role.get_permissions() 
        

    def __str__(self):
        return f"{self.id}, {self.user}"

    def get_permissions(self):
        return self.permissions

    def has_permission(self, permission):
        return self.permissions.get(permission, {}).get("has_permission", False)
