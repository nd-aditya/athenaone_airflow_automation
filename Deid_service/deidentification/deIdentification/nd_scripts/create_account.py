from keycloakauth.keycloakapi import NDKeyCloakAPI, UserDetails
from keycloakauth.models import AuthUser
from keycloakauth.rolemodel import RoleModel, get_default_permissions


def create_account(user_details: UserDetails, permissions: dict = {}):
    api = NDKeyCloakAPI()
    creation_resposne = api.register_user(user_details)
    if creation_resposne["success"]:
        user = creation_resposne["user"]
        role_model, created = RoleModel.objects.get_or_create(user=user)
        base_permissions = get_default_permissions()
        base_permissions.update(permissions)
        role_model.permissions = base_permissions
        role_model.save()
    return creation_resposne
