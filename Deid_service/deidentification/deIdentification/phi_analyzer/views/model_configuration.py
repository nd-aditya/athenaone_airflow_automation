from rest_framework import generics, status
from rest_framework.response import Response
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404
from ..models import ModelConfiguration
from ..serializers import ModelConfigurationSerializer
from nd_api.decorator import conditional_authentication

@conditional_authentication
class ModelConfigurationListCreateView(generics.ListCreateAPIView):
    """List all model configurations or create a new one"""
    queryset = ModelConfiguration.objects.filter(is_active=True)
    serializer_class = ModelConfigurationSerializer
    authentication_classes = []  # Disabled authentication
    
    def get_queryset(self):
        return ModelConfiguration.objects.filter(is_active=True).order_by('-is_default', 'name')

@conditional_authentication
class ModelConfigurationRetrieveUpdateDestroyView(generics.RetrieveUpdateDestroyAPIView):
    """Retrieve, update or delete a model configuration"""
    queryset = ModelConfiguration.objects.all()
    serializer_class = ModelConfigurationSerializer
    authentication_classes = []  # Disabled authentication
    lookup_field = 'id'

@conditional_authentication
class GetDefaultConfigurationView(APIView):
    """Get the default model configuration"""
    authentication_classes = []  # Disabled authentication
    
    def get(self, request):
        try:
            config = ModelConfiguration.get_default_config()
            serializer = ModelConfigurationSerializer(config)
            return Response(serializer.data)
        except Exception as e:
            return Response(
                {'error': f'Failed to get default configuration: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

@conditional_authentication
class SetDefaultConfigurationView(APIView):
    """Set a configuration as default"""
    authentication_classes = []  # Disabled authentication
    
    def post(self, request, config_id):
        try:
            config = get_object_or_404(ModelConfiguration, id=config_id, is_active=True)
            
            # Remove default from all other configurations
            ModelConfiguration.objects.filter(is_default=True).update(is_default=False)
            
            # Set this configuration as default
            config.is_default = True
            config.save()
            
            serializer = ModelConfigurationSerializer(config)
            return Response(serializer.data)
        except Exception as e:
            return Response(
                {'error': f'Failed to set default configuration: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

@conditional_authentication
class DuplicateConfigurationView(APIView):
    """Duplicate an existing configuration"""
    authentication_classes = []  # Disabled authentication
    
    def post(self, request, config_id):
        try:
            original_config = get_object_or_404(ModelConfiguration, id=config_id)
            
            # Create a new configuration based on the original
            new_config = ModelConfiguration.objects.create(
                name=f"{original_config.name} (Copy)",
                description=original_config.description,
                model_name=original_config.model_name,
                temperature=original_config.temperature,
                max_tokens=original_config.max_tokens,
                sample_size=original_config.sample_size,
                is_default=False,  # New configs are not default
                is_active=True,
                created_by=request.user.username if hasattr(request, "user") and getattr(request.user, "is_authenticated", False) else 'admin'
            )
            
            serializer = ModelConfigurationSerializer(new_config)
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response(
                {'error': f'Failed to duplicate configuration: {str(e)}'}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
