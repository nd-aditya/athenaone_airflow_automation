from rest_framework import serializers
from .models import ModelConfiguration


class ModelConfigurationSerializer(serializers.ModelSerializer):
    """Serializer for ModelConfiguration model"""
    
    class Meta:
        model = ModelConfiguration
        fields = [
            'id', 'name', 'description', 'model_name', 'temperature', 
            'max_tokens', 'sample_size', 'is_default', 'is_active', 
            'created_by', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
    
    def validate_temperature(self, value):
        """Validate temperature is between 0 and 1"""
        if not 0 <= value <= 1:
            raise serializers.ValidationError("Temperature must be between 0 and 1")
        return value
    
    def validate_max_tokens(self, value):
        """Validate max_tokens is positive"""
        if value <= 0:
            raise serializers.ValidationError("Max tokens must be positive")
        return value
    
    def validate_sample_size(self, value):
        """Validate sample_size is positive"""
        if value <= 0:
            raise serializers.ValidationError("Sample size must be positive")
        return value
    
    def validate_name(self, value):
        """Validate name is unique"""
        if self.instance and self.instance.name == value:
            return value
        
        if ModelConfiguration.objects.filter(name=value, is_active=True).exists():
            raise serializers.ValidationError("A configuration with this name already exists")
        return value
