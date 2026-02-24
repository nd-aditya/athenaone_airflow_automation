from django.db import models
from django.utils import timezone


class ModelConfiguration(models.Model):
    """Model to store global model configurations for PHI analysis"""
    
    name = models.CharField(max_length=100, unique=True, help_text="Configuration name")
    description = models.TextField(blank=True, help_text="Configuration description")
    
    # Model parameters
    model_name = models.CharField(max_length=100, default="gpt-4", help_text="LLM model name")
    temperature = models.FloatField(default=0.1, help_text="Temperature for model output (0-1)")
    max_tokens = models.IntegerField(default=1000, help_text="Maximum tokens for model response")
    sample_size = models.IntegerField(default=100, help_text="Sample size for analysis")
    
    # Additional configuration
    is_default = models.BooleanField(default=False, help_text="Whether this is the default configuration")
    is_active = models.BooleanField(default=True, help_text="Whether this configuration is active")
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.CharField(max_length=100, blank=True, help_text="User who created this configuration")
    
    class Meta:
        ordering = ['-is_default', 'name']
        indexes = [
            models.Index(fields=['is_active', 'is_default']),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.model_name})"
    
    def save(self, *args, **kwargs):
        # Ensure only one default configuration exists
        if self.is_default:
            ModelConfiguration.objects.filter(is_default=True).update(is_default=False)
        super().save(*args, **kwargs)
    
    @classmethod
    def get_default_config(cls):
        """Get the default configuration or create one if none exists"""
        try:
            return cls.objects.get(is_default=True, is_active=True)
        except cls.DoesNotExist:
            # Create a default configuration if none exists
            return cls.objects.create(
                name="Default Configuration",
                description="Default PHI analysis configuration",
                model_name="gpt-4",
                temperature=0.1,
                max_tokens=1000,
                sample_size=100,
                is_default=True,
                is_active=True,
                created_by="system"
            )
