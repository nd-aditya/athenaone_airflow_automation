from django.core.management.base import BaseCommand
from phi_analyzer.models import ModelConfiguration


class Command(BaseCommand):
    help = 'Create sample model configurations for PHI analyzer'

    def handle(self, *args, **options):
        # Create sample configurations
        configs_data = [
            {
                'name': 'Default Configuration',
                'description': 'Default PHI analysis configuration with balanced settings',
                'model_name': 'gpt-4',
                'temperature': 0.1,
                'max_tokens': 1000,
                'sample_size': 100,
                'is_default': True,
                'is_active': True,
                'created_by': 'system'
            },
            {
                'name': 'High Accuracy Config',
                'description': 'Configuration optimized for high accuracy PHI detection',
                'model_name': 'gpt-4',
                'temperature': 0.05,
                'max_tokens': 1500,
                'sample_size': 200,
                'is_default': False,
                'is_active': True,
                'created_by': 'system'
            },
            {
                'name': 'Fast Analysis Config',
                'description': 'Configuration optimized for faster PHI analysis',
                'model_name': 'gpt-3.5-turbo',
                'temperature': 0.2,
                'max_tokens': 800,
                'sample_size': 50,
                'is_default': False,
                'is_active': True,
                'created_by': 'system'
            },
            {
                'name': 'Comprehensive Config',
                'description': 'Configuration for comprehensive PHI analysis with detailed results',
                'model_name': 'gpt-4',
                'temperature': 0.1,
                'max_tokens': 2000,
                'sample_size': 300,
                'is_default': False,
                'is_active': True,
                'created_by': 'system'
            }
        ]

        created_count = 0
        for config_data in configs_data:
            config, created = ModelConfiguration.objects.get_or_create(
                name=config_data['name'],
                defaults=config_data
            )
            if created:
                created_count += 1
                self.stdout.write(
                    self.style.SUCCESS(f'Created configuration: {config.name}')
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f'Configuration already exists: {config.name}')
                )

        self.stdout.write(
            self.style.SUCCESS(f'Successfully created {created_count} new configurations')
        )
        
        # Display all configurations
        all_configs = ModelConfiguration.objects.filter(is_active=True)
        self.stdout.write(f'\nTotal active configurations: {all_configs.count()}')
        for config in all_configs:
            default_status = " (DEFAULT)" if config.is_default else ""
            self.stdout.write(f'  - {config.name}{default_status} - {config.model_name}')
