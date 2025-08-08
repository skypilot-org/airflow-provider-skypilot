"""SkyPilot provider package."""

__version__ = '0.1.0'


def get_provider_info():
    return {
        'package-name': 'airflow-provider-skypilot',
        'name': 'SkyPilot',
        'description': 'SkyPilot https://docs.skypilot.co/',
        'versions': [__version__],
    }
