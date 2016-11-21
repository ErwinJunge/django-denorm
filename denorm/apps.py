from django.apps import AppConfig


class DenormConfig(AppConfig):
    name = 'denorm'

    def ready(self):
        from denorm.denorms import get_alldenorms

        for denorm in get_alldenorms():
            denorm.setup()
