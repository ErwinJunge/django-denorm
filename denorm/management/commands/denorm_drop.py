from optparse import make_option

from django.core.management.base import NoArgsCommand
from django.db import DEFAULT_DB_ALIAS

from denorm import denorms
from boardinghouse.schema import get_schema_model


class Command(NoArgsCommand):
    option_list = NoArgsCommand.option_list + (
        make_option('--database', action='store', dest='database',
            default=DEFAULT_DB_ALIAS, help='Nominates a database to execute '
                'SQL into. Defaults to the "default" database.'),
    )
    help = "Removes all triggers created by django-denorm."

    def handle_noargs(self, **options):
        using = options['database']
        Schema = get_schema_model()
        for schema in Schema.objects.all():
            schema.activate()
            denorms.drop_triggers(using=using)
            schema.deactivate()
