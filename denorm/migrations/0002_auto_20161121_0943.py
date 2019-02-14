# -*- coding: utf-8 -*-
# Generated by Django 1.10.3 on 2016-11-21 09:43
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('contenttypes', '0002_remove_content_type_name'),
        ('denorm', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dirtyinstance',
            name='object_id',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AlterUniqueTogether(
            name='dirtyinstance',
            unique_together=set([('content_type', 'object_id')]),
        ),
    ]