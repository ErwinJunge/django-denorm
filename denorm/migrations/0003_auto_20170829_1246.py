# -*- coding: utf-8 -*-
# Generated by Django 1.10.7 on 2017-08-29 12:46
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('denorm', '0002_auto_20161121_0943'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='dirtyinstance',
            unique_together=set([]),
        ),
    ]