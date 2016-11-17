from django.db import models, connections, connection, transaction
from ..helpers import remote_field_model
from django.contrib.contenttypes.generic import GenericRelation


class RandomBigInt(object):
    def sql(self):
        raise NotImplementedError


class TriggerNestedSelect:
    def __init__(self, table, columns, **kwargs):
        self.table = table
        self.columns = ", ".join(columns)
        self.kwargs = kwargs

    def sql(self):
        raise NotImplementedError


class TriggerAction(object):
    def __init__(self):
        pass

    def sql(self):
        pass


class TriggerActionInsert(TriggerAction):
    def __init__(self, model, columns, values):
        self.model = model
        self.columns = columns
        self.values = values

    def sql(self):
        raise NotImplementedError


class TriggerActionUpdate(TriggerAction):
    def __init__(self, model, columns, values, where):
        self.model = model
        self.columns = columns
        self.where = where

        self.values = []
        for value in values:
            if hasattr(value, 'sql'):
                self.values.append(value.sql())
            else:
                self.values.append(value)

    def sql(self):
        raise NotImplementedError


def get_fields_with_model(model, meta):
    try:
        return [
            (f, f.model if f.model != model else None)
            for f in meta.get_fields()
            if not f.is_relation
                or f.one_to_one
                or (f.many_to_one and f.related_model)
        ]
    except AttributeError:
        return meta.get_fields_with_model()


class TriggerCondition(object):
    def sql(self, actions):
        params, action_set = [], set()

        for action in actions:
            sql, action_params = action.sql()

            if sql:
                action_set.add(sql)
                params.extend(action_params)

        return ";\n   ".join(action_set) + ';', params


class TriggerConditionFieldChange(TriggerCondition):
    def __init__(self, field_names):
        if isinstance(field_names, (list, tuple)):
            self.field_names = field_names
        else:
            self.field_names = (field_names,)


class Trigger(object):

    def __init__(self, subject, time, event, actions, content_type, using=None, condition=None):
        self.subject = subject
        self.time = time
        self.event = event
        self.content_type = content_type
        self.content_type_field = None
        self.actions = actions
        self.using = using
        self.condition = condition

        if self.using:
            self.connection = connections[self.using]
        else:
            self.connection = connection

        try:
            from django.contrib.contenttypes.fields import GenericRelation
        except ImportError:
            from django.contrib.contenttypes.generic import GenericRelation

        if isinstance(subject, models.ManyToManyField):
            self.model = None
            self.db_table = subject.m2m_db_table()

        elif isinstance(subject, GenericRelation):
            self.model = None
            self.db_table = remote_field_model(subject)._meta.db_table
            self.fields = [(k.attname, k.db_type(connection=self.connection)) for k, v in get_fields_with_model(remote_field_model(subject), remote_field_model(subject)._meta) if not v]
            self.content_type_field = subject.content_type_field_name + '_id'

        elif isinstance(subject, models.ForeignKey):
            self.model = subject.model
            self.db_table = self.model._meta.db_table

        elif hasattr(subject, "_meta"):
            self.model = subject
            self.db_table = self.model._meta.db_table

        else:
            raise NotImplementedError

    def name(self):
        return "_".join([
            "denorm",
            self.time,
            "row",
            self.event,
            "on",
            self.db_table
        ])

    def sql(self):
        if self.condition:
            return self.condition.sql(self.actions)
        else:
            params, action_set = [], set()

            for action in self.actions:
                sql, action_params = action.sql()

                if sql:
                    action_set.add(sql)
                    params.extend(action_params)

            return ";\n   ".join(action_set) + ';', params


class TriggerSet(object):
    def __init__(self, using=None):
        self.using = using
        self.triggers = {}
        if self.using:
            self.connection = connections[self.using]
        else:
            self.connection = connection

    def cursor(self):
        return self.connection.cursor()

    def append(self, triggers):
        if not isinstance(triggers, list):
            triggers = [triggers]

        for trigger in triggers:
            name = trigger.name()
            if name in self.triggers:
                self.triggers[name].append(trigger)
            else:
                self.triggers[name] = [trigger]

    def install(self):
        cursor = self.cursor()

        for name, triggers in self.triggers.iteritems():
            for i, trigger in enumerate(triggers):
                sql, args = trigger.sql(name + "_%s" % i)
                cursor.execute(sql, args)
                transaction.commit_unless_managed(using=self.using)

    def drop(self):
        raise NotImplementedError
