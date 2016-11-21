from django.db import transaction
from denorm.db import base
from django.db.backends.utils import truncate_name


class RandomBigInt(base.RandomBigInt):
    def sql(self):
        return '(9223372036854775806::INT8 * ((RANDOM()-0.5)*2.0) )::INT8'


class TriggerNestedSelect(base.TriggerNestedSelect):

    def sql(self):
        columns = self.columns
        table = self.table
        where = ", ".join(["%s = %s" % (k, v) for k, v in self.kwargs.items()])
        return 'SELECT DISTINCT %(columns)s FROM %(table)s WHERE %(where)s' % locals(), tuple()


class TriggerActionInsert(base.TriggerActionInsert):

    def sql(self):
        table = self.model._meta.db_table
        columns = "(" + ", ".join(self.columns) + ")"
        params = []
        if isinstance(self.values, TriggerNestedSelect):
            sql, nested_params = self.values.sql()
            values = "(" + sql + ")"
            params.extend(nested_params)
        else:
            values = "VALUES (" + ", ".join(self.values) + ")"

        sql = (
            'BEGIN\n'
            '    INSERT INTO %(table)s %(columns)s %(values)s;\n'
            'EXCEPTION WHEN unique_violation THEN\n'
            '    -- do nothing\n'
            'END'
        ) % locals()
        return sql, params


class TriggerActionUpdate(base.TriggerActionUpdate):

    def sql(self):
        table = self.model._meta.db_table
        params = []
        updates = ", ".join(["%s = %s" % (k, v) for k, v in zip(self.columns, self.values)])
        if isinstance(self.where, tuple):
            where, where_params = self.where
        else:
            where, where_params = self.where, []
        params.extend(where_params)
        return 'UPDATE %(table)s SET %(updates)s WHERE %(where)s' % locals(), params


class TriggerConditionFieldChange(base.TriggerConditionFieldChange):
    def sql(self, actions):
        actions, params = super(TriggerConditionFieldChange, self).sql(actions)
        when = ["(" + "OR".join(["(OLD.%s IS DISTINCT FROM NEW.%s)" % (f, f) for f in self.field_names]) + ")"]
        when = "AND".join(when)

        return """
            BEGIN
                IF %(when)s THEN
                    %(actions)s
                END IF;
                RETURN NULL;
            END;
        """ % locals(), tuple(params)


class Trigger(base.Trigger):
    def name(self):
        name = base.Trigger.name(self)
        if self.content_type_field:
            name += "_%s" % self.content_type
        return name

    def sql(self, name):
        actions, params = super(Trigger, self).sql()

        if not self.condition:
            actions = """
                BEGIN
                    %(actions)s
                    RETURN NULL;
                END;
            """ % locals()

        name = truncate_name(name, self.connection.ops.max_name_length() - 5)
        params = []
        action_list = []
        actions_added = set()
        for a in self.actions:
            sql, action_params = a.sql()
            if sql:
                if not sql.endswith(';'):
                    sql += ';'
                action_params = tuple(action_params)
                if (sql, action_params) not in actions_added:
                    actions_added.add((sql, action_params))
                    action_list.extend(sql.split('\n'))
                    params.extend(action_params)
        table = self.db_table
        time = self.time.upper()
        event = self.event.upper()

        sql = """
            CREATE OR REPLACE FUNCTION func_%(name)s()
                RETURNS TRIGGER AS $$
                    %(actions)s
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER %(name)s
                %(time)s %(event)s ON %(table)s
                FOR EACH ROW EXECUTE PROCEDURE func_%(name)s();
        """ % locals()
        return sql, params


class TriggerSet(base.TriggerSet):
    def drop_atomic(self):
        qn = self.connection.ops.quote_name
        cursor = self.cursor()
        cursor.execute("SELECT pg_class.relname, pg_trigger.tgname FROM pg_trigger LEFT JOIN pg_class ON (pg_trigger.tgrelid = pg_class.oid) WHERE pg_trigger.tgname LIKE 'denorm_%%';")
        for table_name, trigger_name in cursor.fetchall():
            cursor.execute('DROP TRIGGER %s ON %s;' % (qn(trigger_name), qn(table_name)))

    def drop(self):
        try:
            with transaction.atomic():
                self.drop_atomic()
        except AttributeError:
            self.drop_atomic()
            transaction.commit_unless_managed(using=self.using)

    def install_atomic(self):
        cursor = self.cursor()
        cursor.execute("SELECT lanname FROM pg_catalog.pg_language WHERE lanname ='plpgsql'")

        if not cursor.fetchall():
            cursor.execute('CREATE LANGUAGE plpgsql')
        for name, triggers in self.triggers.items():
            for i, trigger in enumerate(triggers):
                sql, args = trigger.sql(name + "_%s" % i)
                cursor.execute(sql, args)

    def install(self):
        try:
            with transaction.atomic():
                self.install_atomic()
        except AttributeError:
            self.install_atomic()
            transaction.commit_unless_managed(using=self.using)
