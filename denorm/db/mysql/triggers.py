import random
import string
from denorm.db import base


class RandomBigInt(base.RandomBigInt):
    def sql(self):
        return '(9223372036854775806 * ((RAND()-0.5)*2.0) )'


class TriggerNestedSelect(base.TriggerNestedSelect):

    def sql(self):
        columns = self.columns
        table = self.table
        where = ", ".join(["%s = %s" % (k, v) for k, v in self.kwargs.iteritems()])
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

        return 'INSERT IGNORE INTO %(table)s %(columns)s %(values)s' % locals(), tuple()


class TriggerActionUpdate(base.TriggerActionUpdate):

    def sql(self):
        table = self.model._meta.db_table
        updates = ", ".join(["%s = %s" % (k, v) for k, v in zip(self.columns, self.values)])
        if isinstance(self.where, tuple):
            where, where_params = self.where
        else:
            where, where_params = self.where, []

        return 'UPDATE %(table)s SET %(updates)s WHERE %(where)s' % locals(), tuple(where_params)


class TriggerConditionFieldChange(base.TriggerConditionFieldChange):
    def sql(self, actions):
        actions, params = super(TriggerConditionFieldChange, self).sql(actions)
        when = ["(" + "OR".join(["(OLD.%s <=> NEW.%s)" % (f, f) for f in self.field_names]) + ")"]
        when = "AND".join(when)

        return """
            IF %(when)s THEN
                %(actions)s
            END IF;
        """ % locals(), tuple(params)


class Trigger(base.Trigger):

    def sql(self, name):
        actions, params = super(Trigger, self).sql()

        if len(name) > 50:
            name = name[:45] + ''.join(
                random.choice(string.ascii_uppercase + string.digits)
                for x in range(5)
            )

        if not self.condition:
            actions = """
                %(actions)s
            """ % locals()

        table = self.db_table
        time = self.time.upper()
        event = self.event.upper()

        sql = """
            CREATE TRIGGER %(name)s
                %(time)s %(event)s ON %(table)s
                FOR EACH ROW BEGIN
                    %(actions)s
                END;
        """ % locals()
        return sql, tuple(params)


class TriggerSet(base.TriggerSet):
    def drop(self):
        qn = self.connection.ops.quote_name
        cursor = self.cursor()

        # FIXME: according to MySQL docs the LIKE statement should work
        # but it doesn't. MySQL reports a Syntax Error
        #cursor.execute(r"SHOW TRIGGERS WHERE Trigger LIKE 'denorm_%%'")
        cursor.execute('SHOW TRIGGERS')
        for result in cursor.fetchall():
            if result[0].startswith('denorm_'):
                cursor.execute('DROP TRIGGER %s;' % qn(result[0]))
