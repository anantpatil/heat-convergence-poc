#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import sqlalchemy

from heat.db.sqlalchemy import types as heat_db_types
from heat.db.sqlalchemy import utils as migrate_utils


def upgrade(migrate_engine):
    if migrate_engine.name == 'sqlite':
        upgrade_sqlite(migrate_engine)
        return

    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    tmpl_table = sqlalchemy.Table('raw_template', meta, autoload=True)
    environment = sqlalchemy.Column('environment', heat_db_types.Json)
    environment.create(tmpl_table)
    predecessor = sqlalchemy.Column('predecessor', sqlalchemy.Integer,
                                    sqlalchemy.ForeignKey('raw_template.id'))
    predecessor.create(tmpl_table)

    stack_table = sqlalchemy.Table('stack', meta, autoload=True)
    update_query = tmpl_table.update().values(
        environment=sqlalchemy.select([stack_table.c.parameters]).
        where(tmpl_table.c.id == stack_table.c.raw_template_id).
        as_scalar())
    migrate_engine.execute(update_query)

    stack_table.c.parameters.drop()


def upgrade_sqlite(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    tmpl_table = sqlalchemy.Table('raw_template', meta, autoload=True)
    newcols = [
        sqlalchemy.Column('environment', heat_db_types.Json),
        sqlalchemy.Column('predecessor', sqlalchemy.Integer,
                          sqlalchemy.ForeignKey('raw_template.id'))]
    new_template = migrate_utils.clone_table('new_raw_template',
                                             tmpl_table,
                                             meta, newcols=newcols)
    new_template.create()

    stack_table = sqlalchemy.Table('stack', meta, autoload=True)
    ignorecols = [stack_table.c.parameters]
    new_stack = migrate_utils.clone_table('new_stack', stack_table,
                                          meta, ignorecols=ignorecols)
    new_stack.create()

    # migrate parameters to environment
    templates = list(tmpl_table.select().order_by(
        sqlalchemy.sql.expression.asc(tmpl_table.c.created_at))
        .execute())
    colnames = [c.name for c in tmpl_table.columns]
    for template in templates:
        values = dict(zip(colnames,
                          map(lambda colname: getattr(template, colname),
                              colnames)))
        params = stack_table.select(stack_table.c.parameters).\
            where(stack_table.c.raw_template_id == values['id']).\
            execute().fetchone()
        values['environment'] = params
        migrate_engine.execute(new_template.insert(values))

    # migrate stacks to new table
    stacks = list(stack_table.select().order_by(
        sqlalchemy.sql.expression.asc(stack_table.c.created_at))
        .execute())
    colnames = [c.name for c in stack_table.columns]
    for stack in stacks:
        values = dict(zip(colnames,
                          map(lambda colname: getattr(stack, colname),
                              colnames)))
        del values['parameters']
        migrate_engine.execute(new_stack.insert(values))

    # Drop old tables and rename new ones
    stack_table.drop()
    tmpl_table.drop()
    new_stack.rename('stack')
    new_template.rename('raw_template')


def downgrade(migrate_engine):
    if migrate_engine.name == 'sqlite':
        downgrade_sqlite(migrate_engine)
        return

    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    stack_table = sqlalchemy.Table('stack', meta, autoload=True)
    parameters = sqlalchemy.Column('parameters', heat_db_types.Json)
    parameters.create(stack_table)

    tmpl_table = sqlalchemy.Table('raw_template', meta, autoload=True)
    update_query = stack_table.update().values(
        parameters=sqlalchemy.select([tmpl_table.c.environment]).
        where(stack_table.c.raw_template_id == tmpl_table.c.id).
        as_scalar())
    migrate_engine.execute(update_query)

    tmpl_table.c.environment.drop()
    tmpl_table.c.predecessor.drop()


def downgrade_sqlite(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    stack_table = sqlalchemy.Table('stack', meta, autoload=True)
    newcols = [sqlalchemy.Column('parameters', heat_db_types.Json)]
    new_stack = migrate_utils.clone_table('new_stack', stack_table,
                                          meta, newcols=newcols)
    new_stack.create()

    tmpl_table = sqlalchemy.Table('raw_template', meta, autoload=True)
    ignorecols = [tmpl_table.c.environment, tmpl_table.c.predecessor]
    new_template = migrate_utils.clone_table('new_raw_template', tmpl_table,
                                             meta, ignorecols=ignorecols)
    new_template.create()

    # migrate stack data to new table
    stacks = list(stack_table.select().order_by(
        sqlalchemy.sql.expression.asc(stack_table.c.created_at))
        .execute())
    colnames = [c.name for c in stack_table.columns]
    for stack in stacks:
        values = dict(zip(colnames,
                          map(lambda colname: getattr(stack, colname),
                              colnames)))
        migrate_engine.execute(new_stack.insert(values))

    update_query = new_stack.update().values(
        parameters=sqlalchemy.select([tmpl_table.c.environment]).
        where(new_stack.c.raw_template_id == tmpl_table.c.id).
        as_scalar())
    migrate_engine.execute(update_query)

    # migrate template data to new table
    templates = list(tmpl_table.select().order_by(
        sqlalchemy.sql.expression.asc(tmpl_table.c.created_at))
        .execute())
    colnames = [c.name for c in tmpl_table.columns]
    for template in templates:
        values = dict(zip(colnames,
                          map(lambda colname: getattr(template, colname),
                              colnames)))
        del values['environment']
        del values['predecessor']
        migrate_engine.execute(new_template.insert(values))

    stack_table.drop()
    tmpl_table.drop()
    new_stack.rename('stack')
    new_template.rename('raw_template')