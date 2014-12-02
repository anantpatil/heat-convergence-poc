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

def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    stack = sqlalchemy.Table('stack', meta, autoload=True)
    req_id = sqlalchemy.Column('req_id', sqlalchemy.String(36),
                               nullable=False)
    req_id.create(stack)

    resource = sqlalchemy.Table('resource', meta, autoload=True)
    engine_id = sqlalchemy.Column('engine_id', sqlalchemy.String(36))
    engine_id.create(resource)


def downgrade(migrate_engine):
    meta = sqlalchemy.MetaData(bind=migrate_engine)

    stack = sqlalchemy.Table('stack', meta, autoload=True)
    stack.c.req_id.drop()

    resource = sqlalchemy.Table('resource', meta, autoload=True)
    resource.c.engine_id.drop()
