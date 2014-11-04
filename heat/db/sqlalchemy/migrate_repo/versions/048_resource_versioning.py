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

    resource = sqlalchemy.Table('resource', meta, autoload=True)
    # (ckmvishnu) make nullable=True and remove default values
    # along with backup stack removal.
    version = sqlalchemy.Column('version',
                                sqlalchemy.Integer,
                                default=0,
                                server_default='0')
    version.create(resource)


def downgrade(migrate_engine):
    meta = sqlalchemy.MetaData(bind=migrate_engine)

    table = sqlalchemy.Table('resource', meta, autoload=True)
    table.c.version.drop()
