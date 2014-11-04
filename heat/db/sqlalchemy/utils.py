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

# SQLAlchemy helper functions

import sqlalchemy

def clone_table(name, parent, meta, newcols=[], ignorecols=[], swapcols={}):
    """
    helper function that clones parent table schema onto
    new table.

    :param name: new table name
    :param parent: parent table to copy schema from
    :param newcols: list of new columns to be added
    :param ignorecols: list of columns to be ignored while clone
    :param swapcols: alternative column schema
    :return: sqlalchemy.Table instance
    """

    cols = [c.copy() for c in parent.columns
                if not c.name in ignorecols
                if not c.name in swapcols]
    cols.extend(swapcols.values())
    cols.extend(newcols)
    constraints = [c.copy() for c in parent.constraints]
    return sqlalchemy.Table(name, meta, *(cols + constraints))
